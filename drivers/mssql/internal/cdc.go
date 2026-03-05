package driver

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
)

const (
	cdcCursorKey = "lsn"

	CDCStartLSN = "_cdc_start_lsn" // MSSQL start LSN
	CDCSeqVal   = "_cdc_seqval"    // MSSQL seqval

	defaultCDCMaxTrans           = 500             // SQL Server default maxtrans per scan session
	defaultCDCPollingInterval    = 5 * time.Second // SQL Server default CDC job pollinginterval
	cdcAgentPollInterval         = 2 * time.Second // interval between scan session checks
	minCDCAgentCatchUpTimeout    = 15 * time.Minute
	catchUpTimeoutPollMultiplier = 3
)

// CDC capture instance for a table
type captureInstance struct {
	schema       string
	table        string
	instanceName string
	startLSN     string
}

// cdcScanSession represents a completed CDC log scan session from sys.dm_cdc_log_scan_sessions.
type cdcScanSession struct {
	sessionID int
	endTime   time.Time
	tranCount int
}

// prepareCaptureInstances discovers capture instances for selected streams
func (m *MSSQL) prepareCaptureInstances(ctx context.Context, stream types.StreamInterface) ([]captureInstance, error) {
	// Discover capture instances for selected streams
	rows, err := m.client.QueryContext(ctx, jdbc.MSSQLCDCDiscoverQuery(stream.ID()))
	if err != nil {
		return nil, fmt.Errorf("failed to query MSSQL CDC tables: %s", err)
	}
	defer rows.Close()

	var captureInstances []captureInstance
	for rows.Next() {
		var (
			capture  captureInstance
			startLSN []byte // to track the start LSN of the capture instance
		)

		if err := rows.Scan(&capture.schema, &capture.table, &capture.instanceName, &startLSN); err != nil {
			return nil, fmt.Errorf("failed to scan MSSQL CDC table: %s", err)
		}
		capture.startLSN = hex.EncodeToString(startLSN)
		captureInstances = append(captureInstances, capture)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return captureInstances, nil
}

func (m *MSSQL) ChangeStreamConfig() (bool, bool, bool) {
	return false, false, true // concurrent change streams supported, stream can start after finishing full load
}

// PreCDC initialises CDC state and starting LSN per stream.
func (m *MSSQL) PreCDC(ctx context.Context, streams []types.StreamInterface) error {
	if !m.cdcSupported {
		return fmt.Errorf("invalid call; %s not running in CDC mode", m.Type())
	}

	var currentLSN string
	for _, stream := range streams {
		enabled, err := m.validateCDCStream(ctx, stream.Namespace(), stream.Name())
		if err != nil {
			return fmt.Errorf("failed to validate CDC for stream %s.%s: %s", stream.Namespace(), stream.Name(), err)
		}

		if !enabled {
			return fmt.Errorf("CDC is not enabled for stream %s.%s", stream.Namespace(), stream.Name())
		}

		if m.state.GetCursor(stream.Self(), cdcCursorKey) == nil {
			if currentLSN == "" {
				currentLSN, err = m.resolveInitialLSN(ctx)
				if err != nil {
					return fmt.Errorf("failed to get MSSQL current max LSN: %w", err)
				}
			}
			m.state.SetCursor(stream.Self(), cdcCursorKey, currentLSN)
		}
	}
	m.streams = streams
	return nil
}

// StreamChanges fetches a bounded window of CDC changes for a specific stream.
func (m *MSSQL) StreamChanges(ctx context.Context, streamIndex int, processFn abstract.CDCMsgFn) error {
	stream := m.streams[streamIndex]
	// Get current position for this stream
	lsnString := m.state.GetCursor(stream.Self(), cdcCursorKey)
	lsnInState := lsnString.(string)

	// Get target LSN (current max LSN in DB)
	targetLSN, err := m.currentMaxLSN(ctx)
	if err != nil {
		return fmt.Errorf("failed to get MSSQL max LSN: %s", err)
	}

	// No changes yet
	if lsnInState >= targetLSN {
		return nil
	}

	// prepare capture instance
	captureInstances, err := m.prepareCaptureInstances(ctx, stream)
	if err != nil {
		return fmt.Errorf("failed to prepare capture instance for stream %s.%s: %s", stream.Namespace(), stream.Name(), err)
	}

	// TODO: research how to handle schema evolution

	// When multiple capture instances exist for the same table (due to schema
	// evolution), pick the newest instance whose startLSN is <= fromLSN.
	// This guarantees we continue from an instance that was valid at our last
	// processed LSN.
	//
	// If there is a newer capture instance after the one we select, clamp
	// targetLSN to that newer instance's startLSN so we do not read rows that
	// conceptually belong to the new schema from the old capture instance.
	//
	// Note: we expect column-level data loss (e.g., new columns missing)
	// in the LSN range between the DDL and when the new capture instance becomes active.
	var selectedCapture *captureInstance
	for captureIdx := len(captureInstances) - 1; captureIdx >= 0; captureIdx-- {
		// Skip if this capture started after fromLSN
		if captureInstances[captureIdx].startLSN > lsnInState {
			continue
		}

		// Select the capture instance
		selectedCapture = &captureInstances[captureIdx]

		// If a newer capture instance exists, restrict the targetLSN to the newer instance's startLSN
		nextCaptureIdx := captureIdx + 1
		if nextCaptureIdx < len(captureInstances) && targetLSN > captureInstances[nextCaptureIdx].startLSN {
			newerCapture := captureInstances[nextCaptureIdx]
			logger.Warnf("Newer capture instance [%s] detected for stream %s at LSN %s, but not using it in this sync. Clamping targetLSN to %s. It will be picked up in the next CDC sync", newerCapture.instanceName, stream.ID(), newerCapture.startLSN, newerCapture.startLSN)
			targetLSN = newerCapture.startLSN
		}

		break
	}

	if selectedCapture == nil {
		return fmt.Errorf(
			"LSN %s is earlier than the start LSN of available capture instances for stream %s. Please perform full-refresh",
			lsnInState,
			stream.ID(),
		)
	}

	logger.Infof(
		"Selected CDC capture instance [%s] for stream %s (from LSN %s)",
		selectedCapture.instanceName,
		stream.ID(),
		lsnInState,
	)

	// Fetch changes
	err = m.fetchTableChangesInLSNRange(ctx, stream, *selectedCapture, lsnInState, targetLSN, processFn)
	if err != nil {
		return err
	}

	// Cache target LSN for this stream
	m.lsnMap.Store(stream.ID(), targetLSN)

	return nil
}

// PostCDC per stream state saving is handled here if no error occurred.
func (m *MSSQL) PostCDC(ctx context.Context, streamIndex int) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		stream := m.streams[streamIndex]
		if val, exists := m.lsnMap.Load(stream.ID()); exists {
			m.state.SetCursor(stream.Self(), cdcCursorKey, val)
		} else {
			logger.Warnf("No LSN found for stream: %s", stream.ID())
		}
	}
	return nil
}

// fetchTableChangesInLSNRange fetches and emits CDC changes for a single table/capture-instance within an LSN range.
func (m *MSSQL) fetchTableChangesInLSNRange(ctx context.Context, stream types.StreamInterface, capture captureInstance, fromLSN, toLSN string, processFn abstract.CDCMsgFn) error {
	// Move the lower bound forward by one LSN to avoid re-emitting the last processed row.
	effectiveFromLSN, err := m.advanceLSN(ctx, fromLSN)
	if err != nil {
		return err
	}

	// SQL Server expects LSN parameters as binary; state stores them as hex strings.
	fromLSNBytes, err := hex.DecodeString(effectiveFromLSN)
	if err != nil {
		return fmt.Errorf("failed to parse fromLSN: %s", err)
	}
	toLSNBytes, err := hex.DecodeString(toLSN)
	if err != nil {
		return fmt.Errorf("failed to parse toLSN: %s", err)
	}

	// Query CDC rows for this capture instance between the two LSNs.
	query := jdbc.MSSQLCDCGetChangesQuery(capture.instanceName)
	rows, err := m.client.QueryContext(ctx, query, fromLSNBytes, toLSNBytes)
	if err != nil {
		return fmt.Errorf("failed to query MSSQL CDC changes: %s", err)
	}
	defer rows.Close()

	for rows.Next() {
		// Use MapScan to properly convert data types including binary types
		// TODO: check if we can use MapScanConcurrent for mssql
		record := make(map[string]interface{})
		if err := jdbc.MapScan(rows, record, m.dataTypeConverter); err != nil {
			return fmt.Errorf("failed to scan MSSQL CDC row: %s", err)
		}

		// Determine operation type from SQL Server CDC operation codes.
		// For updates, CDC emits "before" (3) and "after" (4); we skip "before".
		var operationType string
		if val, ok := record["__$operation"]; ok {
			var opCode int32 = val.(int32)
			if opCode == 3 {
				continue
			}
			operationType = operationTypeFromCDCCode(opCode)
		}
		extraColumns := map[string]any{
			CDCStartLSN: fmt.Sprintf("%x", record["__$start_lsn"]),
			CDCSeqVal:   fmt.Sprintf("%x", record["__$seqval"])}
		// Remove metadata columns
		delete(record, "__$operation")
		delete(record, "__$start_lsn")
		delete(record, "__$seqval")
		delete(record, "__$update_mask")

		// Emit one normalized CDC change event.
		if err := processFn(ctx, abstract.CDCChange{
			Stream:       stream,
			Timestamp:    time.Now().UTC(),
			Kind:         operationType,
			Data:         record,
			ExtraColumns: extraColumns,
		}); err != nil {
			return fmt.Errorf("failed to process MSSQL CDC change: %s", err)
		}
	}

	if err := rows.Err(); err != nil {
		return err
	}
	return nil
}

func (m *MSSQL) currentMaxLSN(ctx context.Context) (string, error) {
	var lsn []byte
	err := m.client.QueryRowContext(ctx, jdbc.MSSQLCDCMaxLSNQuery()).Scan(&lsn)
	if err != nil {
		return "", err
	}
	if len(lsn) == 0 {
		return "", fmt.Errorf("no LSN available (CDC may not be initialized or no transactions exist)")
	}
	return hex.EncodeToString(lsn), nil
}

// advanceLSN returns the next valid LSN after the given LSN.
func (m *MSSQL) advanceLSN(ctx context.Context, lsnHex string) (string, error) {
	// Decode the hex string into raw bytes because SQL Server LSN functions use binary values.
	lsnBytes, err := hex.DecodeString(lsnHex)
	if err != nil {
		return "", fmt.Errorf("failed to parse LSN for advance: %s", err)
	}

	// Compute the next LSN.
	var nextLSNBytes []byte
	if err := m.client.QueryRowContext(ctx, jdbc.MSSQLCDCAdvanceLSNQuery(), lsnBytes).Scan(&nextLSNBytes); err != nil {
		return "", fmt.Errorf("failed to advance LSN: %s", err)
	}

	if len(nextLSNBytes) == 0 {
		return "", fmt.Errorf("advanced LSN is empty")
	}

	return hex.EncodeToString(nextLSNBytes), nil
}

// operationTypeFromCDCCode converts SQL Server CDC __$operation codes to our operationType string.
// Codes: 1=delete, 2=insert, 3/4=update (before/after).
func operationTypeFromCDCCode(code int32) string {
	switch code {
	case 1:
		return "delete"
	case 2:
		return "insert"
	case 3, 4:
		return "update"
	default:
		return "update"
	}
}

// resolveInitialLSN returns a correct starting LSN for the first CDC sync.
//
// MSSQL CDC uses an asynchronous capture agent that copies committed transactions from the
// transaction log into CDC change tables. sys.fn_cdc_get_max_lsn() only reflects what the
// agent has processed, which can lag behind the actual transaction log. This creates a race
// condition during initial sync:
//
//  1. PreCDC reads max LSN from CDC tables → gets stale value (e.g. LSN 3)
//  2. Backfill reads all committed rows from the source table (includes data up to LSN 7)
//  3. CDC agent catches up, populating change tables from LSN 3 to 7
//  4. StreamChanges replays LSN 3→7, re-emitting rows already backfilled
//
// To prevent this, we observe sys.dm_cdc_log_scan_sessions to determine when the CDC agent
// has finished a non-throttled scan session (tran_count < maxtrans). A non-throttled session
// means the agent fully drained the transaction log. We then read the max LSN, knowing it
// reflects all committed transactions at that point.
func (m *MSSQL) resolveInitialLSN(ctx context.Context) (string, error) {
	var hasPermission bool
	err := m.client.QueryRowContext(ctx, jdbc.MSSQLViewDatabaseStatePermissionQuery()).Scan(&hasPermission)
	if err != nil {
		logger.Warnf("failed to check VIEW DATABASE STATE permission, falling back to current max LSN: %s", err)
		return m.currentMaxLSN(ctx)
	}

	if !hasPermission {
		logger.Warnf("VIEW DATABASE STATE permission not granted; CDC cursor may lag behind the transaction log, which can cause duplicate rows during backfill.")
		return m.currentMaxLSN(ctx)
	}

	return m.waitForCDCAgentCatchUp(ctx)
}

// waitForCDCAgentCatchUp waits until the CDC capture agent completes a scan
// session where tran_count < maxtrans. Because the agent processes at most
// `maxtrans` transactions per session (default 500), a session that finishes
// with fewer transactions indicates it reached the end of the transaction log
// and is fully caught up at that moment.
func (m *MSSQL) waitForCDCAgentCatchUp(ctx context.Context) (string, error) {
	var (
		maxTrans         int
		pollingIntervalS int
	)
	err := m.client.QueryRowContext(ctx, jdbc.MSSQLCDCCaptureJobConfigQuery()).Scan(&maxTrans, &pollingIntervalS)
	if err != nil {
		logger.Errorf("unable to query CDC capture job config, using defaults maxtrans=%d pollinginterval=%s: %s", defaultCDCMaxTrans, defaultCDCPollingInterval, err)
		maxTrans = defaultCDCMaxTrans
		pollingIntervalS = int(defaultCDCPollingInterval.Seconds())
	}
	if pollingIntervalS < 0 {
		logger.Warnf("invalid CDC job pollinginterval=%d, using default %s", pollingIntervalS, defaultCDCPollingInterval)
		pollingIntervalS = int(defaultCDCPollingInterval.Seconds())
	}
	// Question: polling interval is editable, it is 5s be default and in very rare scenarios
	// it can be set to 24 hrs (which is ideally never the case), should we handle for that as well
	// or have a fixed catchup time
	//
	// 1–30 seconds for “near real-time” pipelines.
	// 1–5 minutes for “fresh but not strict real-time” analytics dashboards.
	// 15–60 minutes only for low-change, low-SLA reporting or cost-constrained workloads.
	catchUpTimeout := max(minCDCAgentCatchUpTimeout, time.Duration(catchUpTimeoutPollMultiplier*pollingIntervalS)*time.Second)

	// Record the baseSession scan session before we start waiting
	// query can fail if there are no scan sessions yet, so fallback to current max LSN
	baseSession, err := m.latestCDCScanSession(ctx)
	if err != nil {
		logger.Warnf("failed to query CDC scan sessions, falling back to current max LSN: %s", err)
		return m.currentMaxLSN(ctx)
	}

	logger.Infof("waiting for CDC capture agent to catch up (baseline end_time=%s, maxtrans=%d, pollinginterval=%ds, timeout=%s)", baseSession.endTime.Format(time.RFC3339), maxTrans, pollingIntervalS, catchUpTimeout)

	deadline := time.After(catchUpTimeout)
	ticker := time.NewTicker(cdcAgentPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-deadline:
			logger.Warnf("CDC agent did not catch up within %s, using current max LSN", catchUpTimeout)
			return m.currentMaxLSN(ctx)
		case <-ticker.C:
			session, err := m.latestCDCScanSession(ctx)
			if err != nil {
				logger.Warnf("failed to query CDC scan session: %s", err)
				continue
			}

			// A session that completed after our baseSession and wasn't throttled
			// means the agent has fully drained the transaction log.
			if session.endTime.After(baseSession.endTime) && session.tranCount < maxTrans {
				logger.Infof("CDC agent caught up (session_id=%d, end_time=%s, tran_count=%d)", session.sessionID, session.endTime.Format(time.RFC3339), session.tranCount)
				return m.currentMaxLSN(ctx)
			}
		}
	}
}

// latestCDCScanSession returns the most recent completed CDC log scan session.
func (m *MSSQL) latestCDCScanSession(ctx context.Context) (session cdcScanSession, err error) {
	err = m.client.QueryRowContext(ctx, jdbc.MSSQLCDCLatestScanSessionQuery()).Scan(&session.sessionID, &session.endTime, &session.tranCount)
	if err != nil {
		return session, fmt.Errorf("failed to query CDC log scan sessions: %s", err)
	}
	return session, nil
}
