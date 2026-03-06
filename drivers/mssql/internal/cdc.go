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

	cdcAgentPollInterval          = 2 * time.Second // interval between scan session checks
	defaultCDCAgentCatchUpTimeout = 15 * time.Minute
	catchUpTimeoutPollMultiplier  = 3
	defaultCDCAgentMaxTrans       = 500
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

func (m *MSSQL) ChangeStreamConfig() (bool, bool, bool) {
	return false, false, true // concurrent change streams supported, stream can start after finishing full load
}

// PreCDC initialises CDC state and starting LSN per stream.
func (m *MSSQL) PreCDC(ctx context.Context, streams []types.StreamInterface) error {
	if !m.cdcSupported {
		return fmt.Errorf("invalid call; %s not running in CDC mode", m.Type())
	}

	var currentLSN string

	streamIDs := make([]string, len(streams))
	for i, s := range streams {
		streamIDs[i] = s.ID()
	}

	// Bulk discovery of capture instances for validation and management
	captureInstancesMap, err := m.prepareCaptureInstancesBulk(ctx, streamIDs)
	if err != nil {
		return fmt.Errorf("failed to discover capture instances: %w", err)
	}

	// check if CDC is enabled for each stream
	for _, stream := range streams {
		if _, found := captureInstancesMap[stream.ID()]; !found {
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

	// Manage capture instance creation and deletion
	if err := m.manageCaptureInstances(ctx, streamIDs, streams, captureInstancesMap); err != nil {
		return fmt.Errorf("failed to manage CDC capture instances: %w", err)
	}

	m.streams = streams
	return nil
}

// StreamChanges fetches a bounded window of CDC changes for a specific stream.
func (m *MSSQL) StreamChanges(ctx context.Context, streamIndex int, metadataStates map[string]any, processFn abstract.CDCMsgFn) (any, error) {
	stream := m.streams[streamIndex]
	// Get current position for this stream
	lsnVal := m.state.GetCursor(stream.Self(), cdcCursorKey)
	lsnInState := lsnVal.(string)

	rawMtState, exists := metadataStates[stream.ID()]
	if exists && rawMtState != nil {
		mtState, ok := rawMtState.(string)
		if !ok {
			return nil, fmt.Errorf("failed to typecast mtstate to string of type[%T]", rawMtState)
		}
		if mtState != lsnInState {
			logger.Infof("Stream[%s] LSN mismatch, updating LSN in state", stream.ID())
			m.lsnMap.Store(stream.ID(), mtState)
			return mtState, nil
		}
	}

	// Get target LSN (current max LSN in DB)
	targetLSN, err := m.currentMaxLSN(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get MSSQL max LSN: %s", err)
	}

	// No changes yet
	if lsnInState >= targetLSN {
		return nil, nil
	}

	// prepare capture instance
	captureInstances, err := m.prepareCaptureInstances(ctx, stream)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare capture instance for stream %s.%s: %s", stream.Namespace(), stream.Name(), err)
	}

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
	captureIdx, selectedCapture := newestValidInstance(captureInstances, lsnInState)
	if selectedCapture == nil {
		return nil, fmt.Errorf(
			"LSN %s is earlier than the start LSN of available capture instances for stream %s. Please perform full-refresh",
			lsnInState,
			stream.ID(),
		)
	}
	// If a newer capture instance exists, restrict the targetLSN to the newer instance's startLSN
	nextCaptureIdx := captureIdx + 1
	if nextCaptureIdx < len(captureInstances) && targetLSN > captureInstances[nextCaptureIdx].startLSN {
		newerCapture := captureInstances[nextCaptureIdx]
		logger.Warnf("Newer capture instance [%s] detected for stream %s at LSN %s, but not using it in this sync. Clamping targetLSN to %s. It will be picked up in the next CDC sync", newerCapture.instanceName, stream.ID(), newerCapture.startLSN, newerCapture.startLSN)
		targetLSN = newerCapture.startLSN
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
		return nil, err
	}

	// Cache target LSN for this stream
	m.lsnMap.Store(stream.ID(), targetLSN)

	return targetLSN, nil
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

// prepareCaptureInstancesBulk retrieves all currently defined CDC capture instances for the specified streams.
func (m *MSSQL) prepareCaptureInstancesBulk(ctx context.Context, streamIDs []string) (map[string][]captureInstance, error) {
	rows, err := m.client.QueryContext(ctx, jdbc.MSSQLCDCDiscoverQuery(streamIDs))
	if err != nil {
		return nil, fmt.Errorf("failed to query MSSQL CDC tables: %s", err)
	}
	defer rows.Close()

	captureInstances := make(map[string][]captureInstance, len(streamIDs))
	for rows.Next() {
		var (
			capture  captureInstance
			startLSN []byte
		)

		if err := rows.Scan(&capture.schema, &capture.table, &capture.instanceName, &startLSN); err != nil {
			return nil, fmt.Errorf("failed to scan MSSQL CDC table: %s", err)
		}
		capture.startLSN = hex.EncodeToString(startLSN)
		streamID := fmt.Sprintf("%s.%s", capture.schema, capture.table)
		captureInstances[streamID] = append(captureInstances[streamID], capture)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over bulk capture instances: %w", err)
	}

	return captureInstances, nil
}

// prepareCaptureInstances discovers capture instances for a stream
func (m *MSSQL) prepareCaptureInstances(ctx context.Context, stream types.StreamInterface) ([]captureInstance, error) {
	res, err := m.prepareCaptureInstancesBulk(ctx, []string{stream.ID()})
	if err != nil {
		return nil, err
	}
	return res[stream.ID()], nil
}

// manageCaptureInstances manages the lifecycle of CDC capture instances for multiple streams.
func (m *MSSQL) manageCaptureInstances(ctx context.Context, streamIDs []string, streams []types.StreamInterface, captureInstancesMap map[string][]captureInstance) error {
	// If manage capture instances is not enabled, do nothing
	if !m.config.ManageCaptureInstances {
		return nil
	}

	// Fetch DDL history for all streams in bulk
	ddlHistoryQuery := jdbc.MSSQLCDCGetDDLHistoryBulkQuery(streamIDs)
	rows, err := m.client.QueryContext(ctx, ddlHistoryQuery)
	if err != nil {
		return fmt.Errorf("failed to query bulk DDL history: %w", err)
	}
	defer rows.Close()

	latestDDLMap := make(map[string]string, len(streamIDs))
	for rows.Next() {
		var (
			schema, table, command string
			ddlLSN                 []byte
			ddlTime                time.Time
			requiredColumnUpdate   bool
		)
		if err := rows.Scan(&schema, &table, &requiredColumnUpdate, &command, &ddlLSN, &ddlTime); err != nil {
			return fmt.Errorf("failed to scan DDL history: %w", err)
		}
		streamID := fmt.Sprintf("%s.%s", schema, table)
		// Since results are ORDER BY ASC, the last one for a table is the latest.
		latestDDLMap[streamID] = hex.EncodeToString(ddlLSN)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating over bulk DDL history: %w", err)
	}

	for _, stream := range streams {
		streamID := stream.ID()
		instances := captureInstancesMap[streamID]
		// if cdc is not enabled it will fail in preCDC
		// So we are sure that lsn exists in state for this stream
		currentCursorLSN := m.state.GetCursor(stream.Self(), cdcCursorKey).(string)

		// Find the newest valid instance
		activeIdx, selected := newestValidInstance(instances, currentCursorLSN)
		if selected == nil {
			return fmt.Errorf("LSN %s for stream %s is older than any available scan instances; please perform a full refresh", currentCursorLSN, streamID)
		}

		// Delete fully consumed older instances and track survivors
		deletedCount := 0
		for idx, capture := range instances {
			if idx != activeIdx && (currentCursorLSN == "" || capture.startLSN <= currentCursorLSN) {
				query := jdbc.MSSQLCDCDisableCaptureInstanceQuery()
				_, err := m.client.ExecContext(ctx, query, capture.schema, capture.table, capture.instanceName)
				if err != nil {
					return fmt.Errorf("failed to delete obsolete capture instance %s for %s: %w", capture.instanceName, streamID, err)
				}
				logger.Infof("Deleted fully consumed CDC capture instance [%s] for stream %s", capture.instanceName, streamID)
				deletedCount++
			}
		}

		// Check if we can create a new one (SQL Server limit is max 2)
		// We use the count from our discovery minus what we just deleted.
		if len(instances)-deletedCount == 1 {
			latestCapture := instances[activeIdx]

			// Check if any DDL event for this table is newer than the latest capture start_lsn
			if ddlLSN, ok := latestDDLMap[streamID]; ok && ddlLSN > latestCapture.startLSN {
				// Create a new instance with a safe length (MSSQL limit is 100 chars)
				streamPart := streamID
				if len(streamPart) > 75 {
					streamPart = streamPart[:75]
				}
				newInstanceName := fmt.Sprintf("olake_%s_%d", streamPart, time.Now().Unix())

				createCaptureInstanceQuery := jdbc.MSSQLCDCCreateCaptureInstanceQuery()
				_, err := m.client.ExecContext(ctx, createCaptureInstanceQuery, latestCapture.schema, latestCapture.table, newInstanceName)
				if err != nil {
					return fmt.Errorf("failed to create new capture instance for schema drift on %s: %w", streamID, err)
				}
				logger.Infof("Detected schema drift and created new CDC capture instance [%s] for stream %s", newInstanceName, streamID)
			}
		}
	}

	return nil
}

// newestValidInstance selects the newest capture instance whose startLSN is <= currentLSN.
func newestValidInstance(instances []captureInstance, currentLSN string) (int, *captureInstance) {
	for i := len(instances) - 1; i >= 0; i-- {
		// if currentLSN is empty, it means we are starting fresh, so we pick the latest instance
		if currentLSN == "" || instances[i].startLSN <= currentLSN {
			return i, &instances[i]
		}
	}
	return -1, nil
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
		return "", fmt.Errorf("failed to check VIEW DATABASE STATE permission: %s", err)
	}

	if !hasPermission {
		logger.Warnf("VIEW DATABASE STATE permission not granted; LSN may be lagging behind the transaction log")
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
		logger.Errorf("unable to query CDC capture job config: %s; using default max trans %d", err, defaultCDCAgentMaxTrans)
		maxTrans = defaultCDCAgentMaxTrans
	}

	catchUpTimeout := max(defaultCDCAgentCatchUpTimeout, time.Duration(catchUpTimeoutPollMultiplier*pollingIntervalS)*time.Second)

	// Record the baseSession scan session before we start waiting.
	baseSession, err := m.latestCDCScanSession(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to query CDC scan sessions: %s", err)
	}

	logger.Infof("waiting for CDC capture agent to catch up (baseline end_time=%s, maxtrans=%d, pollinginterval=%ds, timeout=%s)", baseSession.endTime.Format(time.RFC3339), maxTrans, pollingIntervalS, catchUpTimeout)

	deadline := time.After(catchUpTimeout)
	ticker := time.NewTicker(cdcAgentPollInterval) // poll every 2 seconds
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-deadline:
			logger.Warnf("CDC agent did not catch up within %s, the CDC agent may not be running", catchUpTimeout)
			return m.currentMaxLSN(ctx) // fallback to current max LSN
		case <-ticker.C:
			session, err := m.latestCDCScanSession(ctx)
			if err != nil {
				return "", fmt.Errorf("failed to query CDC scan session: %s", err)
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
