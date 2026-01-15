package driver

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
)

// MSSQLGlobalState keeps last processed LSN for CDC
type MSSQLGlobalState struct {
	LSN string `json:"lsn"`
}

// CDC capture instance for a table
type captureInstance struct {
	schema       string
	table        string
	instanceName string
}

// prepareCaptureInstance discovers capture instances for selected streams and returns the starting LSN for each stream.
func (m *MSSQL) prepareCaptureInstance(ctx context.Context, streams []types.StreamInterface) error {
	streamMap := make(map[string]types.StreamInterface, len(streams))
	streamIDs := make([]string, 0, len(streams))
	for _, stream := range streams {
		streamMap[stream.ID()] = stream
		streamIDs = append(streamIDs, stream.ID())
	}

	// Discover capture instances for selected streams
	rows, err := m.client.QueryContext(ctx, jdbc.MSSQLCDCDiscoverQuery(streamIDs))
	if err != nil {
		return fmt.Errorf("failed to query MSSQL CDC tables: %s", err)
	}
	defer rows.Close()

	// Get current max LSN once to use as start point for new streams
	currentLSN, err := m.currentMaxLSN(ctx)
	if err != nil {
		return fmt.Errorf("failed to get MSSQL current max LSN: %s", err)
	}

	for rows.Next() {
		var capture captureInstance
		if err := rows.Scan(&capture.schema, &capture.table, &capture.instanceName); err != nil {
			return fmt.Errorf("failed to scan MSSQL CDC table: %s", err)
		}

		streamID := fmt.Sprintf("%s.%s", capture.schema, capture.table)
		if stream, ok := streamMap[streamID]; ok {
			var startLSN string

			// Check if start_lsn is already persisted in state.
			if val := m.state.GetCursor(stream.Self(), "start_lsn"); val != nil {
				if s, ok := val.(string); ok {
					startLSN = s
				}
			} else {
				// If not in state, check if it's a new stream (backfill not completed).
				// We set startLSN to currentLSN for new streams to avoid overlap with backfill.
				if !m.state.HasCompletedBackfill(stream.Self()) {
					startLSN = currentLSN
					// Persist to state to handle restarts.
					m.state.SetCursor(stream.Self(), "start_lsn", startLSN)
				}
			}

			m.captures = append(m.captures, captureInfo{
				capture:  capture,
				stream:   stream,
				startLSN: startLSN,
			})
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	if len(m.captures) == 0 {
		logger.Warnf("No CDC capture instances found for selected streams")
	}

	return nil
}

// PreCDC initialises global CDC state and starting LSN.
func (m *MSSQL) PreCDC(ctx context.Context, streams []types.StreamInterface) error {
	if !m.CDCSupport {
		return fmt.Errorf("invalid call; %s not running in CDC mode", m.Type())
	}

	if err := m.prepareCaptureInstance(ctx, streams); err != nil {
		return err
	}

	// Initialize CDC state if needed
	initializeCDCState := func(ctx context.Context) error {
		lsn, err := m.currentMaxLSN(ctx)
		if err != nil {
			return fmt.Errorf("failed to get MSSQL current max LSN: %s", err)
		}
		m.state.SetGlobal(MSSQLGlobalState{LSN: lsn})
		m.state.ResetStreams()
		m.lastProcessedLSN = lsn
		return nil
	}

	globalState := m.state.GetGlobal()
	if globalState == nil || globalState.State == nil {
		return initializeCDCState(ctx)
	}

	var mssqlState MSSQLGlobalState
	if err := utils.Unmarshal(globalState.State, &mssqlState); err != nil {
		return fmt.Errorf("failed to unmarshal MSSQL global state: %s", err)
	}

	if mssqlState.LSN == "" {
		return initializeCDCState(ctx)
	}

	m.lastProcessedLSN = mssqlState.LSN
	return nil
}

// StreamChanges fetches a bounded window of CDC changes up to the current max LSN and returns.
func (m *MSSQL) StreamChanges(ctx context.Context, _ types.StreamInterface, processFn abstract.CDCMsgFn) error {
	fromLSN := m.lastProcessedLSN
	targetLSN, err := m.currentMaxLSN(ctx)
	if err != nil {
		return fmt.Errorf("failed to get MSSQL max LSN: %s", err)
	}

	// If already caught up to the snapshot target, finish immediately.
	if fromLSN >= targetLSN {
		return nil
	}

	// Process bounded window [fromLSN, targetLSN] in parallel across all captured tables.
	err = utils.Concurrent(ctx, m.captures, m.MaxConnections(), func(ctx context.Context, info captureInfo, _ int) error {
		return m.fetchTableChangesInLSNRange(ctx, info, fromLSN, targetLSN, processFn)
	})
	if err != nil {
		return err
	}

	m.lastProcessedLSN = targetLSN
	return nil
}

// PostCDC persists final LSN
func (m *MSSQL) PostCDC(ctx context.Context, _ types.StreamInterface, noErr bool, _ string) error {
	if noErr && m.lastProcessedLSN != "" {
		m.state.SetGlobal(MSSQLGlobalState{LSN: m.lastProcessedLSN})
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

// fetchTableChangesInLSNRange fetches and emits CDC changes for a single table/capture-instance within an LSN range.
func (m *MSSQL) fetchTableChangesInLSNRange(ctx context.Context, info captureInfo, fromLSN, toLSN string, processFn abstract.CDCMsgFn) error {
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
	rows, err := m.client.QueryContext(ctx, jdbc.MSSQLCDCGetChangesQuery(info.capture.instanceName), fromLSNBytes, toLSNBytes)
	if err != nil {
		return fmt.Errorf("failed to query MSSQL CDC changes: %s", err)
	}
	defer rows.Close()

	for rows.Next() {
		// Use MapScan to properly convert data types including binary types
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

		// Extract start_lsn before deleting metadata
		var startLSNBytes []byte
		if startLSN, ok := record["__$start_lsn"]; ok {
			startLSNBytes = []byte(startLSN.(string))
		}

		delete(record, "__$operation")
		delete(record, "__$start_lsn")
		delete(record, "__$seqval")
		delete(record, "__$update_mask")

		// we only process events occurring after startLSN to avoid duplication with the backfill snapshot.
		if info.startLSN != "" {
			// Convert startLSN to hex string for comparison
			eventLSNHex := hex.EncodeToString(startLSNBytes)
			if eventLSNHex <= info.startLSN {
				continue
			}
		}

		// Emit one normalized CDC change event.
		if err := processFn(ctx, abstract.CDCChange{
			Stream:    info.stream,
			Timestamp: time.Now().UTC(),
			Kind:      operationType,
			Data:      record,
		}); err != nil {
			return fmt.Errorf("failed to process MSSQL CDC change: %s", err)
		}
	}

	if err := rows.Err(); err != nil {
		return err
	}
	return nil
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
