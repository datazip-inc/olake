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

// PreCDC initialises global CDC state and starting LSN.
func (m *MSSQL) PreCDC(ctx context.Context, streams []types.StreamInterface) error {
	if !m.CDCSupport {
		return fmt.Errorf("invalid call; %s not running in CDC mode", m.Type())
	}

	if m.streams == nil {
		m.streams = make(map[string]types.StreamInterface, len(streams))
	}
	for _, stream := range streams {
		m.streams[stream.ID()] = stream
	}

	// Validate that all selected streams have CDC enabled at the table level
	for _, stream := range streams {
		enabled, err := m.IsTableCDCEnabled(ctx, stream.Namespace(), stream.Name())
		if err != nil {
			return fmt.Errorf("failed to check CDC for table %s.%s: %s", stream.Namespace(), stream.Name(), err)
		}
		if !enabled {
			logger.Warnf("CDC is not enabled for table %s.%s. Please enable CDC for this table using sys.sp_cdc_enable_table", stream.Namespace(), stream.Name())
		}
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

	// Process bounded window [fromLSN, targetLSN].
	err = m.fetchAllTableChangesInLSNRange(ctx, fromLSN, targetLSN, processFn)
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

// fetchAllTableChangesInLSNRange reads CDC changes for all selected CDC-enabled tables within a single LSN range.
func (m *MSSQL) fetchAllTableChangesInLSNRange(ctx context.Context, fromLSN, toLSN string, processFn abstract.CDCMsgFn) error {
	rows, err := m.client.QueryContext(ctx, jdbc.MSSQLCDCDiscoverQuery())
	if err != nil {
		return fmt.Errorf("failed to query MSSQL CDC tables: %s", err)
	}
	defer rows.Close()

	var captures []captureInstance
	for rows.Next() {
		var capture captureInstance
		if err := rows.Scan(&capture.schema, &capture.table, &capture.instanceName); err != nil {
			return fmt.Errorf("failed to scan MSSQL CDC table: %s", err)
		}

		streamID := fmt.Sprintf("%s.%s", capture.schema, capture.table)
		if _, selected := m.streams[streamID]; !selected {
			continue
		}

		captures = append(captures, capture)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, capture := range captures {
		if err := m.fetchTableChangesInLSNRange(ctx, capture.schema, capture.table, capture.instanceName, fromLSN, toLSN, processFn); err != nil {
			return err
		}
	}
	return nil
}

// fetchTableChangesInLSNRange fetches and emits CDC changes for a single table/capture-instance within an LSN range.
func (m *MSSQL) fetchTableChangesInLSNRange(ctx context.Context, schema, table, captureInstance, fromLSN, toLSN string, processFn abstract.CDCMsgFn) error {
	streamID := fmt.Sprintf("%s.%s", schema, table)
	cfgStream, selected := m.streams[streamID]
	if !selected {
		return fmt.Errorf("CDC capture instance discovered for unselected stream %s", streamID)
	}

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
	rows, err := m.client.QueryContext(ctx, jdbc.MSSQLCDCGetChangesQuery(captureInstance), fromLSNBytes, toLSNBytes)
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
		if operation, ok := record["__$operation"]; ok {
			switch v := operation.(type) {
			case int32:
				if int(v) == 3 {
					continue
				}
				operationType = operationTypeFromCDCCode(int(v))
			case int64:
				if int(v) == 3 {
					continue
				}
				operationType = operationTypeFromCDCCode(int(v))
			case int:
				if v == 3 {
					continue
				}
				operationType = operationTypeFromCDCCode(v)
			default:
				// Fallback: treat unknown types as update.
				operationType = "update"
			}
		}

		// Remove CDC metadata columns so only real table columns remain in the payload.
		delete(record, "__$operation")
		delete(record, "__$start_lsn")
		delete(record, "__$seqval")
		delete(record, "__$update_mask")

		// Emit one normalized CDC change event.
		if err := processFn(ctx, abstract.CDCChange{
			Stream:    cfgStream,
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
func operationTypeFromCDCCode(code int) string {
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
