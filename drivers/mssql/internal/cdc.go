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

// PreCDC initialises global CDC state and starting LSN.
func (m *MSSQL) PreCDC(ctx context.Context, streams []types.StreamInterface) error {
	if !m.CDCSupport {
		return fmt.Errorf("invalid call; %s not running in CDC mode", m.Type())
	}

	// Index streams by ID for CDC routing
	if m.streams == nil {
		m.streams = make(map[string]types.StreamInterface, len(streams))
	}
	for _, s := range streams {
		m.streams[s.ID()] = s
	}

	initializeCDCState := func(ctx context.Context) error {
		lsn, err := m.currentMaxLSN(ctx)
		if err != nil {
			return fmt.Errorf("failed to get MSSQL current max LSN: %s", err)
		}
		m.state.SetGlobal(MSSQLGlobalState{LSN: lsn})
		m.state.ResetStreams()
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
	return nil
}

// StreamChanges polls changes via fn_cdc_get_all_changes_* for each table.
func (m *MSSQL) StreamChanges(ctx context.Context, _ types.StreamInterface, processFn abstract.CDCMsgFn) error {
	globalState := m.state.GetGlobal()
	if globalState == nil || globalState.State == nil {
		return fmt.Errorf("global CDC state not initialised")
	}

	var mssqlState MSSQLGlobalState
	if err := utils.Unmarshal(globalState.State, &mssqlState); err != nil {
		return fmt.Errorf("failed to unmarshal MSSQL global state: %s", err)
	}

	fromLSN := mssqlState.LSN // we get this from the state file
	startTime := time.Now()
	changesReceived := false // we set this to true when we receive at least one change

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Check if no changes received within initial wait time
			if !changesReceived && m.cdcConfig.InitialWaitTime > 0 && time.Since(startTime) > time.Duration(m.cdcConfig.InitialWaitTime)*time.Second {
				logger.Warnf("no records found in given initial wait time, try increasing it")
				// Ensure current LSN is persisted even when exiting early
				toLSN, err := m.currentMaxLSN(ctx)
				if err == nil {
					m.state.SetGlobal(MSSQLGlobalState{LSN: toLSN})
				}
				return nil
			}

			toLSN, err := m.currentMaxLSN(ctx)
			if err != nil {
				return fmt.Errorf("failed to get MSSQL max LSN: %s", err)
			}

			// If caught up to latest position
			if fromLSN >= toLSN {
				if changesReceived {
					return nil
				}
				continue
			}

			// Process changes between fromLSN and toLSN
			recordsProcessed, err := m.streamChangesOnce(ctx, fromLSN, toLSN, processFn)
			if err != nil {
				return err
			}

			// Only set changesReceived if records were actually processed
			if recordsProcessed > 0 {
				changesReceived = true
			}

			fromLSN = toLSN
			m.lastProcessedLSN = fromLSN
			m.state.SetGlobal(MSSQLGlobalState{LSN: fromLSN})
		}
	}
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

// streamChangesOnce reads changes for all CDC-enabled tables between fromLSN and toLSN.
// Returns the number of records processed.
func (m *MSSQL) streamChangesOnce(ctx context.Context, fromLSN, toLSN string, processFn abstract.CDCMsgFn) (int, error) {
	rows, err := m.client.QueryContext(ctx, jdbc.MSSQLCDCDiscoverQuery())
	if err != nil {
		return 0, fmt.Errorf("failed to query MSSQL CDC tables: %s", err)
	}
	defer rows.Close()

	type capture struct {
		schema string
		table  string
		inst   string
	}

	var captures []capture
	for rows.Next() {
		var c capture
		if err := rows.Scan(&c.schema, &c.table, &c.inst); err != nil {
			return 0, fmt.Errorf("failed to scan MSSQL CDC table: %s", err)
		}

		streamID := fmt.Sprintf("%s.%s", c.schema, c.table)
		if _, selected := m.streams[streamID]; !selected {
			continue
		}

		captures = append(captures, c)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	totalRecords := 0
	for _, capture := range captures {
		records, err := m.streamTableChanges(ctx, capture.schema, capture.table, capture.inst, fromLSN, toLSN, processFn)
		if err != nil {
			return totalRecords, err
		}
		totalRecords += records
	}
	return totalRecords, nil
}

func (m *MSSQL) streamTableChanges(ctx context.Context, schema, table, captureInstance, fromLSN, toLSN string, processFn abstract.CDCMsgFn) (int, error) {
	effectiveFromLSN, err := m.incrementLSN(ctx, fromLSN)
	if err != nil {
		return 0, err
	}

	// Convert hex LSN back to binary for function call.
	from, err := hex.DecodeString(effectiveFromLSN)
	if err != nil {
		return 0, fmt.Errorf("failed to parse fromLSN: %s", err)
	}
	to, err := hex.DecodeString(toLSN)
	if err != nil {
		return 0, fmt.Errorf("failed to parse toLSN: %s", err)
	}

	// Use direct function call with parameters - go-mssqldb handles binary parameters correctly
	query := jdbc.MSSQLCDCGetChangesQuery(captureInstance)

	logger.Infof("Streaming CDC changes for MSSQL table %s.%s between LSN %s and %s", schema, table, fromLSN, toLSN)

	rows, err := m.client.QueryContext(ctx, query, from, to)
	if err != nil {
		return 0, fmt.Errorf("failed to query MSSQL CDC changes: %s", err)
	}
	defer rows.Close()

	recordsProcessed := 0
	for rows.Next() {
		// Use MapScan to properly convert data types including binary types
		record := make(map[string]interface{})
		if err := jdbc.MapScan(rows, record, m.dataTypeConverter); err != nil {
			return recordsProcessed, fmt.Errorf("failed to scan MSSQL CDC row: %s", err)
		}

		var opKind string
		// SQL Server CDC returns two rows for updates:
		// - 3 = update (before image) - skip this
		// - 4 = update (after image) - process this
		if opVal, ok := record["__$operation"]; ok {
			switch v := opVal.(type) {
			case int32:
				if int(v) == 3 {
					continue
				}
				opKind = operationFromCode(int(v))
			case int64:
				if int(v) == 3 {
					continue
				}
				opKind = operationFromCode(int(v))
			case int:
				if v == 3 {
					continue
				}
				opKind = operationFromCode(v)
			default:
				opKind = "update"
			}
		}

		// Remove technical columns from payload
		delete(record, "__$operation")
		delete(record, "__$start_lsn")
		delete(record, "__$seqval")
		delete(record, "__$update_mask")

		streamID := fmt.Sprintf("%s.%s", schema, table)
		cfgStream, ok := m.streams[streamID]
		if !ok {
			logger.Warnf("skipping CDC change for unmapped MSSQL stream %s", streamID)
			continue
		}

		if err := processFn(ctx, abstract.CDCChange{
			Stream:    cfgStream,
			Timestamp: time.Now().UTC(),
			Kind:      opKind,
			Data:      record,
		}); err != nil {
			return recordsProcessed, fmt.Errorf("failed to process MSSQL CDC change: %s", err)
		}

		recordsProcessed++
	}
	if err := rows.Err(); err != nil {
		return recordsProcessed, err
	}
	return recordsProcessed, nil
}

func (m *MSSQL) incrementLSN(ctx context.Context, hexLSN string) (string, error) {
	lsnBytes, err := hex.DecodeString(hexLSN)
	if err != nil {
		return "", fmt.Errorf("failed to parse LSN for increment: %s", err)
	}

	var next []byte
	if err := m.client.QueryRowContext(ctx, jdbc.MSSQLCDCIncrementLSNQuery(), lsnBytes).Scan(&next); err != nil {
		return "", fmt.Errorf("failed to increment LSN: %s", err)
	}
	if len(next) == 0 {
		return "", fmt.Errorf("incremented LSN is empty")
	}
	return hex.EncodeToString(next), nil
}

func operationFromCode(code int) string {
	// SQL Server CDC operation codes:
	// 1 = delete, 2 = insert, 3/4 = update (before/after)
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
