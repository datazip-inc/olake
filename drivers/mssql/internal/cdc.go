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

const (
	// cdcPollingDelay is the delay between polling cycles when no changes are detected
	cdcPollingDelay = 500 * time.Millisecond
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

	fromLSN := mssqlState.LSN
	startTime := time.Now()
	changesReceived := false

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

			// If caught up to latest position, wait before polling again
			if fromLSN == toLSN {
				time.Sleep(cdcPollingDelay)
				continue
			}

			// Process changes between fromLSN and toLSN
			if err := m.streamChangesOnce(ctx, fromLSN, toLSN, processFn); err != nil {
				return err
			}

			changesReceived = true
			fromLSN = toLSN
			m.state.SetGlobal(MSSQLGlobalState{LSN: fromLSN})
		}
	}
}

// PostCDC persists final LSN
func (m *MSSQL) PostCDC(ctx context.Context, _ types.StreamInterface, noErr bool, _ string) error {
	if !noErr {
		return nil
	}

	globalState := m.state.GetGlobal()
	if globalState != nil && globalState.State != nil {
		var mssqlState MSSQLGlobalState
		if err := utils.Unmarshal(globalState.State, &mssqlState); err == nil {
			// If LSN is empty or we want to ensure it's current, get max LSN
			if mssqlState.LSN == "" {
				lsn, err := m.currentMaxLSN(ctx)
				if err == nil {
					m.state.SetGlobal(MSSQLGlobalState{LSN: lsn})
				}
			} else {
				// State already updated in StreamChanges loop, but ensure it's persisted
				m.state.SetGlobal(MSSQLGlobalState{LSN: mssqlState.LSN})
			}
		}
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
func (m *MSSQL) streamChangesOnce(ctx context.Context, fromLSN, toLSN string, processFn abstract.CDCMsgFn) error {
	rows, err := m.client.QueryContext(ctx, jdbc.MSSQLCDCDiscoverQuery())
	if err != nil {
		return fmt.Errorf("failed to query MSSQL CDC tables: %s", err)
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
			return fmt.Errorf("failed to scan MSSQL CDC table: %s", err)
		}

		streamID := fmt.Sprintf("%s.%s", c.schema, c.table)
		if _, selected := m.streams[streamID]; !selected {
			continue
		}

		captures = append(captures, c)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, cap := range captures {
		if err := m.streamTableChanges(ctx, cap.schema, cap.table, cap.inst, fromLSN, toLSN, processFn); err != nil {
			return err
		}
	}
	return nil
}

func (m *MSSQL) streamTableChanges(ctx context.Context, schema, table, captureInstance, fromLSN, toLSN string, processFn abstract.CDCMsgFn) error {
	// Convert hex LSN back to binary for function call.
	from, err := hex.DecodeString(fromLSN)
	if err != nil {
		return fmt.Errorf("failed to parse fromLSN: %s", err)
	}
	to, err := hex.DecodeString(toLSN)
	if err != nil {
		return fmt.Errorf("failed to parse toLSN: %s", err)
	}

	// Use direct function call with parameters - go-mssqldb handles binary parameters correctly
	query := jdbc.MSSQLCDCGetChangesQuery(captureInstance)

	logger.Infof("Streaming CDC changes for MSSQL table %s.%s between LSN %s and %s", schema, table, fromLSN, toLSN)

	rows, err := m.client.QueryContext(ctx, query, from, to)
	if err != nil {
		return fmt.Errorf("failed to query MSSQL CDC changes: %s", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get MSSQL CDC columns: %s", err)
	}

	for rows.Next() {
		raw := make([]interface{}, len(cols))
		dest := make([]interface{}, len(cols))
		for i := range raw {
			dest[i] = &raw[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return fmt.Errorf("failed to scan MSSQL CDC row: %s", err)
		}

		record := make(map[string]interface{})
		var opKind string
		for i, name := range cols {
			val := raw[i]
			switch name {
			case "__$operation":
				switch v := val.(type) {
				case int32:
					opKind = operationFromCode(int(v))
				case int64:
					opKind = operationFromCode(int(v))
				default:
					opKind = "update"
				}
			case "__$start_lsn", "__$seqval", "__$update_mask":
				// keep technical columns out of payload
				continue
			default:
				record[name] = val
			}
		}

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
			return fmt.Errorf("failed to process MSSQL CDC change: %s", err)
		}
	}
	return rows.Err()
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
