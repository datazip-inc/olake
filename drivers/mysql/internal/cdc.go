package driver

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

const (
	cdcPositionKey   = "binlog_position"
	cdcFileKey       = "binlog_file"
	cdcServerIDKey   = "server_id"
	maxRetries       = 3
	retryDelay       = 5 * time.Second
	heartbeatPeriod  = 30 * time.Second
	eventReadTimeout = 500 * time.Millisecond // Short timeout to check master position frequently
)

// RunChangeStream implements the CDC functionality for multiple streams concurrently
func (m *MySQL) RunChangeStream(pool *protocol.WriterPool, streams ...protocol.Stream) error {
	// Use the same concurrent approach as MongoDB connector
	return utils.Concurrent(context.TODO(), streams, len(streams), func(ctx context.Context, stream protocol.Stream, executionNumber int) error {
		return m.changeStreamSync(stream, pool)
	})
}

// SetupGlobalState initializes the state for CDC
func (m *MySQL) SetupGlobalState(state *types.State) error {
	// MySQL uses stream-level state similar to MongoDB
	return nil
}

// changeStreamSync implements CDC for a specific stream
func (m *MySQL) changeStreamSync(stream protocol.Stream, pool *protocol.WriterPool) error {
	ctx := context.TODO()
	// Get or initialize binlog position
	var pos mysql.Position
	serverID := uint32(1000 + time.Now().UnixNano()%9000) // Default server ID

	// Check if we have position in state
	hasBinlogPosition := false
	// Check if we have a position in state
	if posStr := m.State.GetCursor(stream.Self(), cdcPositionKey); posStr != nil {
		switch v := posStr.(type) {
		case float64:
			pos.Pos = uint32(v)
		case string:
			if posInt, err := strconv.ParseUint(v, 10, 32); err == nil {
				pos.Pos = uint32(posInt)
			}
		}
		hasBinlogPosition = true
	}

	// Check if we have a file position in state
	if fileStr, ok := m.State.GetCursor(stream.Self(), cdcFileKey).(string); ok {

		pos.Name = fileStr
		hasBinlogPosition = true
	}

	if serverIDStr := m.State.GetCursor(stream.Self(), cdcServerIDKey); serverIDStr != nil {
		switch v := serverIDStr.(type) {
		case float64:
			serverID = uint32(v)
		case string:
			if idVal, err := strconv.ParseUint(v, 10, 32); err == nil {
				serverID = uint32(idVal)
			}
		}
	} else {
		// Save server ID to state
		m.State.SetCursor(stream.Self(), cdcServerIDKey, serverID)
	}

	chunks := m.State.GetChunks(stream.Self())

	// If no position or chunks remaining, do backfill first
	if !hasBinlogPosition || chunks == nil || chunks.Len() != 0 {
		var err error
		// Get current binlog position
		pos, err = m.getCurrentBinlogPosition()
		if err != nil {
			return fmt.Errorf("failed to get current binlog position: %w", err)
		}

		// Save position to state
		m.State.SetCursor(stream.Self(), cdcFileKey, pos.Name)
		m.State.SetCursor(stream.Self(), cdcPositionKey, pos.Pos)

		// Perform backfill
		if err := m.backfill(pool, stream); err != nil {
			return fmt.Errorf("failed to backfill stream %s: %w", stream.ID(), err)
		}

		logger.Infof("Backfill done for stream[%s]", stream.ID())
	}

	// Start CDC from the saved position
	logger.Infof("Starting MySQL CDC for stream[%s] from binlog position %s:%d", stream.ID(), pos.Name, pos.Pos)

	// Configure binlog syncer
	syncerConfig := replication.BinlogSyncerConfig{
		ServerID:        serverID,
		Flavor:          "mysql",
		Host:            m.config.Hosts[0],
		Port:            uint16(m.config.Port),
		User:            m.config.Username,
		Password:        m.config.Password,
		Charset:         "utf8mb4",
		VerifyChecksum:  true,
		HeartbeatPeriod: heartbeatPeriod,
	}

	syncer := replication.NewBinlogSyncer(syncerConfig)
	defer syncer.Close()

	streamer, err := syncer.StartSync(pos)
	if err != nil {
		return fmt.Errorf("failed to start binlog sync: %w", err)
	}

	waitChannel := make(chan error, 1)
	insert, err := pool.NewThread(ctx, stream, protocol.WithErrorChannel(waitChannel))
	if err != nil {
		return err
	}
	defer insert.Close()

	currentPos := pos        // Initialize current position
	consecutiveTimeouts := 0 // Track consecutive timeouts
	for {
		// Get the next event with a very short timeout
		eventCtx, cancel := context.WithTimeout(context.Background(), eventReadTimeout)
		ev, err := streamer.GetEvent(eventCtx)
		cancel()

		if err != nil {
			if err == context.DeadlineExceeded {
				// Timeout - immediately check if we've caught up to master
				consecutiveTimeouts++
				if consecutiveTimeouts >= 2 {
					consecutiveTimeouts = 0
					masterPos, err := m.getCurrentBinlogPosition()
					if err != nil {
						logger.Errorf("Failed to get master status: %v", err)
						continue
					}

					if currentPos.Name == masterPos.Name && currentPos.Pos == masterPos.Pos {
						logger.Infof("Caught up to master position %s:%d, completing CDC for stream[%s]",
							masterPos.Name, masterPos.Pos, stream.ID())
						break
					}
				}
				continue
			}

			// Handle other errors with retry
			logger.Errorf("Error getting binlog event: %v, retrying...", err)
			success := false
			for i := 0; i < maxRetries; i++ {
				time.Sleep(retryDelay)
				eventCtx, cancel := context.WithTimeout(context.Background(), eventReadTimeout)
				ev, err = streamer.GetEvent(eventCtx)
				cancel()
				if err == nil {
					success = true
					break
				}
				logger.Errorf("Retry %d failed: %v", i+1, err)
			}
			if !success {
				return fmt.Errorf("failed to recover after %d retries: %w", maxRetries, err)
			}
		}

		// Reset consecutive timeouts since we got an event
		consecutiveTimeouts = 0

		// Update current position after processing the event
		currentPos.Pos = ev.Header.LogPos

		// Handle different event types
		switch e := ev.Event.(type) {
		case *replication.RotateEvent:
			currentPos.Name = string(e.NextLogName)
			currentPos.Pos = uint32(e.Position)
			logger.Infof("Binlog rotated to %s:%d for stream[%s]", currentPos.Name, currentPos.Pos, stream.ID())

		case *replication.RowsEvent:
			schemaName := string(e.Table.Schema)
			tableName := string(e.Table.Table)

			if schemaName == stream.Namespace() && tableName == stream.Name() {
				var operationType string
				switch ev.Header.EventType {
				case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
					operationType = "insert"
				case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
					operationType = "update"
				case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
					operationType = "delete"
				default:
					continue
				}

				var rowsToProcess [][]interface{}
				if operationType == "update" {
					for i := 1; i < len(e.Rows); i += 2 {
						rowsToProcess = append(rowsToProcess, e.Rows[i])
					}
				} else {
					rowsToProcess = e.Rows
				}

				for _, row := range rowsToProcess {
					record := m.convertRowToMap(stream, row, e.Table.ColumnNameString(), operationType)
					if record == nil {
						continue
					}

					pkColumn := getPrimaryKeyColumn(m.db, tableName)
					recordHash := utils.GetKeysHash(record, pkColumn)
					//rawRecord := types.CreateRawRecord(recordHash, record, time.Now().UnixNano())
					err = insert.Insert(types.CreateRawRecord(recordHash, record, time.Now().UnixNano()))
					if err != nil {
						return fmt.Errorf("failed to insert record: %w", err)
					}
				}
			}
		}

		// Save current position after each event
		m.State.SetCursor(stream.Self(), cdcFileKey, currentPos.Name)
		m.State.SetCursor(stream.Self(), cdcPositionKey, currentPos.Pos)
	}

	// Final save of position before returning
	m.State.SetCursor(stream.Self(), cdcFileKey, currentPos.Name)
	m.State.SetCursor(stream.Self(), cdcPositionKey, currentPos.Pos)

	logger.Infof("Completed CDC processing for stream[%s] at position %s:%d",
		stream.ID(), currentPos.Name, currentPos.Pos)

	return nil
}

// getCurrentBinlogPosition retrieves the current binlog position from MySQL
// getCurrentBinlogPosition retrieves the current binlog position from MySQL
func (m *MySQL) getCurrentBinlogPosition() (mysql.Position, error) {
	rows, err := m.db.Query(jdbc.MySQLMasterStatusQuery())
	if err != nil {
		return mysql.Position{}, fmt.Errorf("failed to get master status: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return mysql.Position{}, fmt.Errorf("no binlog position available")
	}

	var file string
	var position uint32
	var binlogDoDB, binlogIgnoreDB, executeGtidSet string

	if err := rows.Scan(&file, &position, &binlogDoDB, &binlogIgnoreDB, &executeGtidSet); err != nil {
		return mysql.Position{}, fmt.Errorf("failed to scan binlog position: %w", err)
	}

	return mysql.Position{Name: file, Pos: position}, nil
}

// Updated convertRowToMap function
func (m *MySQL) convertRowToMap(stream protocol.Stream, row []interface{}, columns []string, operation string) map[string]interface{} {
	// If no column names available from binlog, fall back to database query
	if len(columns) == 0 || columns[0] == "" {
		query := jdbc.MySQLTableColumnsQuery()
		rows, err := m.db.Query(query, stream.Namespace(), stream.Name())
		if err != nil {
			logger.Errorf("Failed to get columns for table %s.%s: %v", stream.Namespace(), stream.Name(), err)
			return nil
		}
		defer rows.Close()

		columns = []string{}
		for rows.Next() {
			var colName string
			if err := rows.Scan(&colName); err != nil {
				logger.Errorf("Failed to scan column name: %v", err)
				continue
			}
			columns = append(columns, colName)
		}
	}

	if len(columns) != len(row) {
		logger.Errorf("Column count mismatch: expected %d, got %d", len(columns), len(row))
		return nil
	}

	// Convert to map
	record := make(map[string]interface{})
	for i, val := range row {
		if i < len(columns) {
			record[columns[i]] = val
		}
	}

	record["cdc_type"] = operation

	return record
}
