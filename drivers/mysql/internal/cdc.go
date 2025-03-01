package driver

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/datazip-inc/olake/logger"
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

type CDCRowEvent struct {
	OperationType string
	TableName     string
	SchemaName    string
	RowData       map[string]interface{}
}

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

// StateType returns the type of state used by the MySQL driver
func (m *MySQL) StateType() types.StateType {
	return types.StreamType
}

// changeStreamSync implements CDC for a specific stream
func (m *MySQL) changeStreamSync(stream protocol.Stream, pool *protocol.WriterPool) error {
	// Get or initialize binlog position
	var pos mysql.Position
	serverID := uint32(1000 + time.Now().UnixNano()%9000) // Default server ID

	// Check if we have position in state
	hasBinlogPosition := false
	if posStr := stream.GetStateKey(cdcPositionKey); posStr != nil {
		if posVal, ok := posStr.(float64); ok {
			pos.Pos = uint32(posVal)
			hasBinlogPosition = true
		} else if posStrVal, ok := posStr.(string); ok {
			if posInt, err := strconv.ParseUint(posStrVal, 10, 32); err == nil {
				pos.Pos = uint32(posInt)
				hasBinlogPosition = true
			}
		}
	}

	if fileStr := stream.GetStateKey(cdcFileKey); fileStr != nil {
		if fileStrVal, ok := fileStr.(string); ok {
			pos.Name = fileStrVal
			hasBinlogPosition = true
		}
	}

	if serverIDStr := stream.GetStateKey(cdcServerIDKey); serverIDStr != nil {
		if idVal, ok := serverIDStr.(float64); ok {
			serverID = uint32(idVal)
		} else if idStr, ok := serverIDStr.(string); ok {
			if idVal, err := strconv.ParseUint(idStr, 10, 32); err == nil {
				serverID = uint32(idVal)
			}
		}
	} else {
		// Save server ID to state
		stream.SetStateKey(cdcServerIDKey, serverID)
	}

	chunks := stream.GetStateChunks()

	// If no position or chunks remaining, do backfill first
	if !hasBinlogPosition || chunks == nil || chunks.Len() != 0 {
		var err error
		// Get current binlog position
		pos, err = m.getCurrentBinlogPosition()
		if err != nil {
			return fmt.Errorf("failed to get current binlog position: %w", err)
		}

		// Save position to state
		stream.SetStateKey(cdcFileKey, pos.Name)
		stream.SetStateKey(cdcPositionKey, pos.Pos)

		// Perform backfill
		if err := m.backfill(stream, pool); err != nil {
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

	insert, err := pool.NewThread(context.TODO(), stream)
	if err != nil {
		return err
	}
	defer insert.Close()

	currentPos := pos           // Initialize current position
	consecutiveTimeouts := 0    // Track consecutive timeouts
	maxConsecutiveTimeouts := 2 // Number of consecutive timeouts to trigger a master position check

	for {
		// Get the next event with a very short timeout
		eventCtx, cancel := context.WithTimeout(context.Background(), eventReadTimeout)
		ev, err := streamer.GetEvent(eventCtx)
		cancel()

		if err != nil {
			if err == context.DeadlineExceeded {
				// Timeout - immediately check if we've caught up to master
				consecutiveTimeouts++
				if consecutiveTimeouts >= maxConsecutiveTimeouts {
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
					record := m.convertRowToMap(stream, row, operationType)
					if record == nil {
						continue
					}

					pkColumn := getPrimaryKeyColumn(m.db, tableName)
					recordHash := utils.GetKeysHash(record, pkColumn)
					rawRecord := types.CreateRawRecord(recordHash, record, time.Now().UnixNano())
					exit, err := insert.Insert(rawRecord)
					if err != nil {
						return fmt.Errorf("failed to insert record: %w", err)
					}
					if exit {
						stream.SetStateKey(cdcFileKey, currentPos.Name)
						stream.SetStateKey(cdcPositionKey, currentPos.Pos)
						return nil
					}
				}
			}
		}

		// Save current position after each event
		stream.SetStateKey(cdcFileKey, currentPos.Name)
		stream.SetStateKey(cdcPositionKey, currentPos.Pos)
	}

	// Final save of position before returning
	stream.SetStateKey(cdcFileKey, currentPos.Name)
	stream.SetStateKey(cdcPositionKey, currentPos.Pos)

	logger.Infof("Completed CDC processing for stream[%s] at position %s:%d",
		stream.ID(), currentPos.Name, currentPos.Pos)

	return nil
}

// getCurrentBinlogPosition retrieves the current binlog position from MySQL
func (m *MySQL) getCurrentBinlogPosition() (mysql.Position, error) {
	var pos mysql.Position

	rows, err := m.db.Query("SHOW MASTER STATUS")
	if err != nil {
		return pos, fmt.Errorf("failed to get master status: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return pos, fmt.Errorf("no binlog position available")
	}

	var file string
	var position uint32
	var binlogDoDB, binlogIgnoreDB string
	var executeGtidSet string

	// Try to scan with or without the GTID column
	err = rows.Scan(&file, &position, &binlogDoDB, &binlogIgnoreDB, &executeGtidSet)
	if err != nil {
		// Try without GTID set
		rows, err = m.db.Query("SHOW MASTER STATUS")
		if err != nil {
			return pos, fmt.Errorf("failed to retry master status: %w", err)
		}
		defer rows.Close()

		if !rows.Next() {
			return pos, fmt.Errorf("no binlog position available on retry")
		}

		err = rows.Scan(&file, &position, &binlogDoDB, &binlogIgnoreDB)
		if err != nil {
			return pos, fmt.Errorf("failed to scan binlog position: %w", err)
		}
	}

	pos.Name = file
	pos.Pos = position

	return pos, nil
}

// convertRowToMap converts a binary row to a map using the stream schema
func (m *MySQL) convertRowToMap(stream protocol.Stream, row []interface{}, operation string) map[string]interface{} {
	// Get table columns
	query := "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION"
	rows, err := m.db.Query(query, stream.Namespace(), stream.Name())
	if err != nil {
		logger.Errorf("Failed to get columns for table %s.%s: %v", stream.Namespace(), stream.Name(), err)
		return nil
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var colName string
		if err := rows.Scan(&colName); err != nil {
			logger.Errorf("Failed to scan column name: %v", err)
			continue
		}
		columns = append(columns, colName)
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

	// Add operation type field (similar to MongoDB's cdc_type)
	record["cdc_type"] = operation

	return record
}

// Close ensures proper cleanup
func (m *MySQL) Close() error {
	if m.db != nil {
		return m.db.Close()
	}
	return nil
}
