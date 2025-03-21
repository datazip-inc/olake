package binlog

import (
	"time"

	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/protocol"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

// ChangeFilter filters binlog events based on the specified streams.
type ChangeFilter struct {
	streams map[string]protocol.Stream // Keyed by "schema.table"
}

// NewChangeFilter creates a filter for the given streams.
func NewChangeFilter(streams ...protocol.Stream) ChangeFilter {
	filter := ChangeFilter{
		streams: make(map[string]protocol.Stream),
	}
	for _, stream := range streams {
		filter.streams[stream.Namespace()+"."+stream.Name()] = stream
	}
	return filter
}

// FilterRowsEvent processes RowsEvent and calls the callback for matching streams.
func (f ChangeFilter) FilterRowsEvent(e *replication.RowsEvent, ev *replication.BinlogEvent, callback OnChange) error {
	schemaName := string(e.Table.Schema)
	tableName := string(e.Table.Table)
	stream, exists := f.streams[schemaName+"."+tableName]
	if !exists {
		return nil
	}

	var operationType string
	switch ev.Header.EventType {
	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		operationType = "insert"
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		operationType = "update"
	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		operationType = "delete"
	default:
		return nil
	}

	var rowsToProcess [][]interface{}
	if operationType == "update" {
		for i := 1; i < len(e.Rows); i += 2 {
			rowsToProcess = append(rowsToProcess, e.Rows[i]) // Take after-images for updates
		}
	} else {
		rowsToProcess = e.Rows
	}

	for _, row := range rowsToProcess {
		record := convertRowToMap(row, e.Table.ColumnNameString())
		if record == nil {
			continue
		}
		record["cdc_type"] = operationType

		change := CDCChange{
			Stream:    stream,
			Timestamp: time.Unix(int64(ev.Header.Timestamp), 0),
			Position:  mysql.Position{}, // Position will be set in StreamMessages
			Kind:      operationType,
			Schema:    schemaName,
			Table:     tableName,
			Data:      record,
		}
		if err := callback(change); err != nil {
			return err
		}
	}
	return nil
}

// convertRowToMap converts a binlog row to a map.
func convertRowToMap(row []interface{}, columns []string) map[string]interface{} {
	if len(columns) != len(row) {
		logger.Errorf("Column count mismatch: expected %d, got %d", len(columns), len(row))
		return nil
	}
	record := make(map[string]interface{})
	for i, val := range row {
		record[columns[i]] = val
	}
	return record
}
