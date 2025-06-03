package binlog

import (
	"fmt"
	"time"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/go-mysql-org/go-mysql/replication"
)

// ChangeFilter filters binlog events based on the specified streams.
type ChangeFilter struct {
	streams map[string]types.StreamInterface // Keyed by "schema.table"
}

// NewChangeFilter creates a filter for the given streams.
func NewChangeFilter(streams ...types.StreamInterface) ChangeFilter {
	filter := ChangeFilter{
		streams: make(map[string]types.StreamInterface),
	}
	for _, stream := range streams {
		filter.streams[fmt.Sprintf("%s.%s", stream.Namespace(), stream.Name())] = stream
	}
	return filter
}

// FilterRowsEvent processes RowsEvent and calls the callback for matching streams.
func (f ChangeFilter) FilterRowsEvent(e *replication.RowsEvent, ev *replication.BinlogEvent, callback abstract.CDCMsgFn) error {
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
		// For an "update" operation, the rows contain pairs of (before, after) images: [before, after, before, after, ...]
		// We start from the second element (i=1) and step by 2 to get the "after" row (the updated state).
		for i := 1; i < len(e.Rows); i += 2 {
			rowsToProcess = append(rowsToProcess, e.Rows[i]) // Take after-images for updates
		}
	} else {
		rowsToProcess = e.Rows
	}

	for _, row := range rowsToProcess {
		record, err := convertRowToMap(row, e.Table.ColumnNameString())
		if err != nil {
			return err
		}
		if record == nil {
			continue
		}
		change := abstract.CDCChange{
			Stream:    stream,
			Timestamp: typeutils.Time{Time: time.Unix(int64(ev.Header.Timestamp), 0)},
			Kind:      operationType,
			Data:      record,
		}
		if err := callback(change); err != nil {
			return err
		}
	}
	return nil
}

// convertRowToMap converts a binlog row to a map.
func convertRowToMap(row []interface{}, columns []string) (map[string]interface{}, error) {
	if len(columns) != len(row) {
		return nil, fmt.Errorf("column count mismatch: expected %d, got %d", len(columns), len(row))
	}
	record := make(map[string]interface{})
	for i, val := range row {
		record[columns[i]] = val
	}
	return record, nil
}
