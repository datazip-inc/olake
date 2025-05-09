package binlog

import (
	"fmt"
	"time"

	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/typeutils"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

// ChangeFilter filters binlog events based on the specified streams.
type ChangeFilter struct {
	streams   map[string]protocol.Stream // Keyed by "schema.table"
	converter func(value interface{}, columnType string) (interface{}, error)
}

// NewChangeFilter creates a filter for the given streams.
func NewChangeFilter(typeConverter func(value interface{}, columnType string) (interface{}, error), streams ...protocol.Stream) ChangeFilter {
	filter := ChangeFilter{
		converter: typeConverter,
		streams:   make(map[string]protocol.Stream),
	}
	for _, stream := range streams {
		filter.streams[fmt.Sprintf("%s.%s", stream.Namespace(), stream.Name())] = stream
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

	columnTypes := make([]string, len(e.Table.ColumnType))
	for i, ct := range e.Table.ColumnType {
		columnTypes[i] = mysqlTypeName(ct)
	}
	logger.Debugf("Column types: %v", columnTypes)

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
		record, err := convertRowToMap(row, e.Table.ColumnNameString(), columnTypes, f.converter)
		if err != nil {
			return err
		}
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
func convertRowToMap(row []interface{}, columns []string, columnTypes []string, converter func(value interface{}, columnType string) (interface{}, error)) (map[string]interface{}, error) {
	if len(columns) != len(row) {
		return nil, fmt.Errorf("column count mismatch: expected %d, got %d", len(columns), len(row))
	}

	record := make(map[string]interface{})
	for i, val := range row {
		convertedVal, err := converter(val, columnTypes[i])
		if err != nil && err != typeutils.ErrNullValue {
			return nil, err
		}
		record[columns[i]] = convertedVal
	}
	return record, nil
}

func mysqlTypeName(t byte) string {
	switch t {
	case mysql.MYSQL_TYPE_DECIMAL:
		return "DECIMAL"
	case mysql.MYSQL_TYPE_TINY:
		return "TINYINT"
	case mysql.MYSQL_TYPE_SHORT:
		return "SMALLINT"
	case mysql.MYSQL_TYPE_LONG:
		return "INT"
	case mysql.MYSQL_TYPE_FLOAT:
		return "FLOAT"
	case mysql.MYSQL_TYPE_DOUBLE:
		return "DOUBLE"
	case mysql.MYSQL_TYPE_NULL:
		return "NULL"
	case mysql.MYSQL_TYPE_TIMESTAMP:
		return "TIMESTAMP"
	case mysql.MYSQL_TYPE_LONGLONG:
		return "BIGINT"
	case mysql.MYSQL_TYPE_INT24:
		return "MEDIUMINT"
	case mysql.MYSQL_TYPE_DATE:
		return "DATE"
	case mysql.MYSQL_TYPE_TIME:
		return "TIME"
	case mysql.MYSQL_TYPE_DATETIME:
		return "DATETIME"
	case mysql.MYSQL_TYPE_YEAR:
		return "YEAR"
	case mysql.MYSQL_TYPE_VARCHAR:
		return "VARCHAR"
	case mysql.MYSQL_TYPE_BIT:
		return "BIT"
	case mysql.MYSQL_TYPE_JSON:
		return "JSON"
	case mysql.MYSQL_TYPE_NEWDECIMAL:
		return "DECIMAL"
	case mysql.MYSQL_TYPE_ENUM:
		return "ENUM"
	case mysql.MYSQL_TYPE_SET:
		return "SET"
	case mysql.MYSQL_TYPE_TINY_BLOB:
		return "TINYBLOB"
	case mysql.MYSQL_TYPE_BLOB:
		return "BLOB"
	case mysql.MYSQL_TYPE_MEDIUM_BLOB:
		return "MEDIUMBLOB"
	case mysql.MYSQL_TYPE_LONG_BLOB:
		return "LONGBLOB"
	case mysql.MYSQL_TYPE_STRING:
		return "STRING"
	case mysql.MYSQL_TYPE_GEOMETRY:
		return "GEOMETRY"
	default:
		return fmt.Sprintf("UNKNOWN_TYPE: %d", t)
	}
}
