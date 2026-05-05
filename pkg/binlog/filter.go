package binlog

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/unicode"
)

const (
	CDCBinlogFileName = "_cdc_binlog_file_name" // MySQL binlog file name
	CDCBinlogFilePos  = "_cdc_binlog_file_pos"  // MySQL binlog file position

)

// TableMapEvent wraps replication.TableMapEvent so we can define receiver methods (unsignedMap, isNumericColumn).
type TableMapEvent struct {
	*replication.TableMapEvent
}

// ChangeFilter filters binlog events based on the specified streams.
type ChangeFilter struct {
	streams       map[string]types.StreamInterface // Keyed by "schema.table"
	converter     func(value interface{}, columnType string) (interface{}, error)
	lastGTIDEvent time.Time
}

// NewChangeFilter creates a filter for the given streams.
func NewChangeFilter(typeConverter func(value interface{}, columnType string) (interface{}, error), streams ...types.StreamInterface) ChangeFilter {
	filter := ChangeFilter{
		streams:   make(map[string]types.StreamInterface),
		converter: typeConverter,
	}
	for _, stream := range streams {
		filter.streams[fmt.Sprintf("%s.%s", stream.Namespace(), stream.Name())] = stream
	}
	return filter
}

// FilterRowsEvent processes RowsEvent and calls the callback for matching streams.
func (f ChangeFilter) FilterRowsEvent(ctx context.Context, e *replication.RowsEvent, ev *replication.BinlogEvent, pos mysql.Position, callback abstract.CDCMsgFn) error {
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

	unsignedMap := (&TableMapEvent{e.Table}).unsignedMap()
	columnTypes := make([]string, len(e.Table.ColumnType))
	for i, ct := range e.Table.ColumnType {
		columnTypes[i] = mysqlTypeName(ct, unsignedMap != nil && unsignedMap[i])
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
		record, err := convertRowToMap(row, e.Table, columnTypes, f.converter)
		if err != nil {
			return err
		}
		if record == nil {
			continue
		}

		// Use microsecond-precision timestamp from GTID event (MySQL 8.0.1+) if available,
		// otherwise fall back to second-precision header timestamp
		timestamp := utils.Ternary(!f.lastGTIDEvent.IsZero(), f.lastGTIDEvent, time.Unix(int64(ev.Header.Timestamp), 0)).(time.Time)

		change := abstract.CDCChange{
			Stream:    stream,
			Timestamp: timestamp,
			Kind:      operationType,
			Data:      record,
			ExtraColumns: map[string]any{
				CDCBinlogFileName: pos.Name,
				CDCBinlogFilePos:  pos.Pos, // Use the event position
			},
		}
		if err := callback(ctx, change); err != nil {
			return err
		}
	}
	return nil
}

// convertRowToMap converts a binlog row to a map.
func convertRowToMap(row []interface{}, tableMap *replication.TableMapEvent, columnTypes []string, converter func(value interface{}, columnType string) (interface{}, error)) (map[string]interface{}, error) {
	columns := tableMap.ColumnNameString()
	if len(columns) != len(row) {
		return nil, fmt.Errorf("column count mismatch: expected %d, got %d", len(columns), len(row))
	}

	enumRaw := tableMap.EnumStrValue                   // [][][]byte: one entry per ENUM column
	enumCollationMap := tableMap.EnumSetCollationMap() // col idx -> collation ID for ENUM/SET
	collationMap := tableMap.CollationMap()
	enumP := 0 // index into enumRaw; advances only for ENUM columns

	// NOTE: For MySQL CDC (binlog-based), FLOAT values are read directly from the binlog and may
	// differ from SELECT output due to SQL-layer formatting/rounding.
	record := make(map[string]interface{})
	for i, val := range row {
		if tableMap.IsEnumColumn(i) {
			// for an update CDC event, the key of enum value is passed in binlog events which is always in int64
			// during such a case, we need to find out the enum value of it from the index

			if val != nil && enumP < len(enumRaw) {
				if idx, isInt64 := val.(int64); isInt64 {
					// MySQL stores invalid ENUM inserts as index 0 (special error value), which maps to empty string.
					val = ""
					if idx > 0 {
						raw := enumRaw[enumP][idx-1]
						if s, decErr := decodeBytesToString(raw, enumCollationMap[i]); decErr == nil {
							val = s
						} else {
							val = string(raw) // fallback
						}
					}
				}
			}
			enumP++ // always advance, even for NULL values, to keep p in sync with EnumStrValue
		} else if b, isByte := val.([]byte); isByte {
			// Decode raw binlog bytes to UTF-8 using the column's collation.
			// CollationMap only covers character columns, so binary BLOBs are left untouched.
			if collID, exists := collationMap[i]; exists {
				if decoded, decErr := decodeBytesToString(b, collID); decErr == nil {
					val = decoded
				}
			}
		}

		convertedVal, err := converter(val, columnTypes[i])
		if err != nil && err != typeutils.ErrNullValue {
			return nil, err
		}
		record[columns[i]] = convertedVal
	}
	return record, nil
}

// mysqlTypeName maps MySQL binlog protocol type bytes to SQL type names.
func mysqlTypeName(t byte, unsigned bool) string {
	switch t {
	case mysql.MYSQL_TYPE_DECIMAL:
		return "DECIMAL"
	case mysql.MYSQL_TYPE_TINY:
		if unsigned {
			return "UNSIGNED TINYINT"
		}
		return "TINYINT"
	case mysql.MYSQL_TYPE_SHORT:
		if unsigned {
			return "UNSIGNED SMALLINT"
		}
		return "SMALLINT"
	case mysql.MYSQL_TYPE_LONG:
		if unsigned {
			return "UNSIGNED INT"
		}
		return "INT"
	case mysql.MYSQL_TYPE_FLOAT:
		return "FLOAT"
	case mysql.MYSQL_TYPE_DOUBLE:
		return "DOUBLE"
	case mysql.MYSQL_TYPE_NULL:
		return "NULL"
	case mysql.MYSQL_TYPE_TIMESTAMP, mysql.MYSQL_TYPE_TIMESTAMP2:
		return "TIMESTAMP"
	case mysql.MYSQL_TYPE_LONGLONG:
		if unsigned {
			return "UNSIGNED BIGINT"
		}
		return "BIGINT"
	case mysql.MYSQL_TYPE_INT24:
		if unsigned {
			return "UNSIGNED MEDIUMINT"
		}
		return "MEDIUMINT"
	case mysql.MYSQL_TYPE_DATE:
		return "DATE"
	case mysql.MYSQL_TYPE_TIME, mysql.MYSQL_TYPE_TIME2:
		return "TIME"
	case mysql.MYSQL_TYPE_DATETIME, mysql.MYSQL_TYPE_DATETIME2:
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
		return "CHAR"
	case mysql.MYSQL_TYPE_GEOMETRY:
		return "GEOMETRY"
	default:
		return fmt.Sprintf("UNKNOWN_TYPE: %d", t)
	}
}

// unsignedMap returns a map: column index -> unsigned.
// Note that only columns with signedness information will be returned.
// nil is returned if not available or no signedness columns at all.
func (e *TableMapEvent) unsignedMap() map[int]bool {
	if len(e.SignednessBitmap) == 0 {
		return nil
	}
	ret := make(map[int]bool)
	i := 0
	for _, field := range e.SignednessBitmap {
		for c := 0x80; c != 0; {
			if e.isNumericColumn(i) {
				ret[i] = field&byte(c) != 0
				c >>= 1
			}
			i++
			if i >= len(e.ColumnType) {
				return ret
			}
		}
	}
	return ret
}

func (e *TableMapEvent) isNumericColumn(i int) bool {
	switch e.ColumnType[i] {
	case mysql.MYSQL_TYPE_TINY,
		mysql.MYSQL_TYPE_SHORT,
		mysql.MYSQL_TYPE_INT24,
		mysql.MYSQL_TYPE_LONG,
		mysql.MYSQL_TYPE_LONGLONG,
		mysql.MYSQL_TYPE_YEAR,
		mysql.MYSQL_TYPE_FLOAT,
		mysql.MYSQL_TYPE_DOUBLE,
		mysql.MYSQL_TYPE_DECIMAL,
		mysql.MYSQL_TYPE_NEWDECIMAL:
		return true
	default:
		return false
	}
}

// mysqlStringDecoders maps MySQL charset names to their byte-to-UTF-8 decoder functions.
// Charsets not listed here fall back to a raw string cast (passthrough), which is correct
// for utf8/utf8mb4/ascii since their bytes are already valid UTF-8.
var mysqlStringDecoders = map[string]func([]byte) (string, error){
	"utf8":    decodeRawString,
	"utf8mb3": decodeRawString,
	"utf8mb4": decodeRawString,
	"ascii":   decodeRawString,
	"latin1":  decodeLatin1,
	"ucs2":    decodeUTF16BE, // UCS-2 is Big Endian, BMP-only subset of UTF-16
	"utf16":   decodeUTF16BE,
	"utf16le": decodeUTF16LE,
}

func decodeRawString(b []byte) (string, error) {
	return string(b), nil
}

func decodeLatin1(b []byte) (string, error) {
	out, err := charmap.ISO8859_1.NewDecoder().Bytes(b)
	return string(out), err
}

func decodeUTF16BE(b []byte) (string, error) {
	out, err := unicode.UTF16(unicode.BigEndian, unicode.IgnoreBOM).NewDecoder().Bytes(b)
	return string(out), err
}

func decodeUTF16LE(b []byte) (string, error) {
	out, err := unicode.UTF16(unicode.LittleEndian, unicode.IgnoreBOM).NewDecoder().Bytes(b)
	return string(out), err
}

// decodeBytesToString converts raw binlog bytes to a UTF-8 string using the MySQL collation ID.
// Falls back to a raw string cast for unknown collations or charsets.
func decodeBytesToString(b []byte, collationID uint64) (string, error) {
	if len(b) == 0 {
		return "", nil
	}
	// MySQL collation IDs are small integers; guard against overflow before casting.
	if collationID > math.MaxInt32 {
		return string(b), nil
	}
	coll, _ := charset.GetCollationByID(int(collationID)) //nolint:gosec // bounds checked above
	if coll == nil {
		return string(b), nil
	}
	decoder, ok := mysqlStringDecoders[coll.CharsetName]
	if !ok {
		return string(b), nil
	}
	return decoder(b)
}
