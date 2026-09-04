package binlog

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/jmoiron/sqlx"
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
	schema        *schemaCache // information_schema fallback for omitted binlog metadata
}

// NewChangeFilter creates a filter for the given streams. schemaClient backs the
// information_schema fallback used when the server does not emit full binlog row
// metadata (MySQL < 8.0.1, binlog_row_metadata != FULL, or any MariaDB version).
func NewChangeFilter(schemaClient *sqlx.DB, typeConverter func(value interface{}, columnType string) (interface{}, error), streams ...types.StreamInterface) ChangeFilter {
	filter := ChangeFilter{
		streams:   make(map[string]types.StreamInterface),
		converter: typeConverter,
		schema:    newSchemaCache(schemaClient),
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

	view, err := f.resolveColumns(ctx, e)
	if err != nil {
		return err
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
		record, err := convertRowToMap(row, view, f.converter)
		if err != nil {
			return err
		}
		if record == nil {
			continue
		}

		// Use microsecond-precision timestamp from GTID event (MySQL 8.0.1+) if available,
		// otherwise fall back to second-precision header timestamp
		timestamp := utils.Ternary(!f.lastGTIDEvent.IsZero(), f.lastGTIDEvent, time.Unix(int64(ev.Header.Timestamp), 0)).(time.Time)

		// Bytes: InnoDB on-disk byte sum for this row, carried on the change and added per record by the writer.
		change := abstract.NewCDCChange(stream, timestamp, operationType, record,
			map[string]any{
				CDCBinlogFileName: pos.Name,
				CDCBinlogFilePos:  pos.Pos, // Use the event position
			},
			mysqlCDCRowBytes(row, view.types))
		if err := callback(ctx, change); err != nil {
			return err
		}
	}
	return nil
}

// columnView is everything needed to decode one RowsEvent, resolved once per event
// from either the binlog's optional metadata or the information_schema cache.
// All slices are indexed by ordinal column position.
type columnView struct {
	names      []string
	types      []string   // mysqlTypeName output, e.g. "UNSIGNED BIGINT"
	enumValues [][]string // nil for non-ENUM columns
	setMembers [][]string // nil for non-SET columns
	collations []uint64   // 0 = no charset decoding
}

// resolveColumns prefers the binlog's own metadata (binlog_row_metadata=FULL) and falls
// back to information_schema when it is absent. Type bytes and ENUM/SET detection always
// come from the TableMapEvent — those are mandatory fields present on every server.
func (f ChangeFilter) resolveColumns(ctx context.Context, e *replication.RowsEvent) (*columnView, error) {
	tableMap := e.Table
	n := len(tableMap.ColumnType)

	view := &columnView{
		names:      tableMap.ColumnNameString(),
		types:      make([]string, n),
		enumValues: make([][]string, n),
		setMembers: make([][]string, n),
		collations: make([]uint64, n),
	}

	unsignedMap := (&TableMapEvent{tableMap}).unsignedMap()
	fallback := len(view.names) != n

	// unsignedMap is nil both when SignednessBitmap is absent and when the table simply
	// has no numeric columns. Treating that as "needs fallback" costs one extra query per
	// such table and is strictly safer than guessing signed.
	var meta *tableMeta
	if fallback || unsignedMap == nil {
		var err error
		meta, err = f.schema.get(ctx, string(tableMap.Schema), string(tableMap.Table))
		if err != nil {
			return nil, err
		}
		if len(meta.Columns) != n {
			// The table was altered between this event and our information_schema read.
			// Refuse rather than write rows against a mismatched schema.
			return nil, fmt.Errorf("schema drift for %s.%s: binlog has %d columns, information_schema has %d",
				tableMap.Schema, tableMap.Table, n, len(meta.Columns))
		}
	}

	if fallback {
		view.names = make([]string, n)
		for i, col := range meta.Columns {
			view.names[i] = col.Name
		}
	}

	for i := 0; i < n; i++ {
		isUnsigned := unsignedMap != nil && unsignedMap[i]
		if unsignedMap == nil {
			isUnsigned = meta.Columns[i].Unsigned
		}
		view.types[i] = mysqlTypeName(tableMap.ColumnType[i], isUnsigned)
	}

	// meta is also loaded when only signedness was missing; in that case the binlog still
	// carries its own ENUM/SET members and collations, so keep using them.
	var fallbackMeta *tableMeta
	if fallback {
		fallbackMeta = meta
	}
	fillEnumSetAndCollations(view, tableMap, fallbackMeta)
	return view, nil
}

// fillEnumSetAndCollations populates the ENUM/SET members and collation IDs. meta is nil
// when the binlog carried its own metadata, in which case members arrive as raw bytes in
// the column's charset and are decoded here; when meta is set, the members came back from
// information_schema already decoded to UTF-8 through the connection charset and must not
// be decoded a second time.
func fillEnumSetAndCollations(view *columnView, tableMap *replication.TableMapEvent, meta *tableMeta) {
	if meta == nil {
		enumSetCollations := tableMap.EnumSetCollationMap()
		collations := tableMap.CollationMap()
		enumP, setP := 0, 0
		for i := range view.types {
			switch {
			case tableMap.IsEnumColumn(i):
				if enumP < len(tableMap.EnumStrValue) {
					view.enumValues[i] = decodeMembers(tableMap.EnumStrValue[enumP], enumSetCollations[i])
				}
				enumP++ // always advance, even for empty metadata, to stay in step
			case tableMap.IsSetColumn(i):
				if setP < len(tableMap.SetStrValue) {
					view.setMembers[i] = decodeMembers(tableMap.SetStrValue[setP], enumSetCollations[i])
				}
				setP++
			default:
				view.collations[i] = collations[i]
			}
		}
		return
	}

	for i := range view.types {
		switch {
		case tableMap.IsEnumColumn(i):
			view.enumValues[i] = meta.Columns[i].EnumValues
		case tableMap.IsSetColumn(i):
			view.setMembers[i] = meta.Columns[i].SetMembers
		default:
			view.collations[i] = meta.Columns[i].CollationID
		}
	}
}

func decodeMembers(raw [][]byte, collationID uint64) []string {
	out := make([]string, len(raw))
	for i, b := range raw {
		if s, err := decodeBytesToString(b, collationID); err == nil {
			out[i] = s
		} else {
			out[i] = string(b) // fallback
		}
	}
	return out
}

// convertRowToMap converts a binlog row to a map.
func convertRowToMap(row []interface{}, view *columnView, converter func(value interface{}, columnType string) (interface{}, error)) (map[string]interface{}, error) {
	if len(view.names) != len(row) {
		return nil, fmt.Errorf("column count mismatch: expected %d, got %d", len(view.names), len(row))
	}

	// NOTE: For MySQL CDC (binlog-based), FLOAT values are read directly from the binlog and may
	// differ from SELECT output due to SQL-layer formatting/rounding.
	record := make(map[string]interface{}, len(row))
	for i, val := range row {
		switch {
		case view.enumValues[i] != nil:
			// For an update CDC event, the key of the enum value is passed in binlog events
			// as the 1-based member index, always int64. MySQL stores an invalid ENUM insert
			// as index 0 (special error value), which maps to the empty string.
			if idx, isInt64 := val.(int64); isInt64 {
				val = ""
				if idx > 0 && int(idx) <= len(view.enumValues[i]) {
					val = view.enumValues[i][idx-1]
				}
			}

		case view.setMembers[i] != nil:
			// MySQL SET columns are stored in the binlog as an int64 bitmask:
			// bit 0 = first member, bit 1 = second member, etc.
			// e.g. SET('sports','music','gaming','reading') with value 'sports,reading' -> bitmask = 0b1001 = 9
			if bitmask, isInt64 := val.(int64); isInt64 {
				members := view.setMembers[i]
				selected := make([]string, 0, len(members))
				for bit := 0; bit < len(members); bit++ {
					if bitmask&(1<<bit) != 0 {
						selected = append(selected, members[bit])
					}
				}
				val = strings.Join(selected, ",")
			}

		case view.collations[i] != 0:
			// go-mysql blindly casts VARCHAR/CHAR bytes to string via ByteSliceToString;
			// BLOBs arrive as []byte. In both cases, cast back to bytes to recover the
			// original charset bytes, then decode properly.
			var raw []byte
			switch v := val.(type) {
			case string:
				raw = []byte(v)
			case []byte:
				raw = v
			}
			if raw != nil {
				if decoded, decErr := decodeBytesToString(raw, view.collations[i]); decErr == nil {
					val = decoded
				}
			}
		}

		convertedVal, err := converter(val, view.types[i])
		if err != nil && err != typeutils.ErrNullValue {
			return nil, err
		}
		record[view.names[i]] = convertedVal
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
		return "CHAR" // for mysql, string type is char type
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
