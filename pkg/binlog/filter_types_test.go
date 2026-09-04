package binlog

import (
	"context"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file pins convertRowToMap against every MySQL column type the binlog can carry,
// decoding the same row three ways:
//
//	1. reading binlog metadata (binlog_row_metadata=FULL)
//	2. reading information_schema (MySQL 5.7, MariaDB, or the MINIMAL default)
//	3. reading information_schema on a server that reports NULL rather than 'binary'
//	   as the collation of BINARY/VARBINARY/BLOB columns
//
// All three must agree: (1) is the reference, and any divergence in (2) or (3) is a
// record whose values change depending on how the server happens to be configured.

// fixtureColumn declares one column in both dialects: the fields a TableMapEvent carries
// and the row information_schema returns for it, plus a value shaped the way go-mysql's
// decodeValue produces it for that type.
type fixtureColumn struct {
	name        string
	columnType  byte   // TableMapEvent.ColumnType
	columnMeta  uint16 // TableMapEvent.ColumnMeta
	sqlType     string // information_schema COLUMN_TYPE
	collation   string // information_schema COLLATION_NAME, "" when NULL
	enumMembers [][]byte
	setMembers  [][]byte
	value       interface{}
	want        interface{} // expected decoded value
}

// stringColumnMeta packs a CHAR/BINARY column's meta so realType() reports STRING rather
// than ENUM or SET.
func stringColumnMeta(length uint16) uint16 {
	return uint16(mysql.MYSQL_TYPE_STRING)<<8 | length
}

// allTypeColumns covers every type go-mysql's decodeValue can hand us, with the Go value
// it produces: signed and unsigned integers of each width, both decimal families, BIT,
// the temporal types, character types under three collations, binary types, JSON,
// GEOMETRY, ENUM, SET, and a NULL of each kind that takes a special branch.
func allTypeColumns() []fixtureColumn {
	return []fixtureColumn{
		{name: "c_tinyint", columnType: mysql.MYSQL_TYPE_TINY, sqlType: "tinyint(4)",
			value: int64(-5), want: int64(-5)},
		{name: "c_tinyint_u", columnType: mysql.MYSQL_TYPE_TINY, sqlType: "tinyint(3) unsigned",
			value: int64(200), want: int64(200)},
		{name: "c_smallint", columnType: mysql.MYSQL_TYPE_SHORT, sqlType: "smallint(6)",
			value: int64(-300), want: int64(-300)},
		{name: "c_smallint_u", columnType: mysql.MYSQL_TYPE_SHORT, sqlType: "smallint(5) unsigned",
			value: int64(60000), want: int64(60000)},
		{name: "c_mediumint", columnType: mysql.MYSQL_TYPE_INT24, sqlType: "mediumint(9)",
			value: int64(-8000), want: int64(-8000)},
		{name: "c_mediumint_u", columnType: mysql.MYSQL_TYPE_INT24, sqlType: "mediumint(8) unsigned",
			value: int64(16000000), want: int64(16000000)},
		{name: "c_int", columnType: mysql.MYSQL_TYPE_LONG, sqlType: "int(11)",
			value: int64(-70000), want: int64(-70000)},
		{name: "c_int_u", columnType: mysql.MYSQL_TYPE_LONG, sqlType: "int(10) unsigned",
			value: int64(4000000000), want: int64(4000000000)},
		{name: "c_bigint", columnType: mysql.MYSQL_TYPE_LONGLONG, sqlType: "bigint(20)",
			value: int64(-9007199254740993), want: int64(-9007199254740993)},
		// go-mysql sign-extends, so the maximum unsigned BIGINT arrives as -1; the driver's
		// stripSignExtension undoes that using the UNSIGNED prefix in the resolved type.
		{name: "c_bigint_u", columnType: mysql.MYSQL_TYPE_LONGLONG, sqlType: "bigint(20) unsigned",
			value: int64(-1), want: int64(-1)},
		{name: "c_decimal", columnType: mysql.MYSQL_TYPE_NEWDECIMAL, columnMeta: 10<<8 | 2,
			sqlType: "decimal(10,2)", value: float64(1234.56), want: float64(1234.56)},
		{name: "c_float", columnType: mysql.MYSQL_TYPE_FLOAT, columnMeta: 4, sqlType: "float",
			value: float32(3.5), want: float32(3.5)},
		{name: "c_double", columnType: mysql.MYSQL_TYPE_DOUBLE, columnMeta: 8, sqlType: "double",
			value: float64(2.718281828), want: float64(2.718281828)},
		{name: "c_bit", columnType: mysql.MYSQL_TYPE_BIT, columnMeta: 0<<8 | 8, sqlType: "bit(8)",
			value: int64(5), want: int64(5)},
		// go-mysql returns YEAR as int, not int64.
		{name: "c_year", columnType: mysql.MYSQL_TYPE_YEAR, sqlType: "year(4)",
			value: 2024, want: 2024},
		{name: "c_date", columnType: mysql.MYSQL_TYPE_DATE, sqlType: "date",
			value: "2024-01-15", want: "2024-01-15"},
		{name: "c_datetime", columnType: mysql.MYSQL_TYPE_DATETIME2, columnMeta: 0, sqlType: "datetime",
			value: "2024-01-15 10:30:00", want: "2024-01-15 10:30:00"},
		{name: "c_timestamp", columnType: mysql.MYSQL_TYPE_TIMESTAMP2, columnMeta: 0, sqlType: "timestamp",
			value: "2024-01-15 10:30:00", want: "2024-01-15 10:30:00"},
		{name: "c_time", columnType: mysql.MYSQL_TYPE_TIME2, columnMeta: 0, sqlType: "time",
			value: "10:30:00", want: "10:30:00"},
		{name: "c_char", columnType: mysql.MYSQL_TYPE_STRING, columnMeta: stringColumnMeta(32),
			sqlType: "char(32)", collation: "utf8mb4_general_ci", value: "abc", want: "abc"},
		{name: "c_varchar", columnType: mysql.MYSQL_TYPE_VARCHAR, columnMeta: 128,
			sqlType: "varchar(32)", collation: "utf8mb4_general_ci", value: "héllo", want: "héllo"},
		// latin1 bytes must be transcoded to UTF-8, not cast.
		{name: "c_varchar_latin1", columnType: mysql.MYSQL_TYPE_VARCHAR, columnMeta: 128,
			sqlType: "varchar(32)", collation: "latin1_swedish_ci",
			value: string([]byte{0x63, 0x61, 0x66, 0xE9}), want: "café"},
		{name: "c_text", columnType: mysql.MYSQL_TYPE_BLOB, columnMeta: 2, sqlType: "text",
			collation: "utf8mb4_general_ci", value: []byte("text data"), want: "text data"},
		// A binary column carries collation 63; decodeBytesToString passes its bytes through.
		{name: "c_blob", columnType: mysql.MYSQL_TYPE_BLOB, columnMeta: 2, sqlType: "blob",
			collation: "binary", value: []byte{0x00, 0x01, 0xFF},
			want: string([]byte{0x00, 0x01, 0xFF})},
		{name: "c_varbinary", columnType: mysql.MYSQL_TYPE_VARCHAR, columnMeta: 64,
			sqlType: "varbinary(64)", collation: "binary",
			value: string([]byte{0x10, 0x20}), want: string([]byte{0x10, 0x20})},
		{name: "c_json", columnType: mysql.MYSQL_TYPE_JSON, columnMeta: 4, sqlType: "json",
			value: `{"a":1}`, want: `{"a":1}`},
		{name: "c_geometry", columnType: mysql.MYSQL_TYPE_GEOMETRY, columnMeta: 4, sqlType: "geometry",
			value: []byte{0x00, 0x00, 0x00, 0x00, 0x01}, want: []byte{0x00, 0x00, 0x00, 0x00, 0x01}},
		{name: "c_enum", columnType: mysql.MYSQL_TYPE_STRING,
			columnMeta:  uint16(mysql.MYSQL_TYPE_ENUM)<<8 | 1,
			sqlType:     "enum('active','inactive','banned')",
			collation:   "utf8mb4_general_ci",
			enumMembers: [][]byte{[]byte("active"), []byte("inactive"), []byte("banned")},
			value:       int64(2), want: "inactive"},
		{name: "c_set", columnType: mysql.MYSQL_TYPE_STRING,
			columnMeta: uint16(mysql.MYSQL_TYPE_SET)<<8 | 1,
			sqlType:    "set('a','b','c')",
			collation:  "utf8mb4_general_ci",
			setMembers: [][]byte{[]byte("a"), []byte("b"), []byte("c")},
			value:      int64(5), want: "a,c"},
		// NULLs must survive each branch that would otherwise rewrite the value.
		{name: "c_null_int", columnType: mysql.MYSQL_TYPE_LONG, sqlType: "int(11)",
			value: nil, want: nil},
		{name: "c_null_varchar", columnType: mysql.MYSQL_TYPE_VARCHAR, columnMeta: 128,
			sqlType: "varchar(32)", collation: "utf8mb4_general_ci", value: nil, want: nil},
		{name: "c_null_enum", columnType: mysql.MYSQL_TYPE_STRING,
			columnMeta:  uint16(mysql.MYSQL_TYPE_ENUM)<<8 | 1,
			sqlType:     "enum('active','inactive','banned')",
			collation:   "utf8mb4_general_ci",
			enumMembers: [][]byte{[]byte("active"), []byte("inactive"), []byte("banned")},
			value:       nil, want: nil},
		{name: "c_null_set", columnType: mysql.MYSQL_TYPE_STRING,
			columnMeta: uint16(mysql.MYSQL_TYPE_SET)<<8 | 1,
			sqlType:    "set('a','b','c')",
			collation:  "utf8mb4_general_ci",
			setMembers: [][]byte{[]byte("a"), []byte("b"), []byte("c")},
			value:      nil, want: nil},
	}
}

// bareTableMapFrom builds the half of TableMapEvent every server writes, whatever
// binlog_row_metadata says.
func bareTableMapFrom(cols []fixtureColumn) *replication.TableMapEvent {
	e := &replication.TableMapEvent{
		Schema:      []byte("shop"),
		Table:       []byte("wide"),
		ColumnCount: uint64(len(cols)),
		ColumnType:  make([]byte, len(cols)),
		ColumnMeta:  make([]uint16, len(cols)),
	}
	for i, c := range cols {
		e.ColumnType[i] = c.columnType
		e.ColumnMeta[i] = c.columnMeta
	}
	return e
}

// fullTableMapFrom adds the optional metadata a server emits with
// binlog_row_metadata=FULL, laid out the way the binlog actually packs it: signedness as
// one bit per numeric column, and charsets as one entry per character (or enum/set)
// column rather than per column.
func fullTableMapFrom(t *testing.T, cols []fixtureColumn) *replication.TableMapEvent {
	t.Helper()
	e := bareTableMapFrom(cols)
	wrapped := &TableMapEvent{e}

	var signedness []byte
	bit := 0
	for i, c := range cols {
		if !wrapped.isNumericColumn(i) {
			continue
		}
		if bit%8 == 0 {
			signedness = append(signedness, 0)
		}
		if strings.Contains(c.sqlType, "unsigned") {
			signedness[bit/8] |= byte(0x80 >> (bit % 8))
		}
		bit++
	}
	e.SignednessBitmap = signedness

	for i, c := range cols {
		switch {
		case e.IsEnumColumn(i):
			e.EnumStrValue = append(e.EnumStrValue, c.enumMembers)
			e.EnumSetColumnCharset = append(e.EnumSetColumnCharset, mustCollationID(t, c.collation))
		case e.IsSetColumn(i):
			e.SetStrValue = append(e.SetStrValue, c.setMembers)
			e.EnumSetColumnCharset = append(e.EnumSetColumnCharset, mustCollationID(t, c.collation))
		case e.IsCharacterColumn(i):
			e.ColumnCharset = append(e.ColumnCharset, mustCollationID(t, c.collation))
		}
		e.ColumnName = append(e.ColumnName, []byte(c.name))
	}
	return e
}

func mustCollationID(t *testing.T, name string) uint64 {
	t.Helper()
	require.NotEmpty(t, name, "character column fixture is missing a collation")
	id := collationIDByName(name)
	require.NotZero(t, id, "unknown collation %q", name)
	return id
}

// metaFrom builds the cache entry that loading this table from information_schema
// produces, going through the same columnMetaFrom the loader uses.
func metaFrom(cols []fixtureColumn) *tableMeta {
	meta := &tableMeta{}
	for _, c := range cols {
		meta.Columns = append(meta.Columns, columnMetaFrom(c.name, c.sqlType, c.collation))
	}
	return meta
}

// metaFromNullBinaryCollation is metaFrom for servers that report COLLATION_NAME as NULL
// on BINARY/VARBINARY/BLOB columns instead of 'binary'. Those columns must still resolve
// to the binary collation, or their bytes would decode differently from the binlog path.
func metaFromNullBinaryCollation(cols []fixtureColumn) *tableMeta {
	meta := &tableMeta{}
	for _, c := range cols {
		collation := c.collation
		if collation == "binary" {
			collation = ""
		}
		meta.Columns = append(meta.Columns, columnMetaFrom(c.name, c.sqlType, collation))
	}
	return meta
}

func rowFrom(cols []fixtureColumn) []interface{} {
	row := make([]interface{}, len(cols))
	for i, c := range cols {
		row[i] = c.value
	}
	return row
}

func TestConvertRowToMapAllTypes(t *testing.T) {
	ctx := context.Background()
	cols := allTypeColumns()
	row := rowFrom(cols)

	full := fullTableMapFrom(t, cols)
	bare := bareTableMapFrom(cols)

	fullView, err := filterWithCache(nil).resolveColumns(ctx, &replication.RowsEvent{Table: full})
	require.NoError(t, err)
	fallbackView, err := filterWithCacheFor("shop.wide", metaFrom(cols)).resolveColumns(ctx, &replication.RowsEvent{Table: bare})
	require.NoError(t, err)

	fromFull, err := convertRowToMap(row, fullView, identityConverter)
	require.NoError(t, err)
	fromFallback, err := convertRowToMap(row, fallbackView, identityConverter)
	require.NoError(t, err)
	nullCollationView, err := filterWithCacheFor("shop.wide", metaFromNullBinaryCollation(cols)).
		resolveColumns(ctx, &replication.RowsEvent{Table: bare})
	require.NoError(t, err)
	fromNullCollation, err := convertRowToMap(row, nullCollationView, identityConverter)
	require.NoError(t, err)

	for i, c := range cols {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, fromFull[c.name], "binlog metadata path")
			assert.Equal(t, c.want, fromFallback[c.name], "information_schema fallback path")
			assert.Equal(t, c.want, fromNullCollation[c.name], "fallback with NULL binary collation")
			assert.Equal(t, fullView.types[i], fallbackView.types[i], "resolved SQL type must match")
		})
	}

	// Compared whole so an extra or missing key fails too, not just the values.
	assert.Equal(t, fromFull, fromFallback, "fallback must decode identically to binlog metadata")
	assert.Equal(t, fromFull, fromNullCollation, "binary columns must not depend on COLLATION_NAME being reported")
}

// TestResolvedTypesAllTypes pins the SQL type strings handed to the driver's converter,
// since those select the destination type and the unsigned handling downstream.
func TestResolvedTypesAllTypes(t *testing.T) {
	cols := allTypeColumns()
	full := fullTableMapFrom(t, cols)

	view, err := filterWithCache(nil).resolveColumns(context.Background(), &replication.RowsEvent{Table: full})
	require.NoError(t, err)

	want := map[string]string{
		"c_tinyint": "TINYINT", "c_tinyint_u": "UNSIGNED TINYINT",
		"c_smallint": "SMALLINT", "c_smallint_u": "UNSIGNED SMALLINT",
		"c_mediumint": "MEDIUMINT", "c_mediumint_u": "UNSIGNED MEDIUMINT",
		"c_int": "INT", "c_int_u": "UNSIGNED INT",
		"c_bigint": "BIGINT", "c_bigint_u": "UNSIGNED BIGINT",
		"c_decimal": "DECIMAL", "c_float": "FLOAT", "c_double": "DOUBLE",
		"c_bit": "BIT", "c_year": "YEAR", "c_date": "DATE",
		"c_datetime": "DATETIME", "c_timestamp": "TIMESTAMP", "c_time": "TIME",
		"c_char": "CHAR", "c_varchar": "VARCHAR", "c_varchar_latin1": "VARCHAR",
		"c_text": "BLOB", "c_blob": "BLOB", "c_varbinary": "VARCHAR",
		"c_json": "JSON", "c_geometry": "GEOMETRY",
		"c_enum": "CHAR", "c_set": "CHAR",
		"c_null_int": "INT", "c_null_varchar": "VARCHAR",
		"c_null_enum": "CHAR", "c_null_set": "CHAR",
	}

	for i, c := range cols {
		if expected, ok := want[c.name]; ok {
			assert.Equal(t, expected, view.types[i], c.name)
		}
	}
}

// TestConvertRowToMapAllTypesUnsignedFallback checks the signedness half of the fallback
// on its own: with the binlog's SignednessBitmap absent, every UNSIGNED column must still
// resolve to an UNSIGNED type name.
func TestConvertRowToMapAllTypesUnsignedFallback(t *testing.T) {
	cols := allTypeColumns()
	bare := bareTableMapFrom(cols)

	view, err := filterWithCacheFor("shop.wide", metaFrom(cols)).resolveColumns(context.Background(),
		&replication.RowsEvent{Table: bare})
	require.NoError(t, err)

	for i, c := range cols {
		if strings.Contains(c.sqlType, "unsigned") {
			assert.True(t, strings.HasPrefix(view.types[i], "UNSIGNED "),
				"%s resolved to %q", c.name, view.types[i])
		}
	}
}

// TestColumnMetaFromBinaryCollation pins the binary-collation rule in both directions:
// servers that report 'binary' and servers that report NULL must produce the same
// collation, and non-string types must stay at 0 so their values pass through untouched.
func TestColumnMetaFromBinaryCollation(t *testing.T) {
	tests := []struct {
		sqlType   string
		collation string
		want      uint64
	}{
		{"blob", "binary", binaryCollationID},
		{"blob", "", binaryCollationID},
		{"tinyblob", "", binaryCollationID},
		{"mediumblob", "", binaryCollationID},
		{"longblob", "", binaryCollationID},
		{"binary(16)", "", binaryCollationID},
		{"varbinary(64)", "", binaryCollationID},
		{"varchar(32)", "utf8mb4_general_ci", 45},
		{"text", "utf8mb4_general_ci", 45},
		// GEOMETRY decodes as a blob but is not a character column in the binlog, so it
		// must carry no collation or the two paths would diverge.
		{"geometry", "", 0},
		{"int(11)", "", 0},
		{"datetime", "", 0},
		// A type that merely starts with a binary type's name must not match.
		{"blobbish_custom", "", 0},
	}

	for _, tt := range tests {
		t.Run(tt.sqlType+"/"+tt.collation, func(t *testing.T) {
			assert.Equal(t, tt.want, columnMetaFrom("c", tt.sqlType, tt.collation).CollationID)
		})
	}
}
