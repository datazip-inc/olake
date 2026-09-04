package binlog

import (
	"context"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const utf8mb4GeneralCI = 45

// identityConverter stands in for the driver's dataTypeConverter so the tests assert on
// the decoding this package is responsible for, not on type reformatting.
func identityConverter(value interface{}, _ string) (interface{}, error) {
	return value, nil
}

// baseTableMap builds the mandatory half of a TableMapEvent for the fixture table:
//
//	id BIGINT UNSIGNED, name VARCHAR(32), status ENUM('active','inactive'), tags SET('a','b')
//
// Every server writes this much, regardless of binlog_row_metadata.
func baseTableMap() *replication.TableMapEvent {
	return &replication.TableMapEvent{
		Schema:      []byte("shop"),
		Table:       []byte("orders"),
		ColumnCount: 4,
		ColumnType: []byte{
			mysql.MYSQL_TYPE_LONGLONG,
			mysql.MYSQL_TYPE_VARCHAR,
			mysql.MYSQL_TYPE_STRING, // ENUM is a STRING whose meta high byte says ENUM
			mysql.MYSQL_TYPE_STRING, // ditto for SET
		},
		ColumnMeta: []uint16{
			0,
			32,
			uint16(mysql.MYSQL_TYPE_ENUM) << 8,
			uint16(mysql.MYSQL_TYPE_SET) << 8,
		},
	}
}

// fullTableMap adds the optional metadata a server emits with binlog_row_metadata=FULL.
func fullTableMap() *replication.TableMapEvent {
	e := baseTableMap()
	e.ColumnName = [][]byte{[]byte("id"), []byte("name"), []byte("status"), []byte("tags")}
	e.SignednessBitmap = []byte{0x80} // first numeric column is unsigned
	e.DefaultCharset = []uint64{utf8mb4GeneralCI}
	e.EnumSetDefaultCharset = []uint64{utf8mb4GeneralCI}
	e.EnumStrValue = [][][]byte{{[]byte("active"), []byte("inactive")}}
	e.SetStrValue = [][][]byte{{[]byte("a"), []byte("b")}}
	return e
}

// fixtureMeta is what loading the same table from information_schema produces.
func fixtureMeta() *tableMeta {
	return &tableMeta{Columns: []columnMeta{
		{Name: "id", Unsigned: true},
		{Name: "name", CollationID: utf8mb4GeneralCI},
		{Name: "status", EnumValues: []string{"active", "inactive"}},
		{Name: "tags", SetMembers: []string{"a", "b"}},
	}}
}

// filterWithCacheFor returns a ChangeFilter whose cache is pre-populated for one table,
// so resolveColumns exercises the fallback without a live server.
func filterWithCacheFor(key string, meta *tableMeta) ChangeFilter {
	f := NewChangeFilter(nil, identityConverter)
	if meta != nil {
		f.schema.tables[key] = meta
	}
	return f
}

func filterWithCache(meta *tableMeta) ChangeFilter {
	return filterWithCacheFor("shop.orders", meta)
}

// TestResolveColumnsFallbackMatchesBinlogMetadata is the core guarantee: the same row
// decodes identically whether the column metadata came from the binlog or from
// information_schema.
func TestResolveColumnsFallbackMatchesBinlogMetadata(t *testing.T) {
	ctx := context.Background()
	row := []interface{}{int64(5), "héllo", int64(1), int64(3)}

	fromBinlog, err := filterWithCache(nil).resolveColumns(ctx, &replication.RowsEvent{Table: fullTableMap()})
	require.NoError(t, err)

	fromSchema, err := filterWithCache(fixtureMeta()).resolveColumns(ctx, &replication.RowsEvent{Table: baseTableMap()})
	require.NoError(t, err)

	assert.Equal(t, fromBinlog.names, fromSchema.names)
	assert.Equal(t, fromBinlog.types, fromSchema.types)
	assert.Equal(t, fromBinlog.enumValues, fromSchema.enumValues)
	assert.Equal(t, fromBinlog.setMembers, fromSchema.setMembers)
	assert.Equal(t, fromBinlog.collations, fromSchema.collations)

	binlogRecord, err := convertRowToMap(row, fromBinlog, identityConverter)
	require.NoError(t, err)
	schemaRecord, err := convertRowToMap(row, fromSchema, identityConverter)
	require.NoError(t, err)

	assert.Equal(t, map[string]interface{}{
		"id":     int64(5),
		"name":   "héllo",
		"status": "active",
		"tags":   "a,b",
	}, binlogRecord)
	assert.Equal(t, binlogRecord, schemaRecord)
}

func TestResolveColumnsUnsignedFromFallback(t *testing.T) {
	// UNSIGNED must survive the fallback: it drives stripSignExtension in the driver, and
	// getting it wrong corrupts large values silently rather than failing.
	view, err := filterWithCache(fixtureMeta()).resolveColumns(context.Background(),
		&replication.RowsEvent{Table: baseTableMap()})
	require.NoError(t, err)
	assert.Equal(t, "UNSIGNED BIGINT", view.types[0])
}

// TestResolveColumnsSignednessOnlyFallback covers a FULL server whose table has no
// numeric columns: unsignedMap is nil, so the cache is consulted, but the binlog's own
// ENUM/SET members and collations must still win.
func TestResolveColumnsSignednessOnlyFallback(t *testing.T) {
	tableMap := fullTableMap()
	tableMap.SignednessBitmap = nil

	meta := fixtureMeta()
	meta.Columns[2].EnumValues = []string{"WRONG", "ALSO WRONG"}

	view, err := filterWithCache(meta).resolveColumns(context.Background(),
		&replication.RowsEvent{Table: tableMap})
	require.NoError(t, err)

	assert.Equal(t, []string{"active", "inactive"}, view.enumValues[2], "binlog members still win")
	assert.Equal(t, "UNSIGNED BIGINT", view.types[0], "signedness comes from information_schema")
}

func TestResolveColumnsRejectsSchemaDrift(t *testing.T) {
	meta := fixtureMeta()
	meta.Columns = meta.Columns[:3] // a column was added after our information_schema read

	_, err := filterWithCache(meta).resolveColumns(context.Background(),
		&replication.RowsEvent{Table: baseTableMap()})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "schema drift")
}

func TestResolveColumnsWithoutSchemaClient(t *testing.T) {
	// A bare TableMapEvent and no client to fall back on must fail loudly rather than
	// emit records with missing columns.
	_, err := filterWithCache(nil).resolveColumns(context.Background(),
		&replication.RowsEvent{Table: baseTableMap()})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no schema client is configured")
}

func TestConvertRowToMapEnumAndSetEdgeCases(t *testing.T) {
	view, err := filterWithCache(fixtureMeta()).resolveColumns(context.Background(),
		&replication.RowsEvent{Table: baseTableMap()})
	require.NoError(t, err)

	tests := []struct {
		name       string
		row        []interface{}
		wantStatus interface{}
		wantTags   interface{}
	}{
		{"invalid enum stored as index 0", []interface{}{int64(1), "a", int64(0), int64(0)}, "", ""},
		{"out-of-range enum index", []interface{}{int64(1), "a", int64(99), int64(0)}, "", ""},
		{"last enum member", []interface{}{int64(1), "a", int64(2), int64(0)}, "inactive", ""},
		{"single set bit", []interface{}{int64(1), "a", int64(1), int64(2)}, "active", "b"},
		{"null enum and set", []interface{}{int64(1), "a", nil, nil}, nil, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record, err := convertRowToMap(tt.row, view, identityConverter)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, record["status"])
			assert.Equal(t, tt.wantTags, record["tags"])
		})
	}
}

func TestConvertRowToMapColumnCountMismatch(t *testing.T) {
	view, err := filterWithCache(fixtureMeta()).resolveColumns(context.Background(),
		&replication.RowsEvent{Table: baseTableMap()})
	require.NoError(t, err)

	_, err = convertRowToMap([]interface{}{int64(1), "a"}, view, identityConverter)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "column count mismatch")
}
