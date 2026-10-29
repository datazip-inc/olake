package binlog

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	utf8mb4GeneralCI = 45
	latin1SwedishCI  = 8
)

// identityConverter stands in for the driver's dataTypeConverter, so tests assert on this
// package's decoding rather than on type reformatting.
func identityConverter(value interface{}, _ string) (interface{}, error) {
	return value, nil
}

// baseTableMap is the mandatory half of a TableMapEvent, which every server writes:
//
//	id BIGINT UNSIGNED, name VARCHAR(32), status ENUM('active','inactive'), tags SET('a','b')
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

// minimalTableMap adds what binlog_row_metadata=MINIMAL emits: signedness and charsets,
// but no column names and no ENUM/SET member lists.
func minimalTableMap() *replication.TableMapEvent {
	e := baseTableMap()
	e.SignednessBitmap = []byte{0x80}
	e.DefaultCharset = []uint64{utf8mb4GeneralCI}
	return e
}

// stringOnlyTableMap is a FULL-metadata table with no numeric column, the shape for which
// MySQL writes no SignednessBitmap.
func stringOnlyTableMap() *replication.TableMapEvent {
	return &replication.TableMapEvent{
		Schema:         []byte("shop"),
		Table:          []byte("notes"),
		ColumnCount:    2,
		ColumnType:     []byte{mysql.MYSQL_TYPE_VARCHAR, mysql.MYSQL_TYPE_DATETIME2},
		ColumnMeta:     []uint16{32, 0},
		ColumnName:     [][]byte{[]byte("body"), []byte("created_at")},
		DefaultCharset: []uint64{utf8mb4GeneralCI},
	}
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

// filterWithCacheFor pre-populates the cache for one table, so resolveColumns exercises
// the fallback without a live server.
func filterWithCacheFor(key string, meta *tableMeta) ChangeFilter {
	f := NewChangeFilter(nil, Binlog{}, false, identityConverter)
	if meta != nil {
		f.schema.tables[key] = meta
	}
	return f
}

func filterWithCache(meta *tableMeta) ChangeFilter {
	return filterWithCacheFor("shop.orders", meta)
}

func TestChangeFilter_FilterRowsEvent(t *testing.T) {
	defer func(version int) { constants.LoadedStateVersion = version }(constants.LoadedStateVersion)
	constants.LoadedStateVersion = 8
	row := func(id int64, name any) []interface{} { return []interface{}{id, name, int64(1), int64(3)} }
	newFilter := func(keys ...string) (ChangeFilter, *types.ConfiguredStream) {
		stream := types.NewStream("orders", "shop", nil).WithPrimaryKey(keys...).Wrap(0)
		filter := NewChangeFilter(nil, Binlog{}, false, identityConverter, stream)
		filter.schema.tables["shop.orders"] = fixtureMeta()
		return filter, stream
	}
	position := mysql.Position{Name: "mysql-bin.000001", Pos: 120}
	event := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.UPDATE_ROWS_EVENTv2, Timestamp: 2}}
	before, after := row(1, "before"), row(2, "after")
	binaryTable := fullTableMap()
	binaryTable.ColumnType[1] = mysql.MYSQL_TYPE_STRING
	binaryTable.ColumnMeta[1] = uint16(mysql.MYSQL_TYPE_STRING)<<8 | 4
	binaryTable.DefaultCharset = []uint64{63}
	// Sizes include eight ID bytes, raw name bytes, and one byte per ENUM/SET index.
	type expectedChange struct {
		kind  string
		id    int64
		name  any
		bytes int64
	}
	tests := []struct {
		name  string
		keys  []string
		table *replication.TableMapEvent
		rows  [][]interface{}
		want  []expectedChange
	}{
		{
			name: "single key with FULL metadata", keys: []string{"id"}, table: fullTableMap(), rows: [][]interface{}{before, after},
			want: []expectedChange{{"delete", 1, "before", 16}, {"update", 2, "after", 15}},
		},
		{
			name: "single key with MINIMAL metadata", keys: []string{"id"}, table: minimalTableMap(), rows: [][]interface{}{before, after},
			want: []expectedChange{{"delete", 1, "before", 16}, {"update", 2, "after", 15}},
		},
		{
			name: "composite key changes second component", keys: []string{"id", "name"}, table: fullTableMap(), rows: [][]interface{}{before, row(1, "after")},
			want: []expectedChange{{"delete", 1, "before", 16}, {"update", 1, "after", 15}},
		},
		{
			name: "binary key outside first column", keys: []string{"name"}, table: binaryTable, rows: [][]interface{}{row(1, "\x01"), row(1, "\x02")},
			want: []expectedChange{{"delete", 1, []byte{1, 0, 0, 0}, 11}, {"update", 1, []byte{2, 0, 0, 0}, 11}},
		},
		{
			name: "binary key unchanged by value", keys: []string{"name"}, table: binaryTable, rows: [][]interface{}{row(1, "\x01"), row(2, "\x01")},
			want: []expectedChange{{"update", 2, []byte{1, 0, 0, 0}, 11}},
		},
		{
			name: "ordinary update", keys: []string{"id"}, table: fullTableMap(), rows: [][]interface{}{before, row(1, "after")},
			want: []expectedChange{{"update", 1, "after", 15}},
		},
		{
			name: "no primary key", table: fullTableMap(), rows: [][]interface{}{before, after},
			want: []expectedChange{{"update", 2, "after", 15}},
		},
		{
			name: "multiple update pairs preserve order", keys: []string{"id"}, table: fullTableMap(), rows: [][]interface{}{before, after, row(3, "before"), row(3, "after")},
			want: []expectedChange{{"delete", 1, "before", 16}, {"update", 2, "after", 15}, {"update", 3, "after", 15}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, stream := newFilter(tt.keys...)
			filter.lastGTIDEvent = time.Unix(1, 123000).UTC()
			rows := &replication.RowsEvent{Table: tt.table, ColumnCount: 4, ColumnBitmap1: []byte{15}, ColumnBitmap2: []byte{15}, Rows: tt.rows}
			var changes []abstract.CDCChange
			err := filter.FilterRowsEvent(context.Background(), rows, event, position, func(_ context.Context, change abstract.CDCChange) error {
				changes = append(changes, change)
				return nil
			})
			require.NoError(t, err)
			require.Len(t, changes, len(tt.want))
			for i, want := range tt.want {
				assert.Equal(t, want.kind, changes[i].Kind)
				assert.Equal(t, map[string]any{"id": want.id, "name": want.name, "status": "active", "tags": "a,b"}, changes[i].Data)
				assert.Same(t, stream, changes[i].Stream)
				assert.Equal(t, filter.lastGTIDEvent, changes[i].Timestamp)
				assert.Equal(t, map[string]any{CDCBinlogFileName: position.Name, CDCBinlogFilePos: position.Pos}, changes[i].ExtraColumns)
				assert.Equal(t, want.bytes, changes[i].Bytes)
			}
		})
	}

	for _, tt := range []struct {
		kind      string
		eventType replication.EventType
	}{{"insert", replication.WRITE_ROWS_EVENTv2}, {"delete", replication.DELETE_ROWS_EVENTv2}} {
		t.Run(tt.kind+" preserves rows", func(t *testing.T) {
			filter, _ := newFilter("id")
			rows := &replication.RowsEvent{Table: fullTableMap(), ColumnCount: 4, ColumnBitmap1: []byte{15}, Rows: [][]interface{}{before, after}}
			event := &replication.BinlogEvent{Header: &replication.EventHeader{EventType: tt.eventType, Timestamp: 2}}
			var changes []abstract.CDCChange
			err := filter.FilterRowsEvent(context.Background(), rows, event, position, func(_ context.Context, change abstract.CDCChange) error {
				changes = append(changes, change)
				return nil
			})
			require.NoError(t, err)
			require.Len(t, changes, 2)
			for i, change := range changes {
				assert.Equal(t, tt.kind, change.Kind)
				assert.Equal(t, int64(i+1), change.Data["id"])
				assert.Equal(t, time.Unix(2, 0), change.Timestamp)
			}
		})
	}

	for _, failKind := range []string{"delete", "update"} {
		t.Run(failKind+" callback failure", func(t *testing.T) {
			filter, _ := newFilter("id")
			rows := &replication.RowsEvent{Table: fullTableMap(), ColumnCount: 4, ColumnBitmap1: []byte{15}, ColumnBitmap2: []byte{15}, Rows: [][]interface{}{before, after, row(3, "before"), row(3, "after")}}
			failure := errors.New("destination write failed")
			var kinds []string
			err := filter.FilterRowsEvent(context.Background(), rows, event, position, func(_ context.Context, change abstract.CDCChange) error {
				kinds = append(kinds, change.Kind)
				if change.Kind == failKind {
					return failure
				}
				return nil
			})
			require.ErrorIs(t, err, failure)
			want := []string{"delete"}
			if failKind == "update" {
				want = append(want, "update")
			}
			assert.Equal(t, want, kinds)
		})
	}

	for _, failValue := range []string{"before", "after"} {
		t.Run(failValue+" image conversion failure", func(t *testing.T) {
			filter, _ := newFilter("id")
			failure := errors.New("invalid column value")
			filter.converter = func(value interface{}, _ string) (interface{}, error) {
				if value == failValue {
					return nil, failure
				}
				return value, nil
			}
			rows := &replication.RowsEvent{Table: fullTableMap(), ColumnCount: 4, ColumnBitmap1: []byte{15}, ColumnBitmap2: []byte{15}, Rows: [][]interface{}{before, after}}
			err := filter.FilterRowsEvent(context.Background(), rows, event, position, func(context.Context, abstract.CDCChange) error {
				t.Error("conversion failure must precede delivery")
				return nil
			})
			require.ErrorIs(t, err, failure)
		})
	}
}

// TestResolveColumnsFallbackMatchesBinlogMetadata is the core guarantee: a row decodes
// identically whether its metadata came from the binlog or from information_schema.
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
	// UNSIGNED drives stripSignExtension in the driver; losing it corrupts large values
	// silently rather than failing.
	view, err := filterWithCache(fixtureMeta()).resolveColumns(context.Background(),
		&replication.RowsEvent{Table: baseTableMap()})
	require.NoError(t, err)
	assert.Equal(t, "UNSIGNED BIGINT", view.types[0])
}

// TestResolveColumnsSignednessOnlyFallback covers names logged but SignednessBitmap absent
// on a table that has a numeric column: the cache supplies signedness alone, and the
// binlog's own ENUM/SET members and collations must still win.
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

// TestResolveColumnsMinimalPrefersBinlogCollations covers MINIMAL, where each field must
// come from whichever source carries it. The binlog's collations describe the table as of
// the event, information_schema only as it is now.
func TestResolveColumnsMinimalPrefersBinlogCollations(t *testing.T) {
	meta := fixtureMeta()
	meta.Columns[1].CollationID = latin1SwedishCI // stale; the binlog says utf8mb4

	view, err := filterWithCache(meta).resolveColumns(context.Background(),
		&replication.RowsEvent{Table: minimalTableMap()})
	require.NoError(t, err)

	assert.Equal(t, uint64(utf8mb4GeneralCI), view.collations[1], "binlog collation wins over information_schema")
	assert.Equal(t, "UNSIGNED BIGINT", view.types[0], "signedness comes from the binlog")
	assert.Equal(t, []string{"id", "name", "status", "tags"}, view.names, "names come from information_schema")
	assert.Equal(t, []string{"active", "inactive"}, view.enumValues[2], "ENUM members come from information_schema")
	assert.Equal(t, []string{"a", "b"}, view.setMembers[3], "SET members come from information_schema")
}

// TestResolveColumnsNoNumericColumnsSkipsSchemaLookup pins the other half: with no numeric
// column there is no signedness to be missing, so a FULL server must not touch
// information_schema. The filter has no client, so returning a view at all is the assertion.
func TestResolveColumnsNoNumericColumnsSkipsSchemaLookup(t *testing.T) {
	view, err := filterWithCacheFor("shop.notes", nil).resolveColumns(context.Background(),
		&replication.RowsEvent{Table: stringOnlyTableMap()})
	require.NoError(t, err)

	assert.Equal(t, []string{"body", "created_at"}, view.names)
	assert.Equal(t, []string{"VARCHAR", "DATETIME"}, view.types)
	assert.Equal(t, uint64(utf8mb4GeneralCI), view.collations[0])
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
	// A bare TableMapEvent and no client must fail loudly, not emit missing columns.
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

func TestIsBinaryCollation(t *testing.T) {
	testCases := []struct {
		name        string
		collationID uint64
		binary      bool
	}{
		{
			name:        "the binary charset",
			collationID: 63,
			binary:      true,
		},
		{
			name:        "utf8mb4 general",
			collationID: 45,
			binary:      false,
		},
		{
			name:        "utf8mb4 0900",
			collationID: 255,
			binary:      false,
		},
		{
			name:        "latin1 swedish",
			collationID: 8,
			binary:      false,
		},
		{
			name:        "unset",
			collationID: 0,
			binary:      false,
		},
		{
			name:        "out of range ids are not binary",
			collationID: 1 << 40,
			binary:      false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.binary, isBinaryCollation(tc.collationID))
		})
	}
}

func TestBinaryTypeName(t *testing.T) {
	testCases := []struct {
		wireType string
		want     string
	}{
		{
			wireType: "CHAR",
			want:     "BINARY",
		},
		{
			wireType: "VARCHAR",
			want:     "VARBINARY",
		},
		{
			wireType: "BLOB",
			want:     "BLOB",
		},
		{
			wireType: "LONGBLOB",
			want:     "LONGBLOB",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.wireType, func(t *testing.T) {
			assert.Equal(t, tc.want, binaryTypeName(tc.wireType))
		})
	}
}

// TestFixedStringWidth pins the declared width unpacked from a CHAR or BINARY column's TableMapEvent
// meta the three ways it is packed: a bare width, the real type in the high byte with the width
// below it, and the width's high bits folded into the type byte once the width passes 255.
func TestFixedStringWidth(t *testing.T) {
	testCases := []struct {
		name string
		meta uint16
		want int
	}{
		{
			name: "binary(16)",
			meta: uint16(mysql.MYSQL_TYPE_STRING)<<8 | 16,
			want: 16,
		},
		{
			name: "binary(255)",
			meta: uint16(mysql.MYSQL_TYPE_STRING)<<8 | 255,
			want: 255,
		},
		{
			name: "char(255) utf8mb4 width folded into the type byte",
			meta: uint16(mysql.MYSQL_TYPE_STRING^((1020&0x300)>>4))<<8 | (1020 & 0xFF),
			want: 1020,
		},
		{
			name: "bare width",
			meta: 16,
			want: 16,
		},
		{
			name: "no width",
			want: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, fixedWidth(tc.meta))
		})
	}
}

func TestTextTypeName(t *testing.T) {
	testCases := []struct {
		wireType string
		want     string
	}{
		{
			wireType: "TINYBLOB",
			want:     "TINYTEXT",
		},
		{
			wireType: "BLOB",
			want:     "TEXT",
		},
		{
			wireType: "MEDIUMBLOB",
			want:     "MEDIUMTEXT",
		},
		{
			wireType: "LONGBLOB",
			want:     "LONGTEXT",
		},
		{
			wireType: "VARCHAR",
			want:     "VARCHAR",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.wireType, func(t *testing.T) {
			assert.Equal(t, tc.want, textTypeName(tc.wireType))
		})
	}
}

// TestDecodeCollatedValue checks what a collated column's value and wire type become on each side
// of state version 8.
func TestDecodeCollatedValue(t *testing.T) {
	defer func(version int) { constants.LoadedStateVersion = version }(constants.LoadedStateVersion)

	testCases := []struct {
		name         string
		stateVersion int
		val          any
		collationID  uint64
		width        int
		columnType   string
		want         any
		wantType     string
	}{
		{
			name:         "below v8 binary bytes decode as text under the wire type",
			stateVersion: 7,
			val:          []byte{0xFF, 0x05},
			collationID:  63,
			width:        16,
			columnType:   "CHAR",
			want:         string([]byte{0xFF, 0x05}),
			wantType:     "CHAR",
		},
		{
			name:         "below v8 latin1 decodes as text under the wire type",
			stateVersion: 7,
			val:          []byte{0xE9},
			collationID:  8,
			width:        0,
			columnType:   "BLOB",
			want:         "é",
			wantType:     "BLOB",
		},
		{
			name:         "binary CHAR pads to its width as BINARY",
			stateVersion: 8,
			val:          string([]byte{0xFF, 0x05}),
			collationID:  63,
			width:        16,
			columnType:   "CHAR",
			want:         append([]byte{0xFF, 0x05}, make([]byte, 14)...),
			wantType:     "BINARY",
		},
		{
			name:         "binary VARCHAR keeps its bytes as VARBINARY",
			stateVersion: 8,
			val:          string([]byte{0x10, 0x20}),
			collationID:  63,
			width:        0,
			columnType:   "VARCHAR",
			want:         []byte{0x10, 0x20},
			wantType:     "VARBINARY",
		},
		{
			name:         "binary BLOB keeps its bytes and type",
			stateVersion: 8,
			val:          []byte{0x00, 0x01, 0xFF},
			collationID:  63,
			width:        0,
			columnType:   "BLOB",
			want:         []byte{0x00, 0x01, 0xFF},
			wantType:     "BLOB",
		},
		{
			name:         "latin1 BLOB decodes as TEXT",
			stateVersion: 8,
			val:          []byte{0xE9},
			collationID:  8,
			width:        0,
			columnType:   "BLOB",
			want:         "é",
			wantType:     "TEXT",
		},
		{
			name:         "utf8mb4 CHAR stays text",
			stateVersion: 8,
			val:          "héllo",
			collationID:  45,
			width:        0,
			columnType:   "CHAR",
			want:         "héllo",
			wantType:     "CHAR",
		},
		{
			name:         "a value that is not bytes is left alone",
			stateVersion: 8,
			val:          int64(5),
			collationID:  63,
			width:        16,
			columnType:   "CHAR",
			want:         int64(5),
			wantType:     "CHAR",
		},
		{
			name:         "a nil byte slice is left alone",
			stateVersion: 8,
			val:          []byte(nil),
			collationID:  63,
			width:        16,
			columnType:   "CHAR",
			want:         []byte(nil),
			wantType:     "CHAR",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			constants.LoadedStateVersion = tc.stateVersion
			got, gotType := decodeCollatedValue(tc.val, tc.collationID, tc.width, tc.columnType)
			assert.Equal(t, tc.want, got)
			assert.Equal(t, tc.wantType, gotType)
		})
	}
}
