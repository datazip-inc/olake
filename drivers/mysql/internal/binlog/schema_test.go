package binlog

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseEnumSetMembers(t *testing.T) {
	tests := []struct {
		name       string
		columnType string
		want       []string
	}{
		{name: "simple enum", columnType: "enum('a','b')", want: []string{"a", "b"}},
		{name: "simple set", columnType: "set('sports','music','gaming','reading')", want: []string{"sports", "music", "gaming", "reading"}},
		{name: "member containing a comma", columnType: "set('x','y,z')", want: []string{"x", "y,z"}},
		{name: "doubled-quote escape", columnType: "enum('it''s','ok')", want: []string{"it's", "ok"}},
		{name: "backslash escape", columnType: `enum('it\'s','ok')`, want: []string{"it's", "ok"}},
		{name: "empty member", columnType: "enum('','a')", want: []string{"", "a"}},
		{name: "multi-byte member", columnType: "enum('日本語','ok')", want: []string{"日本語", "ok"}},
		{name: "member containing parens", columnType: "enum('a(1)','b')", want: []string{"a(1)", "b"}},
		{name: "single member", columnType: "enum('only')", want: []string{"only"}},
		{name: "no parens", columnType: "enum", want: nil},
		{name: "empty body", columnType: "enum()", want: nil},
		{name: "unclosed", columnType: "enum('a'", want: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, parseEnumSetMembers(tt.columnType))
		})
	}
}

func TestCollationIDByName(t *testing.T) {
	// IDs are MySQL's own; they must match what the binlog carries when metadata is FULL.
	tests := []struct {
		name string
		want uint64
	}{
		{name: "utf8mb4_general_ci", want: 45},
		{name: "latin1_swedish_ci", want: 8},
		{name: "ucs2_general_ci", want: 35},
		{name: "utf8mb4_0900_ai_ci", want: 255},
		{name: "", want: 0},
		{name: "not_a_collation", want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, collationIDByName(tt.name))
		})
	}
}

func TestIsDDL(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  bool
	}{
		{name: "begin", query: "BEGIN", want: false},
		{name: "commit", query: "COMMIT", want: false},
		{name: "rollback", query: "ROLLBACK", want: false},
		{name: "savepoint", query: "SAVEPOINT sp1", want: false},
		{name: "truncate leaves columns alone", query: "TRUNCATE TABLE users", want: false},
		{name: "insert", query: "INSERT INTO users VALUES (1)", want: false},
		{name: "alter table", query: "ALTER TABLE users ADD COLUMN age INT", want: true},
		{name: "lowercase alter", query: "alter table users drop column age", want: true},
		{name: "leading whitespace", query: "\n\t ALTER TABLE users ADD COLUMN age INT", want: true},
		{name: "comment-prefixed migration", query: "/* gh-ost */ ALTER TABLE users ADD COLUMN age INT", want: true},
		{name: "multiline comment prefix", query: "/* migration\n123 */ ALTER TABLE t ADD c INT", want: true},
		{name: "rename table", query: "RENAME TABLE users TO _users_del, _users_gho TO users", want: true},
		{name: "drop table", query: "DROP TABLE users", want: true},
		{name: "mariadb create or replace", query: "CREATE OR REPLACE TABLE users (id INT)", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isDDL(tt.query))
		})
	}
}

// TestNewSchemaCache covers seeding the cache from a saved state.
func TestNewSchemaCache(t *testing.T) {
	t.Run("keeps only schemas valid at resume", testSeedKeepsOnlySchemasValidAtResume)
	t.Run("old state without schemas seeds nothing", testOldStateWithoutSchemasSeedsNothing)
	t.Run("GTID state preserves schemas across server positions", func(t *testing.T) {
		gtid := testSID + ":1-3"
		state := Binlog{Position: mysql.Position{Name: "mysql-bin.000001", Pos: 100}, GTIDSet: &gtid,
			Schemas: map[string]*tableMeta{"shop.orders": {
				Columns: []columnMeta{{Name: "id"}}, AnchoredAt: ddlPos(900),
			}}}
		c := newSchemaCache(nil, state, false)
		set, err := parseGTIDSet(gtid)
		require.NoError(t, err)
		assert.Equal(t, state.Schemas, c.snapshot(state.Position, set))
	})
}

// A saved schema anchored after the resume position may already include DDL the reader has
// not reached, so newSchemaCache must drop it; an empty anchor compares as earliest and is
// dropped too.
func testSeedKeepsOnlySchemasValidAtResume(t *testing.T) {
	resume := mysql.Position{Name: "mysql-bin.000010", Pos: 500}
	saved := map[string]*tableMeta{
		"shop.before": {Columns: []columnMeta{{Name: "id"}}, AnchoredAt: mysql.Position{Name: "mysql-bin.000010", Pos: 400}},
		"shop.same":   {Columns: []columnMeta{{Name: "id"}}, AnchoredAt: resume},
		"shop.after":  {Columns: []columnMeta{{Name: "id"}}, AnchoredAt: mysql.Position{Name: "mysql-bin.000012", Pos: 900}},
		"shop.empty":  {Columns: []columnMeta{{Name: "id"}}},
	}

	c := newSchemaCache(nil, Binlog{Position: resume, Schemas: saved}, false)

	assert.Contains(t, c.tables, "shop.before")
	assert.Contains(t, c.tables, "shop.same")
	assert.NotContains(t, c.tables, "shop.after")
	assert.NotContains(t, c.tables, "shop.empty")
}

// TestBinlogJSON covers the saved state's JSON.
func TestBinlogJSON(t *testing.T) {
	t.Run("schemas round trip", testBinlogSchemasRoundTrip)
	t.Run("format", testBinlogJSONFormat)
}

func testBinlogSchemasRoundTrip(t *testing.T) {
	in := Binlog{
		Position: mysql.Position{Name: "mysql-bin.000010", Pos: 500},
		Schemas: map[string]*tableMeta{
			"shop.orders": {
				Columns:    []columnMeta{{Name: "status", EnumValues: []string{"new", "paid"}}},
				AnchoredAt: mysql.Position{Name: "mysql-bin.000010", Pos: 400},
			},
		},
	}
	raw, err := json.Marshal(in)
	require.NoError(t, err)

	var out Binlog
	require.NoError(t, json.Unmarshal(raw, &out))
	assert.Equal(t, in, out)
}

// State files written before schemas were persisted must still load, with an empty cache.
func testOldStateWithoutSchemasSeedsNothing(t *testing.T) {
	var out Binlog
	require.NoError(t, json.Unmarshal([]byte(`{"position":{"Name":"mysql-bin.000010","Pos":500}}`), &out))
	assert.Nil(t, out.Schemas)

	c := newSchemaCache(nil, out, false)
	assert.Empty(t, c.tables)
}

// TestSchemaCacheGet covers get: cached answers and the table id check.
func TestSchemaCacheGet(t *testing.T) {
	t.Run("seeded schema answers without a query", testSeededSchemaAnswersWithoutQuery)
	t.Run("table id", testSchemaCacheGetTableID)
	t.Run("table id after resume", testSchemaCacheGetTableIDAfterResume)
	t.Run("GTID load waits for its logical anchor", testSchemaCacheGetGTIDAnchor)
}

func testSchemaCacheGetGTIDAnchor(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, mock.ExpectationsWereMet())
		_ = db.Close()
	})
	saved, loaded := testSID+":1-2", testSID+":1-3"
	c := newSchemaCache(sqlx.NewDb(db, "sqlmock"), Binlog{GTIDSet: &saved}, false)
	mock.ExpectQuery(jdbc.MySQLCDCColumnMetadataQuery()).WithArgs("shop", "orders").
		WillReturnRows(sqlmock.NewRows([]string{"name", "type", "collation"}).AddRow("id", "int", ""))
	mock.ExpectQuery(jdbc.MySQLCDCTableCollationQuery()).WithArgs("shop", "orders").
		WillReturnRows(sqlmock.NewRows([]string{"collation"}).AddRow("utf8mb4_general_ci"))
	mock.ExpectQuery(jdbc.MySQLCDCSchemaCollationQuery()).WithArgs("shop").
		WillReturnRows(sqlmock.NewRows([]string{"collation"}).AddRow("utf8mb4_general_ci"))
	mock.ExpectQuery("SELECT @@version").WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("8.4.2"))
	mock.ExpectQuery(jdbc.MySQLGTIDModeQuery()).WillReturnRows(sqlmock.NewRows([]string{"uuid", "mode"}).AddRow("server-b", "ON"))
	mock.ExpectQuery(jdbc.MySQLMasterStatusQueryNew()).WillReturnRows(sqlmock.NewRows([]string{"file", "position", "do", "ignore", "gtids"}).
		AddRow("mysql-bin.000001", 200, "", "", loaded))
	meta, err := c.get(context.Background(), "shop", "orders", 7, []byte{mysql.MYSQL_TYPE_LONG})
	require.NoError(t, err)
	processed, err := parseGTIDSet(saved)
	require.NoError(t, err)
	// Physical progress can pass the SQL anchor while a reordered transaction remains outside the target.
	pos := mysql.Position{Name: "mysql-bin.000001", Pos: 900}
	assert.Empty(t, c.snapshot(pos, processed))
	require.NoError(t, processed.Update(loaded))
	assert.Equal(t, meta, c.snapshot(pos, processed)["shop.orders"])
}

// The cache has no client, so any information_schema query would fail: getting the saved
// schema back proves a resumed run decodes with the schema as of its start position.
func testSeededSchemaAnswersWithoutQuery(t *testing.T) {
	resume := mysql.Position{Name: "mysql-bin.000010", Pos: 500}
	c := newSchemaCache(nil, Binlog{Position: resume, Schemas: map[string]*tableMeta{
		"shop.orders": {
			Columns:    []columnMeta{{Name: "status", EnumValues: []string{"new", "paid"}}},
			AnchoredAt: mysql.Position{Name: "mysql-bin.000010", Pos: 400},
		},
	}}, false)

	meta, err := c.get(context.Background(), "shop", "orders", 0, []byte{mysql.MYSQL_TYPE_STRING})
	require.NoError(t, err)
	assert.Equal(t, []string{"new", "paid"}, meta.Columns[0].EnumValues)
}

func TestConnection_Checkpoint(t *testing.T) {
	pos := mysql.Position{Name: "mysql-bin.000010", Pos: 500}
	saved := map[string]*tableMeta{
		"shop.orders": {Columns: []columnMeta{{Name: "status"}}, AnchoredAt: mysql.Position{Name: "mysql-bin.000010", Pos: 400}},
		"shop.future": {Columns: []columnMeta{{Name: "id"}}, AnchoredAt: mysql.Position{Name: "mysql-bin.000012", Pos: 900}},
	}

	conn, err := NewConnection(context.Background(), &Config{ServerID: 1001, Flavor: "mysql"},
		Binlog{Position: pos, Schemas: saved}, nil, identityConverter)
	require.NoError(t, err)
	defer conn.Cleanup()

	state := conn.Checkpoint()
	assert.Equal(t, pos, state.Position)
	assert.Contains(t, state.Schemas, "shop.orders")
	assert.NotContains(t, state.Schemas, "shop.future")
}

// The saved state format must not change by accident: a renamed Go field would otherwise
// silently drop every saved schema on upgrade. TableID is left out: MySQL reassigns it on restart.
func testBinlogJSONFormat(t *testing.T) {
	in := Binlog{
		Position: mysql.Position{Name: "mysql-bin.000010", Pos: 500},
		Schemas: map[string]*tableMeta{
			"shop.orders": {
				Columns: []columnMeta{
					{Name: "id", Unsigned: true},
					{Name: "status", EnumValues: []string{"new"}},
					{Name: "tags", SetMembers: []string{"a"}},
					{Name: "note", CollationID: 45},
				},
				AnchoredAt:         mysql.Position{Name: "mysql-bin.000010", Pos: 400},
				DefaultCollationID: 45,
				SchemaCollationID:  8,
				TableID:            7,
			},
		},
	}
	raw, err := json.Marshal(in)
	require.NoError(t, err)
	assert.JSONEq(t, `{
		"position": {"Name": "mysql-bin.000010", "Pos": 500},
		"schemas": {"shop.orders": {
			"columns": [
				{"name": "id", "unsigned": true},
				{"name": "status", "enum_values": ["new"]},
				{"name": "tags", "set_members": ["a"]},
				{"name": "note", "collation_id": 45}
			],
			"anchored_at": {"Name": "mysql-bin.000010", "Pos": 400},
			"default_collation_id": 45,
			"schema_collation_id": 8
		}}
	}`, string(raw))
}

// The cache has no client, so a reload fails: an error proves get tried to reload. The cached
// entry is (id INT, name VARCHAR).
func testSchemaCacheGetTableID(t *testing.T) {
	intVarchar := []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_VARCHAR}
	varcharInt := []byte{mysql.MYSQL_TYPE_VARCHAR, mysql.MYSQL_TYPE_LONG}
	tests := []struct {
		name        string
		cachedID    uint64
		cachedTypes []byte
		eventID     uint64
		eventTypes  []byte
		wantError   bool
	}{
		{name: "same id uses cache", cachedID: 7, cachedTypes: intVarchar, eventID: 7, eventTypes: intVarchar},
		{name: "unset id records the new one", eventID: 9, eventTypes: intVarchar},
		{name: "changed id, same types, keeps cache", cachedID: 7, cachedTypes: intVarchar, eventID: 9, eventTypes: intVarchar},
		{name: "changed id and reordered types reloads", cachedID: 7, cachedTypes: intVarchar, eventID: 9, eventTypes: varcharInt, wantError: true},
		{name: "changed id and column count reloads", cachedID: 7, cachedTypes: intVarchar, eventID: 9, eventTypes: []byte{mysql.MYSQL_TYPE_LONG}, wantError: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newSchemaCache(nil, Binlog{}, false)
			c.tables["shop.orders"] = &tableMeta{
				Columns:     []columnMeta{{Name: "id"}, {Name: "name"}},
				TableID:     tt.cachedID,
				ColumnTypes: tt.cachedTypes,
			}

			meta, err := c.get(context.Background(), "shop", "orders", tt.eventID, tt.eventTypes)
			if tt.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.eventID, meta.TableID)
			assert.Equal(t, tt.eventTypes, meta.ColumnTypes)
		})
	}
}

// A MySQL restart gives tables new ids, so a resumed run must adopt the new id rather than
// reload: the cache has no client, so a reload would fail.
func testSchemaCacheGetTableIDAfterResume(t *testing.T) {
	pos := mysql.Position{Name: "mysql-bin.000010", Pos: 400}
	raw, err := json.Marshal(Binlog{Position: pos, Schemas: map[string]*tableMeta{
		"shop.orders": {Columns: []columnMeta{{Name: "id"}}, AnchoredAt: pos, TableID: 112},
	}})
	require.NoError(t, err)
	var state Binlog
	require.NoError(t, json.Unmarshal(raw, &state))

	c := newSchemaCache(nil, state, false)
	meta, err := c.get(context.Background(), "shop", "orders", 98, []byte{mysql.MYSQL_TYPE_LONG})
	require.NoError(t, err)
	assert.Equal(t, uint64(98), meta.TableID)
}

// State saves only schemas valid at the resume position: a later anchor may include DDL the
// next run has yet to read.
func TestSchemaCacheSnapshot(t *testing.T) {
	before := &tableMeta{Columns: []columnMeta{{Name: "id"}}, AnchoredAt: mysql.Position{Name: "mysql-bin.000010", Pos: 400}}
	at := &tableMeta{Columns: []columnMeta{{Name: "id"}}, AnchoredAt: mysql.Position{Name: "mysql-bin.000010", Pos: 500}}
	after := &tableMeta{Columns: []columnMeta{{Name: "id"}}, AnchoredAt: mysql.Position{Name: "mysql-bin.000011", Pos: 100}}

	tests := []struct {
		name string
		upTo mysql.Position
		want []string
	}{
		{name: "keeps entries anchored at or before upTo", upTo: mysql.Position{Name: "mysql-bin.000010", Pos: 500}, want: []string{"shop.before", "shop.at"}},
		{name: "keeps every entry once past all anchors", upTo: mysql.Position{Name: "mysql-bin.000011", Pos: 100}, want: []string{"shop.before", "shop.at", "shop.after"}},
		{name: "keeps none before every anchor", upTo: mysql.Position{Name: "mysql-bin.000010", Pos: 300}, want: []string{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newSchemaCache(nil, Binlog{}, false)
			c.tables = map[string]*tableMeta{"shop.before": before, "shop.at": at, "shop.after": after}

			got := c.snapshot(tt.upTo, nil)

			keys := make([]string, 0, len(got))
			for key := range got {
				keys = append(keys, key)
			}
			assert.ElementsMatch(t, tt.want, keys)
			assert.Len(t, c.tables, 3, "snapshot must not change the cache")
		})
	}
}

// StreamMessages reads past its stop until it passes maxAnchor, so the latest anchor must win,
// across binlog files too.
func TestSchemaCacheMaxAnchor(t *testing.T) {
	tests := []struct {
		name    string
		anchors map[string]mysql.Position
		want    mysql.Position
	}{
		{name: "empty cache gives the zero position", anchors: nil, want: mysql.Position{}},
		{name: "latest offset in one file", anchors: map[string]mysql.Position{
			"shop.a": {Name: "mysql-bin.000010", Pos: 400},
			"shop.b": {Name: "mysql-bin.000010", Pos: 900},
			"shop.c": {Name: "mysql-bin.000010", Pos: 500},
		}, want: mysql.Position{Name: "mysql-bin.000010", Pos: 900}},
		{name: "later file beats a larger offset", anchors: map[string]mysql.Position{
			"shop.a": {Name: "mysql-bin.000010", Pos: 900},
			"shop.b": {Name: "mysql-bin.000011", Pos: 100},
		}, want: mysql.Position{Name: "mysql-bin.000011", Pos: 100}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newSchemaCache(nil, Binlog{}, false)
			for key, anchor := range tt.anchors {
				c.tables[key] = &tableMeta{Columns: []columnMeta{{Name: "id"}}, AnchoredAt: anchor}
			}
			assert.Equal(t, tt.want, c.maxAnchor())
		})
	}
}

func TestConnection_LastPosToRead(t *testing.T) {
	target, anchor := ddlPos(200), ddlPos(500)
	gtid, err := parseGTIDSet(testSID + ":1-2")
	require.NoError(t, err)
	for _, tt := range []struct {
		name       string
		recovering bool
		gtid       mysql.GTIDSet
		want       mysql.Position
	}{
		{"file sync reaches schema anchor", false, nil, anchor},
		{"file recovery keeps committed boundary", true, nil, target},
		{"GTID sync keeps logical target", false, gtid, target},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cache := newSchemaCache(nil, Binlog{}, false)
			cache.setTable("shop.orders", &tableMeta{AnchoredAt: anchor})
			conn := &Connection{gtidSet: tt.gtid, changeFilter: ChangeFilter{schema: cache}}
			assert.Equal(t, tt.want, conn.lastPosToRead(target, tt.recovering))
		})
	}
}
