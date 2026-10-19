package binlog

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseEnumSetMembers(t *testing.T) {
	tests := []struct {
		name       string
		columnType string
		want       []string
	}{
		{"simple enum", "enum('a','b')", []string{"a", "b"}},
		{"simple set", "set('sports','music','gaming','reading')", []string{"sports", "music", "gaming", "reading"}},
		{"member containing a comma", "set('x','y,z')", []string{"x", "y,z"}},
		{"doubled-quote escape", "enum('it''s','ok')", []string{"it's", "ok"}},
		{"backslash escape", `enum('it\'s','ok')`, []string{"it's", "ok"}},
		{"empty member", "enum('','a')", []string{"", "a"}},
		{"multi-byte member", "enum('日本語','ok')", []string{"日本語", "ok"}},
		{"member containing parens", "enum('a(1)','b')", []string{"a(1)", "b"}},
		{"single member", "enum('only')", []string{"only"}},
		{"no parens", "enum", nil},
		{"empty body", "enum()", nil},
		{"unclosed", "enum('a'", nil},
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
		{"utf8mb4_general_ci", 45},
		{"latin1_swedish_ci", 8},
		{"ucs2_general_ci", 35},
		{"utf8mb4_0900_ai_ci", 255},
		{"", 0},
		{"not_a_collation", 0},
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
		{"begin", "BEGIN", false},
		{"commit", "COMMIT", false},
		{"rollback", "ROLLBACK", false},
		{"savepoint", "SAVEPOINT sp1", false},
		{"truncate leaves columns alone", "TRUNCATE TABLE users", false},
		{"insert", "INSERT INTO users VALUES (1)", false},
		{"alter table", "ALTER TABLE users ADD COLUMN age INT", true},
		{"lowercase alter", "alter table users drop column age", true},
		{"leading whitespace", "\n\t ALTER TABLE users ADD COLUMN age INT", true},
		{"comment-prefixed migration", "/* gh-ost */ ALTER TABLE users ADD COLUMN age INT", true},
		{"multiline comment prefix", "/* migration\n123 */ ALTER TABLE t ADD c INT", true},
		{"rename table", "RENAME TABLE users TO _users_del, _users_gho TO users", true},
		{"drop table", "DROP TABLE users", true},
		{"mariadb create or replace", "CREATE OR REPLACE TABLE users (id INT)", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isDDL([]byte(tt.query)))
		})
	}
}

// A saved schema anchored after the resume position may already include DDL the reader has
// not reached, so newSchemaCache must drop it; an empty anchor compares as earliest and is
// dropped too.
func TestSeedKeepsOnlySchemasValidAtResume(t *testing.T) {
	resume := mysql.Position{Name: "mysql-bin.000010", Pos: 500}
	saved := map[string]*tableMeta{
		"shop.before": {Columns: []columnMeta{{Name: "id"}}, AnchoredAt: mysql.Position{Name: "mysql-bin.000010", Pos: 400}},
		"shop.same":   {Columns: []columnMeta{{Name: "id"}}, AnchoredAt: resume},
		"shop.after":  {Columns: []columnMeta{{Name: "id"}}, AnchoredAt: mysql.Position{Name: "mysql-bin.000012", Pos: 900}},
		"shop.empty":  {Columns: []columnMeta{{Name: "id"}}},
	}

	c := newSchemaCache(nil, Binlog{Position: resume, Schemas: saved})

	assert.Contains(t, c.tables, "shop.before")
	assert.Contains(t, c.tables, "shop.same")
	assert.NotContains(t, c.tables, "shop.after")
	assert.NotContains(t, c.tables, "shop.empty")
}

func TestBinlogSchemasRoundTrip(t *testing.T) {
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
func TestOldStateWithoutSchemasSeedsNothing(t *testing.T) {
	var out Binlog
	require.NoError(t, json.Unmarshal([]byte(`{"position":{"Name":"mysql-bin.000010","Pos":500}}`), &out))
	assert.Nil(t, out.Schemas)

	c := newSchemaCache(nil, out)
	assert.Empty(t, c.tables)
}

// The cache has no client, so any information_schema query would fail: getting the saved
// schema back proves a resumed run decodes with the schema as of its start position.
func TestSeededSchemaAnswersWithoutQuery(t *testing.T) {
	resume := mysql.Position{Name: "mysql-bin.000010", Pos: 500}
	c := newSchemaCache(nil, Binlog{Position: resume, Schemas: map[string]*tableMeta{
		"shop.orders": {
			Columns:    []columnMeta{{Name: "status", EnumValues: []string{"new", "paid"}}},
			AnchoredAt: mysql.Position{Name: "mysql-bin.000010", Pos: 400},
		},
	}})

	meta, err := c.get(context.Background(), "shop", "orders", 0, 1)
	require.NoError(t, err)
	assert.Equal(t, []string{"new", "paid"}, meta.Columns[0].EnumValues)
}

func TestConnectionSeedsAndReturnsState(t *testing.T) {
	pos := mysql.Position{Name: "mysql-bin.000010", Pos: 500}
	saved := map[string]*tableMeta{
		"shop.orders": {Columns: []columnMeta{{Name: "status"}}, AnchoredAt: mysql.Position{Name: "mysql-bin.000010", Pos: 400}},
		"shop.future": {Columns: []columnMeta{{Name: "id"}}, AnchoredAt: mysql.Position{Name: "mysql-bin.000012", Pos: 900}},
	}

	conn, err := NewConnection(context.Background(), &Config{ServerID: 1001, Flavor: "mysql"},
		Binlog{Position: pos, Schemas: saved}, nil, identityConverter)
	require.NoError(t, err)
	defer conn.Cleanup()

	state := conn.State()
	assert.Equal(t, pos, state.Position)
	assert.Contains(t, state.Schemas, "shop.orders")
	assert.NotContains(t, state.Schemas, "shop.future")
}

// The saved state format must not change by accident: a renamed Go field would otherwise
// silently drop every saved schema on upgrade. TableID is left out: MySQL reassigns it on restart.
func TestBinlogJSONFormat(t *testing.T) {
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
			"default_collation_id": 45
		}}
	}`, string(raw))
}

// The cache has no client, so a reload fails: an error proves get tried to reload. The cached
// entry has one column.
func TestSchemaCacheGetTableID(t *testing.T) {
	tests := []struct {
		name      string
		cachedID  uint64
		eventID   uint64
		columns   int
		wantError bool
	}{
		{"same id uses cache", 7, 7, 1, false},
		{"unset id records the new one", 0, 9, 1, false},
		{"changed id, same column count, keeps cache", 7, 9, 1, false},
		{"changed id and column count reloads", 7, 9, 2, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newSchemaCache(nil, Binlog{})
			c.tables["shop.orders"] = &tableMeta{Columns: []columnMeta{{Name: "id"}}, TableID: tt.cachedID}

			meta, err := c.get(context.Background(), "shop", "orders", tt.eventID, tt.columns)
			if tt.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.eventID, meta.TableID)
		})
	}
}

// A MySQL restart gives tables new ids, so a resumed run must adopt the new id rather than
// reload: the cache has no client, so a reload would fail.
func TestSchemaCacheGetTableIDAfterResume(t *testing.T) {
	pos := mysql.Position{Name: "mysql-bin.000010", Pos: 400}
	raw, err := json.Marshal(Binlog{Position: pos, Schemas: map[string]*tableMeta{
		"shop.orders": {Columns: []columnMeta{{Name: "id"}}, AnchoredAt: pos, TableID: 112},
	}})
	require.NoError(t, err)
	var state Binlog
	require.NoError(t, json.Unmarshal(raw, &state))

	c := newSchemaCache(nil, state)
	meta, err := c.get(context.Background(), "shop", "orders", 98, 1)
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
		{"keeps entries anchored at or before upTo", mysql.Position{Name: "mysql-bin.000010", Pos: 500}, []string{"shop.before", "shop.at"}},
		{"keeps every entry once past all anchors", mysql.Position{Name: "mysql-bin.000011", Pos: 100}, []string{"shop.before", "shop.at", "shop.after"}},
		{"keeps none before every anchor", mysql.Position{Name: "mysql-bin.000010", Pos: 300}, []string{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newSchemaCache(nil, Binlog{})
			c.tables = map[string]*tableMeta{"shop.before": before, "shop.at": at, "shop.after": after}

			got := c.snapshot(tt.upTo)

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
		{"empty cache gives the zero position", nil, mysql.Position{}},
		{"latest offset in one file", map[string]mysql.Position{
			"shop.a": {Name: "mysql-bin.000010", Pos: 400},
			"shop.b": {Name: "mysql-bin.000010", Pos: 900},
			"shop.c": {Name: "mysql-bin.000010", Pos: 500},
		}, mysql.Position{Name: "mysql-bin.000010", Pos: 900}},
		{"later file beats a larger offset", map[string]mysql.Position{
			"shop.a": {Name: "mysql-bin.000010", Pos: 900},
			"shop.b": {Name: "mysql-bin.000011", Pos: 100},
		}, mysql.Position{Name: "mysql-bin.000011", Pos: 100}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newSchemaCache(nil, Binlog{})
			for key, anchor := range tt.anchors {
				c.tables[key] = &tableMeta{Columns: []columnMeta{{Name: "id"}}, AnchoredAt: anchor}
			}
			assert.Equal(t, tt.want, c.maxAnchor())
		})
	}
}
