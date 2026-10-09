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
// not reached, so seed must drop it; an empty anchor compares as earliest and is dropped too.
func TestSeedKeepsOnlySchemasValidAtResume(t *testing.T) {
	resume := mysql.Position{Name: "mysql-bin.000010", Pos: 500}
	saved := map[string]*tableMeta{
		"shop.before": {Columns: []columnMeta{{Name: "id"}}, AnchoredAt: mysql.Position{Name: "mysql-bin.000010", Pos: 400}},
		"shop.same":   {Columns: []columnMeta{{Name: "id"}}, AnchoredAt: resume},
		"shop.after":  {Columns: []columnMeta{{Name: "id"}}, AnchoredAt: mysql.Position{Name: "mysql-bin.000012", Pos: 900}},
		"shop.empty":  {Columns: []columnMeta{{Name: "id"}}},
	}

	c := newSchemaCache(nil)
	c.seed(saved, resume)

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

	c := newSchemaCache(nil)
	c.seed(out.Schemas, out.Position)
	assert.Empty(t, c.tables)
}

// The cache has no client, so any information_schema query would fail: getting the seeded
// schema back proves a resumed run decodes with the schema as of its start position.
func TestSeededSchemaAnswersWithoutQuery(t *testing.T) {
	resume := mysql.Position{Name: "mysql-bin.000010", Pos: 500}
	c := newSchemaCache(nil)
	c.seed(map[string]*tableMeta{
		"shop.orders": {
			Columns:    []columnMeta{{Name: "status", EnumValues: []string{"new", "paid"}}},
			AnchoredAt: mysql.Position{Name: "mysql-bin.000010", Pos: 400},
		},
	}, resume)

	meta, err := c.get(context.Background(), "shop", "orders", mysql.Position{Name: "mysql-bin.000010", Pos: 700})
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
