package binlog

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
