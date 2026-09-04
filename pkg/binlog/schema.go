package binlog

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/jmoiron/sqlx"
	"github.com/pingcap/tidb/pkg/parser/charset"
)

// columnMeta is one column's decoding info, derived from information_schema.
// Index in tableMeta.Columns is the column's ordinal position, which matches
// TableMapEvent.ColumnType ordering.
type columnMeta struct {
	Name        string
	Unsigned    bool
	EnumValues  []string // ENUM members in definition order; value idx 1..n
	SetMembers  []string // SET members; bit 0..n-1
	CollationID uint64   // 0 when the column is not character data
}

type tableMeta struct {
	Columns []columnMeta
}

// schemaCache supplies the column metadata the binlog omits when the server does not
// emit full row metadata. Safe for concurrent use; StreamMessages is single-goroutine
// today but the cache outlives one event.
type schemaCache struct {
	mu     sync.RWMutex
	client *sqlx.DB
	tables map[string]*tableMeta
}

func newSchemaCache(client *sqlx.DB) *schemaCache {
	return &schemaCache{client: client, tables: map[string]*tableMeta{}}
}

func (c *schemaCache) get(ctx context.Context, schema, table string) (*tableMeta, error) {
	key := schema + "." + table

	c.mu.RLock()
	meta, ok := c.tables[key]
	c.mu.RUnlock()
	if ok {
		return meta, nil
	}

	if c.client == nil {
		return nil, fmt.Errorf("binlog metadata for %s.%s is incomplete and no schema client is configured; "+
			"set binlog_row_metadata=FULL or report this as a bug", schema, table)
	}

	meta, err := c.load(ctx, schema, table)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	c.tables[key] = meta
	c.mu.Unlock()
	return meta, nil
}

// invalidate drops every cached table. Called on any DDL seen in the binlog: DDL is
// rare enough that a full drop is cheaper to reason about than tracking which table
// a statement touched, and lazy reload means we only pay for tables still streaming.
func (c *schemaCache) invalidate() {
	c.mu.Lock()
	c.tables = map[string]*tableMeta{}
	c.mu.Unlock()
}

func (c *schemaCache) load(ctx context.Context, schema, table string) (*tableMeta, error) {
	rows, err := c.client.QueryContext(ctx, jdbc.MySQLCDCColumnMetadataQuery(), schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to load column metadata for %s.%s: %w", schema, table, err)
	}
	defer rows.Close()

	meta := &tableMeta{}
	for rows.Next() {
		var name, columnType, collationName string
		if err := rows.Scan(&name, &columnType, &collationName); err != nil {
			return nil, fmt.Errorf("failed to scan column metadata for %s.%s: %w", schema, table, err)
		}

		meta.Columns = append(meta.Columns, columnMetaFrom(name, columnType, collationName))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read column metadata for %s.%s: %w", schema, table, err)
	}
	if len(meta.Columns) == 0 {
		return nil, fmt.Errorf("no columns found for %s.%s", schema, table)
	}
	return meta, nil
}

// binaryCollationID is MySQL's `binary` collation. TableMapEvent labels BINARY, VARBINARY
// and BLOB columns with it when metadata is FULL, and decodeBytesToString passes their
// bytes through unchanged. Some servers report COLLATION_NAME as NULL for these columns
// rather than 'binary', so the type is checked as well to keep both paths identical.
const binaryCollationID = 63

// binaryStringTypeRe matches the COLUMN_TYPE values MySQL stores with charset `binary`.
// GEOMETRY is deliberately absent: go-mysql decodes it as a blob, but TableMapEvent does
// not count it as a character column outside MariaDB, so it carries no collation.
var binaryStringTypeRe = regexp.MustCompile(`^(binary|varbinary|tinyblob|blob|mediumblob|longblob)\b`)

// columnMetaFrom builds one column's decoding info from its information_schema row.
func columnMetaFrom(name, columnType, collationName string) columnMeta {
	lowered := strings.ToLower(columnType)

	collationID := collationIDByName(collationName)
	if collationID == 0 && binaryStringTypeRe.MatchString(lowered) {
		collationID = binaryCollationID
	}

	col := columnMeta{
		Name:        name,
		Unsigned:    strings.Contains(lowered, "unsigned"),
		CollationID: collationID,
	}
	switch {
	case strings.HasPrefix(lowered, "enum("):
		col.EnumValues = parseEnumSetMembers(columnType)
	case strings.HasPrefix(lowered, "set("):
		col.SetMembers = parseEnumSetMembers(columnType)
	}
	return col
}

// collationIDByName resolves a MySQL collation name to its numeric ID, matching the
// IDs the binlog carries when metadata is FULL. Returns 0 for binary/unknown columns,
// which decodeBytesToString already treats as passthrough.
func collationIDByName(name string) uint64 {
	if name == "" {
		return 0
	}
	coll, err := charset.GetCollationByName(name)
	if err != nil || coll == nil || coll.ID < 0 {
		return 0
	}
	return uint64(coll.ID)
}

// parseEnumSetMembers extracts members from an information_schema COLUMN_TYPE value
// such as enum('a','b') or set('x','y,z','it”s'). Handles both quote-escaping forms
// MySQL emits: doubled quotes (”) and backslash escapes (\').
func parseEnumSetMembers(columnType string) []string {
	open := strings.Index(columnType, "(")
	closing := strings.LastIndex(columnType, ")")
	if open < 0 || closing <= open {
		return nil
	}
	body := columnType[open+1 : closing]

	var (
		members []string
		cur     strings.Builder
		inQuote bool
	)
	for i := 0; i < len(body); i++ {
		ch := body[i]
		switch {
		case !inQuote && ch == '\'':
			inQuote = true
		case inQuote && ch == '\\' && i+1 < len(body):
			i++
			cur.WriteByte(body[i])
		case inQuote && ch == '\'' && i+1 < len(body) && body[i+1] == '\'':
			i++
			cur.WriteByte('\'')
		case inQuote && ch == '\'':
			inQuote = false
			members = append(members, cur.String())
			cur.Reset()
		case inQuote:
			cur.WriteByte(ch)
		}
	}
	return members
}

// ddlRe matches the DDL that can change a table's column layout. BEGIN, COMMIT, ROLLBACK
// and SAVEPOINT also arrive as QueryEvents and must not invalidate, and neither must
// TRUNCATE — it removes rows, not columns. Leading comments are skipped so annotated
// migrations still match.
//
// CREATE stays in the list even though a table already in the cache cannot be created:
// MariaDB's CREATE OR REPLACE TABLE drops and recreates, which does reshape an existing
// name.
//
// TODO: attribute DDL to the table it touches, so a migration against a table we do not
// capture — gh-ost/pt-osc shadow tables, partition maintenance elsewhere — stops evicting
// every cached stream. Needs real statement parsing to be safe: RENAME and DROP TABLE
// accept table lists, and the two-table swap those tools use at cutover
// (RENAME TABLE users TO _users_del, _users_gho TO users) must not be attributed to its
// first name alone. Until then every invalidation is logged by the caller; a log full of
// them during a migration is the signal to pick this up.
var ddlRe = regexp.MustCompile(`(?is)^\s*(?:/\*.*?\*/\s*)*(alter|rename|drop|create)\s`)

func isDDL(query []byte) bool {
	return ddlRe.Match(query)
}
