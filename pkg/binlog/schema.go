package binlog

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/pingcap/tidb/pkg/parser/charset"
)

// columnMeta is one column's decoding info from information_schema. Its index in
// tableMeta.Columns is the ordinal position, matching TableMapEvent.ColumnType. The json tags
// fix the saved state format, so renaming a field cannot silently change it.
type columnMeta struct {
	Name        string   `json:"name"`
	Unsigned    bool     `json:"unsigned,omitempty"`
	EnumValues  []string `json:"enum_values,omitempty"`  // ENUM members in definition order; value idx 1..n
	SetMembers  []string `json:"set_members,omitempty"`  // SET members; bit 0..n-1
	CollationID uint64   `json:"collation_id,omitempty"` // 0 when the column is not character data
}

// tableMeta is one table's decoding info, valid at the binlog position AnchoredAt.
type tableMeta struct {
	Columns    []columnMeta   `json:"columns"`
	AnchoredAt mysql.Position `json:"anchored_at"`
	// DefaultCollationID is the table default, which a column added without a charset takes.
	DefaultCollationID uint64 `json:"default_collation_id,omitempty"`
	// TableID is the TableMapEvent.TableID last seen in this run, 0 until the table's first row.
	// Not saved: MySQL reassigns ids on restart and FLUSH TABLES.
	TableID uint64 `json:"-"`
}

// schemaCache supplies the column metadata the binlog omits. Safe for concurrent use:
// StreamMessages is single-goroutine today, but the cache outlives one event.
//
// Each entry carries AnchoredAt, the binlog position it is valid at. Entries are saved in
// Binlog.Schemas and loaded back by newSchemaCache on resume, and applyDDL (ddl.go) advances
// them through each DDL in stream order, so they describe tables at the reader's position.
// Only a table's first load reads information_schema, the server's present, and get()
// anchors that load.
type schemaCache struct {
	mu     sync.RWMutex
	client *sqlx.DB
	tables map[string]*tableMeta
}

// tableKey is the key a table's entry is cached under, "schematable". Every lookup, store and
// drop builds it here, so a DDL finds exactly the entry a row event cached.
func tableKey(schema, table string) string {
	return schema + "." + table
}

// newSchemaCache starts from the previous run's saved schemas, keeping only those valid at or
// before the resume position. A later anchor may already include DDL the reader has not reached,
// so that table is reloaded instead. An empty anchor would compare as earliest, so it is skipped.
func newSchemaCache(client *sqlx.DB, state Binlog) *schemaCache {
	tables := map[string]*tableMeta{}
	for key, meta := range state.Schemas {
		if meta != nil && meta.AnchoredAt.Name != "" && meta.AnchoredAt.Compare(state.Position) <= 0 {
			tables[key] = meta
		}
	}
	return &schemaCache{client: client, tables: tables}
}

// Single reads and writes of c.tables go through these. getTable and setTable take c.mu
// themselves. The Locked variants are for a step that reads and changes several entries as one
// change (applyDDL): the caller holds c.mu.

func (c *schemaCache) getTable(key string) (*tableMeta, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.getTableLocked(key)
}

func (c *schemaCache) setTable(key string, meta *tableMeta) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.setTableLocked(key, meta)
}

func (c *schemaCache) getTableLocked(key string) (*tableMeta, bool) {
	meta, ok := c.tables[key]
	return meta, ok
}

func (c *schemaCache) setTableLocked(key string, meta *tableMeta) {
	c.tables[key] = meta
}

func (c *schemaCache) deleteTableLocked(key string) {
	delete(c.tables, key)
}

func (c *schemaCache) resetLocked() {
	c.tables = map[string]*tableMeta{}
}

// get returns a table's cached schema, loading it on first use. tableID and columns are the
// TableMapEvent's. MySQL gives a table a new id on every ALTER, logged or not, but also on
// TRUNCATE, OPTIMIZE, FLUSH TABLES and table cache eviction, so a new id alone proves nothing.
// The cached schema is reloaded only when the id changed and the column count differs too, as
// after an unlogged ALTER (sql_log_bin=0, binlog-ignore-db).
func (c *schemaCache) get(ctx context.Context, schema, table string, tableID uint64, columns int) (*tableMeta, error) {
	key := tableKey(schema, table)

	meta, ok := c.getTable(key)

	switch {
	case ok && meta.TableID == tableID:
		return meta, nil
	case ok && len(meta.Columns) == columns:
		// A new id with the columns as cached: record it and keep the cached schema.
		next := meta.clone()
		next.TableID = tableID
		c.setTable(key, next)
		return next, nil
	case ok:
		logger.Infof("cached column metadata for %s has %d columns, binlog has %d; reloading", key, len(meta.Columns), columns)
	}

	if c.client == nil {
		return nil, fmt.Errorf("binlog metadata for %s.%s is incomplete and no schema client is configured; "+
			"set binlog_row_metadata=FULL or report this as a bug", schema, table)
	}

	meta, err := c.load(ctx, schema, table)
	if err != nil {
		return nil, err
	}

	// Read the position after load, never before: a DDL in between then lands before the
	// anchor, so a later run rejects this entry instead of reusing a schema from the future.
	meta.AnchoredAt, err = GetCurrentBinlogPosition(ctx, c.client)
	if err != nil {
		return nil, fmt.Errorf("failed to anchor column metadata for %s.%s: %w", schema, table, err)
	}

	meta.TableID = tableID

	c.setTable(key, meta)
	return meta, nil
}

// snapshot returns the cached schemas valid at upTo, for saving alongside the binlog position.
// An entry anchored later may include DDL a run resuming at upTo has yet to read, so it is left
// out. The copy is shallow: it shares *tableMeta pointers with the live cache and they are
// persisted into the state file, so tableMeta must stay immutable after insertion. applyDDL
// and get replace an entry with a modified clone() rather than editing it in place.
func (c *schemaCache) snapshot(upTo mysql.Position) map[string]*tableMeta {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]*tableMeta, len(c.tables))
	for key, meta := range c.tables {
		if meta.AnchoredAt.Compare(upTo) <= 0 {
			out[key] = meta
		}
	}
	return out
}

// maxAnchor returns the latest AnchoredAt in the cache, or the zero position when it is empty.
func (c *schemaCache) maxAnchor() mysql.Position {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var latest mysql.Position
	for _, meta := range c.tables {
		if meta.AnchoredAt.Compare(latest) > 0 {
			latest = meta.AnchoredAt
		}
	}
	return latest
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

	var tableCollation string
	if err := c.client.QueryRowContext(ctx, jdbc.MySQLCDCTableCollationQuery(), schema, table).Scan(&tableCollation); err != nil {
		return nil, fmt.Errorf("failed to load default collation for %s.%s: %w", schema, table, err)
	}
	meta.DefaultCollationID = collationIDByName(tableCollation)
	return meta, nil
}

// binaryCollationID is MySQL's `binary` collation, which TableMapEvent gives BINARY,
// VARBINARY and BLOB columns under FULL and decodeBytesToString passes through unchanged.
// Some servers report COLLATION_NAME as NULL for these, so the type is checked too.
const binaryCollationID = 63

// binaryStringTypeRe matches COLUMN_TYPE values MySQL stores with charset `binary`.
// GEOMETRY is deliberately absent: TableMapEvent does not count it as a character column
// outside MariaDB, so it carries no collation.
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

// collationIDByName resolves a collation name to the numeric ID the binlog carries under
// FULL. Returns 0 for binary/unknown, which decodeBytesToString treats as passthrough.
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

// parseEnumSetMembers extracts members from a COLUMN_TYPE such as enum('a','b') or
// set('x','y,z'), handling both quote-escaping forms MySQL emits: doubled and backslashed.
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

// leadingComments matches what may precede a DDL keyword: block comments, # and "-- " line
// comments, and the opening of a versioned comment such as /*!50700. It starts with ^, so ddlRe,
// nonTableDDLRe and tableDDLNameRe, which are all built on it, match only at the start of a query.
const leadingComments = `^\s*(?:(?:/\*.*?\*/|(?:#|--[^\S\n])[^\n]*\n|--\n)\s*)*(?:/\*!\d*\s*)?`

// ddlRe matches DDL that can change a table's column layout, as a cheap filter before
// applyDDL parses a statement. BEGIN, COMMIT, ROLLBACK and SAVEPOINT arrive as QueryEvents
// too and are skipped, as is TRUNCATE, which removes rows, not columns. Leading comments are
// skipped so annotated migrations match. CREATE stays in the list for a table recreated
// under an existing name.
var ddlRe = regexp.MustCompile(`(?is)` + leadingComments + `(alter|rename|drop|create)\s`)

func isDDL(query []byte) bool {
	return ddlRe.Match(query)
}
