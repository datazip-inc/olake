package binlog

import (
	"bytes"
	"context"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"sync"

	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
)

// binaryCollationID is MySQL's `binary` collation, which TableMapEvent gives BINARY,
// VARBINARY and BLOB columns under FULL and decodeBytesToString passes through unchanged.
// Some servers report COLLATION_NAME as NULL for these, so the type is checked too.
const binaryCollationID = 63

// binaryStringTypeRe matches COLUMN_TYPE values MySQL stores with charset `binary`.
// GEOMETRY is deliberately absent: TableMapEvent does not count it as a character column
// outside MariaDB, so it carries no collation.
var binaryStringTypeRe = regexp.MustCompile(`^(binary|varbinary|tinyblob|blob|mediumblob|longblob)\b`)

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
	// TableID is the TableMapEvent.TableID last seen in this run, 0 until the table's first row and
	// after an ALTER applied to it. Not saved: MySQL reassigns ids on restart and FLUSH TABLES.
	TableID uint64 `json:"-"`
	// ColumnTypes are the TableMapEvent.ColumnType bytes recorded with TableID. Not saved either.
	ColumnTypes []byte `json:"-"`
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
	// foldCase is lower_case_table_names 1 or 2: the server compares table names case-insensitively.
	foldCase bool
}

// newSchemaCache starts from the previous run's saved schemas, keeping only those valid at or
// before the resume position. A later anchor may already include DDL the reader has not reached,
// so that table is reloaded instead. An empty anchor would compare as earliest, so it is skipped.
func newSchemaCache(client *sqlx.DB, state Binlog, foldCase bool) *schemaCache {
	c := &schemaCache{client: client, tables: map[string]*tableMeta{}, foldCase: foldCase}
	for key, meta := range state.Schemas {
		if meta != nil && meta.AnchoredAt.Name != "" && meta.AnchoredAt.Compare(state.Position) <= 0 {
			if foldCase {
				key = strings.ToLower(key)
			}
			c.tables[key] = meta
		}
	}
	return c
}

// tableKey is the key a table's entry is cached under, "schema.table", lowercased when the server
// compares names case-insensitively, so a DDL spelling a name differently finds the entry a row
// event cached. Every lookup, store and drop builds it here.
func (c *schemaCache) tableKey(schema, table string) string {
	key := schema + "." + table
	if c.foldCase {
		return strings.ToLower(key)
	}
	return key
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

// isEmpty reports whether nothing is cached. With binlog_row_metadata=FULL every row carries its
// own metadata, so nothing is ever cached and applyDDL has nothing to update.
func (c *schemaCache) isEmpty() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.tables) == 0
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

// get returns a table's cached schema, loading it on first use. tableID and columnTypes are the
// TableMapEvent's. MySQL gives a table a new id on every ALTER, logged or not, but also on
// TRUNCATE, OPTIMIZE, FLUSH TABLES and table cache eviction, so a new id alone proves nothing.
// The cached schema is reloaded only when the id changed and the column types differ from those
// recorded with the old id, as after an unlogged ALTER (sql_log_bin=0, binlog-ignore-db). A swap
// of two columns of the same type changes no type, so it is not detected.
func (c *schemaCache) get(ctx context.Context, schema, table string, tableID uint64, columnTypes []byte) (*tableMeta, error) {
	key := c.tableKey(schema, table)

	meta, ok := c.getTable(key)

	switch {
	case ok && meta.TableID == tableID:
		return meta, nil
	case ok && len(meta.Columns) == len(columnTypes) && (meta.ColumnTypes == nil || bytes.Equal(meta.ColumnTypes, columnTypes)):
		next := meta.clone()
		next.TableID, next.ColumnTypes = tableID, slices.Clone(columnTypes)
		c.setTable(key, next)
		return next, nil
	case ok:
		logger.Infof("column layout of %s changed with no DDL seen, reloading column metadata", key)
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

	meta.TableID, meta.ColumnTypes = tableID, slices.Clone(columnTypes)

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

// applyDDL updates the cache for one QueryEvent, in binlog order, so each cached schema keeps
// describing its table at the reader's position. pos is the event's position and
// defaultSchema the database in use when the statement ran.
//
// It never guesses: a statement it cannot parse drops the table it alters or creates, or wipes the cache
// when that is unknown, and an ALTER it cannot apply drops just that table. Either way the
// table reloads lazily from information_schema, anchored by get(). An empty cache
// (binlog_row_metadata=FULL) returns before any regex or parse. MySQL logs each statement as its
// own QueryEvent, so a rejected query is one statement; a query that parses into several is
// applied statement by statement.
// MariaDB's /*M!...*/ wrapper, ALTER ONLINE and CREATE OR REPLACE are rewritten to the statement
// they run before parsing.
func (c *schemaCache) applyDDL(query, defaultSchema string, pos mysql.Position) {
	if c.isEmpty() {
		return
	}
	query = setStatementRe.ReplaceAllLiteralString(query, "")
	query = versionedCommentRe.ReplaceAllString(query, "${1}")
	query = mariaDBTableKeywordRe.ReplaceAllString(query, "${1} ${2}")
	if !isDDL(query) {
		return
	}

	ev := ddlEvent{query: shortQuery(query), defaultSchema: defaultSchema, pos: pos}

	// Parse before taking c.mu: it reads only the query. DDL on objects with no columns (triggers,
	// views, users, ...) needs no parse.
	nonTable := nonTableDDLRe.MatchString(query)
	var stmts []ast.StmtNode
	var err error
	if !nonTable {
		stmts, _, err = parser.New().ParseSQL(query)
	}

	// One lock for the whole change, from the empty check to the last write, so get() never sees
	// it half done. Everything below is a Locked helper.
	c.mu.Lock()
	defer c.mu.Unlock()

	// Checked again under the lock, so the change applies to the cache it checked.
	if len(c.tables) == 0 {
		return
	}

	if nonTable {
		return
	}

	var out ddlOutcome
	if err != nil || len(stmts) == 0 {
		if name, ok := tableOfUnparsedDDL(query); ok {
			out = c.dropTablesLocked(ev, name)
			out.reasons = append(out.reasons, "cannot parse DDL")
		} else {
			c.resetLocked()
			out = ddlOutcome{reset: true, reasons: []string{"cannot parse DDL"}}
		}
	} else {
		for _, stmt := range stmts {
			out.merge(c.applyStmtLocked(stmt, ev))
		}
	}
	out.log(ev.query)
}

// applyStmtLocked applies one parsed statement to the cache and returns what it did. The caller
// holds c.mu.
func (c *schemaCache) applyStmtLocked(stmt ast.StmtNode, ev ddlEvent) ddlOutcome {
	switch stmt := stmt.(type) {
	case *ast.AlterTableStmt:
		return c.alterTableLocked(stmt, ev)
	case *ast.RenameTableStmt:
		return c.renameTablesLocked(ev, stmt.TableToTables)
	case *ast.DropTableStmt:
		if !stmt.IsView {
			return c.dropTablesLocked(ev, stmt.Tables...)
		}
	case *ast.CreateTableStmt:
		return c.createTableLocked(stmt, ev)
	case *ast.DropDatabaseStmt:
		return c.dropSchemaLocked(stmt.Name.O)
	case *ast.CreateDatabaseStmt, *ast.AlterDatabaseStmt, *ast.CreateIndexStmt, *ast.DropIndexStmt, *ast.CreateViewStmt,
		*ast.TruncateTableStmt, *ast.OptimizeTableStmt, *ast.AnalyzeTableStmt, *ast.RepairTableStmt:
		// No table's columns change.
	default:
		// A statement type this file does not know: forget any cached table it names.
		return c.dropTablesLocked(ev, referencedTables(stmt)...)
	}
	return ddlOutcome{}
}

// alterTableLocked applies an ALTER TABLE to the cached schema of its table, if cached, and
// returns what it did. The caller holds c.mu.
func (c *schemaCache) alterTableLocked(stmt *ast.AlterTableStmt, ev ddlEvent) ddlOutcome {
	for _, spec := range stmt.Specs {
		if spec.Tp == ast.AlterTableRenameTable {
			// The table moves to a new name: forget both, so each reloads fresh if streamed.
			return c.dropTablesLocked(ev, stmt.Table, spec.NewTable)
		}
	}

	schema, known := tableSchema(ev.defaultSchema, stmt.Table)
	if !known {
		return c.dropTablesLocked(ev, stmt.Table)
	}
	key := c.tableKey(schema, stmt.Table.Name.O)
	meta, cached := c.getTableLocked(key)
	if !cached {
		return ddlOutcome{}
	}

	// An entry anchored at or after this DDL was read from information_schema later, and may
	// already include it; applying the DDL again would corrupt the entry.
	if meta.AnchoredAt.Compare(ev.pos) >= 0 {
		c.deleteTableLocked(key)
		return ddlOutcome{dropped: []string{key}, reasons: []string{"cached metadata may already include this DDL"}}
	}

	// Copy on write: snapshot() shares *tableMeta with the saved state, so never mutate in place.
	next := meta.clone()
	next.TableID, next.ColumnTypes = 0, nil
	for _, spec := range stmt.Specs {
		if err := next.applyAlterSpec(spec); err != nil {
			c.deleteTableLocked(key)
			return ddlOutcome{dropped: []string{key}, reasons: []string{"cannot apply: " + err.Error()}}
		}
	}
	next.AnchoredAt = ev.pos
	c.setTableLocked(key, next)
	return ddlOutcome{applied: []string{key}}
}

// renameTablesLocked moves each renamed table's entry to its new name, pair by pair, so a swap such
// as gh-ost's cutover (RENAME TABLE t TO _t_del, _t_gho TO t) leaves t with the shadow table's
// schema. An entry anchored at or after the RENAME was loaded later, when another table may have
// held its name, so it is dropped instead of moved. It returns what it did. The caller holds c.mu.
func (c *schemaCache) renameTablesLocked(ev ddlEvent, pairs []*ast.TableToTable) ddlOutcome {
	var out ddlOutcome
	for _, pair := range pairs {
		oldSchema, oldKnown := tableSchema(ev.defaultSchema, pair.OldTable)
		newSchema, newKnown := tableSchema(ev.defaultSchema, pair.NewTable)
		if !oldKnown || !newKnown {
			c.resetLocked()
			return ddlOutcome{reset: true, reasons: []string{"DDL names a table without a database"}}
		}
		oldKey := c.tableKey(oldSchema, pair.OldTable.Name.O)
		newKey := c.tableKey(newSchema, pair.NewTable.Name.O)
		meta, hadOld := c.getTableLocked(oldKey)
		_, hadNew := c.getTableLocked(newKey)
		c.deleteTableLocked(oldKey)
		c.deleteTableLocked(newKey)

		moved := hadOld && meta.AnchoredAt.Compare(ev.pos) < 0
		if moved {
			c.setTableLocked(newKey, meta)
			out.applied = append(out.applied, newKey)
		}
		if hadOld && !moved {
			out.dropped = append(out.dropped, oldKey)
		}
		if hadNew && !moved {
			out.dropped = append(out.dropped, newKey)
		}
	}
	return out
}

// createTableLocked records the schema a CREATE TABLE gives a name, so DDL after it in the backlog
// applies to that schema instead of to a reload that already has the DDL. CREATE ... LIKE copies
// the source's cached schema, which is how gh-ost and pt-online-schema-change start their shadow
// tables; a CREATE of a cached name is read from its column definitions. Any other CREATE, a
// temporary table and CREATE ... SELECT load on first use, and a definition that cannot be read
// without guessing (a text column with no charset and no table default) drops the name. IF NOT
// EXISTS on a cached table changes nothing. It returns what it did. The caller holds c.mu.
func (c *schemaCache) createTableLocked(stmt *ast.CreateTableStmt, ev ddlEvent) ddlOutcome {
	if stmt.TemporaryKeyword != ast.TemporaryNone {
		return ddlOutcome{}
	}
	schema, known := tableSchema(ev.defaultSchema, stmt.Table)
	if !known {
		return c.dropTablesLocked(ev, stmt.Table)
	}
	key := c.tableKey(schema, stmt.Table.Name.O)
	_, cached := c.getTableLocked(key)
	if stmt.IfNotExists && cached {
		return ddlOutcome{}
	}

	var meta *tableMeta
	switch {
	case stmt.ReferTable != nil:
		srcSchema, srcKnown := tableSchema(ev.defaultSchema, stmt.ReferTable)
		src, srcCached := c.getTableLocked(c.tableKey(srcSchema, stmt.ReferTable.Name.O))
		if !srcKnown || !srcCached || src.AnchoredAt.Compare(ev.pos) >= 0 {
			return c.dropTablesLocked(ev, stmt.Table)
		}
		meta = src.clone()
	case cached && stmt.Select == nil:
		var err error
		if meta, err = tableMetaFromDef(stmt); err != nil {
			out := c.dropTablesLocked(ev, stmt.Table)
			out.reasons = append(out.reasons, "cannot read CREATE TABLE: "+err.Error())
			return out
		}
	default:
		return c.dropTablesLocked(ev, stmt.Table)
	}
	meta.AnchoredAt, meta.TableID, meta.ColumnTypes = ev.pos, 0, nil
	c.setTableLocked(key, meta)
	return ddlOutcome{applied: []string{key}}
}

// dropTablesLocked forgets each named table. A name that resolves to no database wipes the cache,
// since any cached table could be the one meant. It returns what it did. The caller holds c.mu.
func (c *schemaCache) dropTablesLocked(ev ddlEvent, names ...*ast.TableName) ddlOutcome {
	var out ddlOutcome
	for _, name := range names {
		schema, ok := tableSchema(ev.defaultSchema, name)
		if !ok {
			c.resetLocked()
			return ddlOutcome{reset: true, reasons: []string{"DDL names a table without a database"}}
		}
		key := c.tableKey(schema, name.Name.O)
		if _, ok := c.getTableLocked(key); ok {
			c.deleteTableLocked(key)
			out.dropped = append(out.dropped, key)
		}
	}
	return out
}

// dropSchemaLocked forgets every cached table in a dropped database and returns them. The caller
// holds c.mu.
func (c *schemaCache) dropSchemaLocked(schema string) ddlOutcome {
	var out ddlOutcome
	prefix := c.tableKey(schema, "")
	for key := range c.tables {
		if strings.HasPrefix(key, prefix) {
			c.deleteTableLocked(key)
			out.dropped = append(out.dropped, key)
		}
	}
	return out
}

// clone returns a deep copy, so changing it never reaches a *tableMeta shared with a snapshot:
// callers set columns, AnchoredAt, TableID and ColumnTypes on the copy, while the saved state keeps
// the old one.
// next := *m copies every field (new ones included, so none is forgotten), then Columns is
// replaced with the deep copy built above; returning m itself would edit the shared entry.
func (m *tableMeta) clone() *tableMeta {
	cols := make([]columnMeta, len(m.Columns))
	for i, col := range m.Columns {
		col.EnumValues = slices.Clone(col.EnumValues)
		col.SetMembers = slices.Clone(col.SetMembers)
		cols[i] = col
	}
	next := *m
	next.Columns = cols
	return &next
}

// applyAlterSpec applies one ALTER TABLE clause. Clauses that do not change columns
// (indexes, constraints, partitions, most table options, column defaults) are no-ops.
func (m *tableMeta) applyAlterSpec(spec *ast.AlterTableSpec) error {
	switch spec.Tp {
	case ast.AlterTableAddColumns:
		return m.addColumns(spec)
	case ast.AlterTableDropColumn:
		return m.dropColumn(spec)
	case ast.AlterTableModifyColumn, ast.AlterTableChangeColumn:
		return m.replaceColumn(spec)
	case ast.AlterTableRenameColumn:
		return m.renameColumn(spec)
	case ast.AlterTableOption:
		return m.convertCharset(spec.Options)
	}
	return nil
}

// addColumns applies ADD COLUMN, at the end or at its FIRST / AFTER position. IF NOT EXISTS
// skips a column that is already there.
func (m *tableMeta) addColumns(spec *ast.AlterTableSpec) error {
	at, err := columnIndex(m.Columns, spec.Position, len(m.Columns))
	if err != nil {
		return err
	}
	var added []columnMeta
	for _, def := range spec.NewColumns {
		if findColumn(m.Columns, def.Name.Name.O) != -1 {
			if spec.IfNotExists {
				continue
			}
			return fmt.Errorf("column %q already exists", def.Name.Name.O)
		}
		col, err := columnMetaFromDef(def, m.DefaultCollationID)
		if err != nil {
			return err
		}
		added = append(added, col)
	}
	m.Columns = slices.Insert(m.Columns, at, added...)
	return nil
}

// dropColumn applies DROP COLUMN. IF EXISTS makes a missing column a no-op.
func (m *tableMeta) dropColumn(spec *ast.AlterTableSpec) error {
	i := findColumn(m.Columns, spec.OldColumnName.Name.O)
	if i == -1 {
		if spec.IfExists {
			return nil
		}
		return fmt.Errorf("unknown column %q", spec.OldColumnName.Name.O)
	}
	m.Columns = slices.Delete(m.Columns, i, i+1)
	return nil
}

// replaceColumn applies MODIFY and CHANGE: the column takes its new definition, and its FIRST /
// AFTER position when given. MODIFY keeps the name as stored; CHANGE renames it.
func (m *tableMeta) replaceColumn(spec *ast.AlterTableSpec) error {
	def := spec.NewColumns[0]
	old := def.Name
	if spec.Tp == ast.AlterTableChangeColumn {
		old = spec.OldColumnName
	}
	i := findColumn(m.Columns, old.Name.O)
	if i == -1 {
		if spec.IfExists {
			return nil
		}
		return fmt.Errorf("unknown column %q", old.Name.O)
	}
	col, err := columnMetaFromDef(def, m.DefaultCollationID)
	if err != nil {
		return err
	}
	if spec.Tp == ast.AlterTableModifyColumn {
		col.Name = m.Columns[i].Name
	}
	m.Columns = slices.Delete(m.Columns, i, i+1)
	if findColumn(m.Columns, col.Name) != -1 {
		return fmt.Errorf("column %q already exists", col.Name)
	}
	at, err := columnIndex(m.Columns, spec.Position, i)
	if err != nil {
		return err
	}
	m.Columns = slices.Insert(m.Columns, at, col)
	return nil
}

// renameColumn applies RENAME COLUMN. Renaming onto another existing column is an error.
func (m *tableMeta) renameColumn(spec *ast.AlterTableSpec) error {
	i := findColumn(m.Columns, spec.OldColumnName.Name.O)
	if i == -1 {
		return fmt.Errorf("unknown column %q", spec.OldColumnName.Name.O)
	}
	if j := findColumn(m.Columns, spec.NewColumnName.Name.O); j != -1 && j != i {
		return fmt.Errorf("column %q already exists", spec.NewColumnName.Name.O)
	}
	m.Columns[i].Name = spec.NewColumnName.Name.O
	return nil
}

// convertCharset applies a table's charset options. CONVERT TO re-encodes every character
// column and sets the table default; DEFAULT CHARSET and COLLATE change only the default,
// which columns added later take.
func (m *tableMeta) convertCharset(opts []*ast.TableOption) error {
	var cs, collate string
	var convert, change bool
	for _, opt := range opts {
		switch opt.Tp {
		case ast.TableOptionCharset:
			if opt.Default {
				return fmt.Errorf("CHARACTER SET DEFAULT needs the database default, which is not cached")
			}
			change, cs = true, opt.StrValue
			convert = convert || opt.UintValue == ast.TableOptionCharsetWithConvertTo
		case ast.TableOptionCollate:
			change, collate = true, opt.StrValue
		}
	}
	if !change {
		return nil
	}

	id, err := collationID(cs, collate, false, 0)
	if err != nil {
		return err
	}
	if convert && id == binaryCollationID {
		return fmt.Errorf("CONVERT TO CHARACTER SET binary changes column types, which is not supported")
	}
	m.DefaultCollationID = id
	if convert {
		for i := range m.Columns {
			if c := m.Columns[i].CollationID; c != 0 && c != binaryCollationID {
				m.Columns[i].CollationID = id
			}
		}
	}
	return nil
}

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
