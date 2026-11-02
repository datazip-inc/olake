package binlog

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// schemaCache supplies the column metadata the binlog omits. Safe for concurrent use:
// StreamMessages is single-goroutine today, but the cache outlives one event.
//
// Each entry carries AnchoredAt, the binlog position it is valid at. Entries are saved in
// Binlog.Schemas and loaded back by newSchemaCache on resume, and applyDDL advances
// them through each DDL in stream order, so they describe tables at the reader's position.
// Only a table's first load reads information_schema, the server's present, and get()
// anchors that load.
type schemaCache struct {
	mu     sync.RWMutex
	client *sqlx.DB
	tables map[string]*tableMeta
	// foldCase is lower_case_table_names 1 or 2: the server compares table names case-insensitively.
	foldCase bool
	// streamed are the keys of the tables the sync captures. A CREATE of one is read from its
	// definition even when it is not cached, as after DROP TABLE t; CREATE TABLE t.
	streamed map[string]bool
	// active is set once the cache has held an entry, and stays set when DDL later drops every one.
	active bool
	// dropped are the keys of tables a DROP TABLE removed and nothing has brought back since, so a
	// CREATE TABLE IF NOT EXISTS of one must have created it.
	dropped map[string]bool
	// schemaDefaults keeps a database's default collation from its last dropped table, for a CREATE
	// in it once no cached table carries the default.
	schemaDefaults map[string]uint64
}

// newSchemaCache starts from the previous run's saved schemas, keeping only those valid at or
// before the resume position. A later anchor may already include DDL the reader has not reached,
// so that table is reloaded instead. An empty anchor would compare as earliest, so it is skipped.
func newSchemaCache(client *sqlx.DB, state Binlog, foldCase bool) *schemaCache {
	c := &schemaCache{
		client:         client,
		tables:         map[string]*tableMeta{},
		foldCase:       foldCase,
		streamed:       map[string]bool{},
		dropped:        map[string]bool{},
		schemaDefaults: map[string]uint64{},
	}
	for key, meta := range state.Schemas {
		if meta != nil && meta.AnchoredAt.Name != "" && meta.AnchoredAt.Compare(state.Position) <= 0 {
			if foldCase {
				key = strings.ToLower(key)
			}
			c.tables[key] = meta
		}
	}
	c.active = len(c.tables) > 0
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
// themselves. The NoLock variants are for a step that reads and changes several entries as one
// change (applyDDL): the caller holds c.mu.
func (c *schemaCache) getTable(key string) (*tableMeta, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.getTableNoLock(key)
}

func (c *schemaCache) setTable(key string, meta *tableMeta) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.setTableNoLock(key, meta)
}

// idle reports whether the cache has never held an entry. With binlog_row_metadata=FULL every
// row carries its own metadata, so nothing is ever cached and applyDDL has nothing to update. A
// cache that DDL emptied stays active: after DROP TABLE t, the CREATE TABLE t that follows must
// still be applied.
func (c *schemaCache) idle() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.idleNoLock()
}

// idleNoLock is idle for a caller that holds c.mu.
func (c *schemaCache) idleNoLock() bool {
	return !c.active && len(c.tables) == 0
}

func (c *schemaCache) getTableNoLock(key string) (*tableMeta, bool) {
	meta, ok := c.tables[key]
	return meta, ok
}

func (c *schemaCache) setTableNoLock(key string, meta *tableMeta) {
	c.tables[key] = meta
	c.active = true
	delete(c.dropped, key)
}

func (c *schemaCache) deleteTableNoLock(key string) {
	delete(c.tables, key)
}

func (c *schemaCache) resetNoLock() {
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

	var schemaCollation string
	if err := c.client.QueryRowContext(ctx, jdbc.MySQLCDCSchemaCollationQuery(), schema).Scan(&schemaCollation); err != nil {
		return nil, fmt.Errorf("failed to load default collation of database %s: %w", schema, err)
	}
	meta.SchemaCollationID = collationIDByName(schemaCollation)
	return meta, nil
}

// applyDDL updates the cache for one QueryEvent, in binlog order, so each cached schema keeps
// describing its table at the reader's position. pos is the event's position and
// defaultSchema the database in use when the statement ran.
//
// It never guesses: a statement it cannot parse drops the table it alters or creates, or wipes the cache
// when that is unknown, and an ALTER it cannot apply drops just that table. Either way the
// table reloads lazily from information_schema, anchored by get(). A cache that never
// held an entry (binlog_row_metadata=FULL) returns before any regex or parse. MySQL logs each statement as its
// own QueryEvent, so a rejected query is one statement; a query that parses into several is
// applied statement by statement.
// MariaDB's /*M!...*/ wrapper, ALTER ONLINE and CREATE OR REPLACE are rewritten to the statement
// they run before parsing.
func (c *schemaCache) applyDDL(query, defaultSchema string, pos mysql.Position) {
	if c.idle() {
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

	// One lock for the whole change, from the idle check to the last write, so get() never sees
	// it half done. Everything below is a NoLock helper.
	c.mu.Lock()
	defer c.mu.Unlock()

	// Checked again under the lock, so the change applies to the cache it checked.
	if c.idleNoLock() {
		return
	}

	if nonTable {
		return
	}

	var out ddlOutcome
	if err != nil || len(stmts) == 0 {
		if name, ok := tableOfUnparsedDDL(query); ok {
			out = c.dropTablesNoLock(ev, name)
			out.reasons = append(out.reasons, "cannot parse DDL")
		} else {
			c.resetNoLock()
			out = ddlOutcome{reset: true, reasons: []string{"cannot parse DDL"}}
		}
	} else {
		for _, stmt := range stmts {
			out.merge(c.applyStmtNoLock(stmt, ev))
		}
	}
	out.log(ev.query)
}

// applyStmtNoLock applies one parsed statement to the cache and returns what it did. The caller
// holds c.mu.
func (c *schemaCache) applyStmtNoLock(stmt ast.StmtNode, ev ddlEvent) ddlOutcome {
	switch stmt := stmt.(type) {
	case *ast.AlterTableStmt:
		return c.alterTableNoLock(stmt, ev)
	case *ast.RenameTableStmt:
		return c.renameTablesNoLock(ev, stmt.TableToTables)
	case *ast.DropTableStmt:
		if !stmt.IsView && stmt.TemporaryKeyword == ast.TemporaryNone {
			out := c.dropTablesNoLock(ev, stmt.Tables...)
			for _, key := range out.dropped {
				c.dropped[key] = true
			}
			return out
		}
	case *ast.CreateTableStmt:
		return c.createTableNoLock(stmt, ev)
	case *ast.DropDatabaseStmt:
		return c.dropSchemaNoLock(stmt.Name.O)
	case *ast.AlterDatabaseStmt:
		return c.alterSchemaNoLock(stmt, ev)
	case *ast.CreateDatabaseStmt, *ast.CreateIndexStmt, *ast.DropIndexStmt, *ast.CreateViewStmt,
		*ast.TruncateTableStmt, *ast.OptimizeTableStmt, *ast.AnalyzeTableStmt, *ast.RepairTableStmt:
		// No table's columns change.
	default:
		// A statement type this file does not know: forget any cached table it names.
		return c.dropTablesNoLock(ev, referencedTables(stmt)...)
	}
	return ddlOutcome{}
}

// alterTableNoLock applies an ALTER TABLE to the cached schema of its table, if cached, and
// returns what it did. The caller holds c.mu.
func (c *schemaCache) alterTableNoLock(stmt *ast.AlterTableStmt, ev ddlEvent) ddlOutcome {
	for _, spec := range stmt.Specs {
		if spec.Tp == ast.AlterTableRenameTable {
			// The table moves to a new name: forget both, so each reloads fresh if streamed.
			return c.dropTablesNoLock(ev, stmt.Table, spec.NewTable)
		}
	}

	schema, known := tableSchema(ev.defaultSchema, stmt.Table)
	if !known {
		return c.dropTablesNoLock(ev, stmt.Table)
	}
	key := c.tableKey(schema, stmt.Table.Name.O)
	meta, cached := c.getTableNoLock(key)
	if !cached {
		return ddlOutcome{}
	}

	// An entry anchored at or after this DDL was read from information_schema later, and may
	// already include it; applying the DDL again would corrupt the entry.
	if meta.AnchoredAt.Compare(ev.pos) >= 0 {
		c.deleteTableNoLock(key)
		return ddlOutcome{dropped: []string{key}, reasons: []string{"cached metadata may already include this DDL"}}
	}

	// Copy on write: snapshot() shares *tableMeta with the saved state, so never mutate in place.
	next := meta.clone()
	next.TableID, next.ColumnTypes = 0, nil
	for _, spec := range stmt.Specs {
		if err := next.applyAlterSpec(spec); err != nil {
			c.deleteTableNoLock(key)
			return ddlOutcome{dropped: []string{key}, reasons: []string{"cannot apply: " + err.Error()}}
		}
	}
	next.AnchoredAt = ev.pos
	c.setTableNoLock(key, next)
	return ddlOutcome{applied: []string{key}}
}

// renameTablesNoLock moves each renamed table's entry to its new name, pair by pair, so a swap such
// as gh-ost's cutover (RENAME TABLE t TO _t_del, _t_gho TO t) leaves t with the shadow table's
// schema. An entry anchored at or after the RENAME was loaded later, when another table may have
// held its name, so it is dropped instead of moved. It returns what it did. The caller holds c.mu.
func (c *schemaCache) renameTablesNoLock(ev ddlEvent, pairs []*ast.TableToTable) ddlOutcome {
	var out ddlOutcome
	for _, pair := range pairs {
		oldSchema, oldKnown := tableSchema(ev.defaultSchema, pair.OldTable)
		newSchema, newKnown := tableSchema(ev.defaultSchema, pair.NewTable)
		if !oldKnown || !newKnown {
			c.resetNoLock()
			return ddlOutcome{reset: true, reasons: []string{"DDL names a table without a database"}}
		}
		oldKey := c.tableKey(oldSchema, pair.OldTable.Name.O)
		newKey := c.tableKey(newSchema, pair.NewTable.Name.O)
		meta, hadOld := c.getTableNoLock(oldKey)
		_, hadNew := c.getTableNoLock(newKey)
		c.deleteTableNoLock(oldKey)
		c.deleteTableNoLock(newKey)

		moved := hadOld && meta.AnchoredAt.Compare(ev.pos) < 0
		if moved {
			c.setTableNoLock(newKey, meta)
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

// createTableNoLock records the schema a CREATE TABLE gives a name, so DDL after it in the backlog
// applies to that schema instead of to a reload that already has the DDL. CREATE ... LIKE copies
// the source's cached schema, which is how gh-ost and pt-online-schema-change start their shadow
// tables; a CREATE of a cached or streamed name is read from its column definitions. Any other CREATE, a
// temporary table and CREATE ... SELECT load on first use, and a definition that cannot be read
// without guessing (a text column with no charset and no table default) drops the name. IF NOT
// EXISTS on a cached table changes nothing, and on an uncached one is read only right after a DROP
// of it: MySQL logs it even when the table exists. It returns what it did. The caller holds c.mu.
func (c *schemaCache) createTableNoLock(stmt *ast.CreateTableStmt, ev ddlEvent) ddlOutcome {
	if stmt.TemporaryKeyword != ast.TemporaryNone {
		return ddlOutcome{}
	}
	schema, known := tableSchema(ev.defaultSchema, stmt.Table)
	if !known {
		return c.dropTablesNoLock(ev, stmt.Table)
	}
	key := c.tableKey(schema, stmt.Table.Name.O)
	_, cached := c.getTableNoLock(key)
	if stmt.IfNotExists && cached {
		return ddlOutcome{}
	}
	// IF NOT EXISTS is logged even when the table exists, so it created the table only right after a DROP.
	isNewTableCreated := !stmt.IfNotExists || c.dropped[key]
	useColumnsFromCreate := cached || (c.streamed[key] && isNewTableCreated)

	var meta *tableMeta
	switch {
	case stmt.ReferTable != nil:
		srcSchema, srcKnown := tableSchema(ev.defaultSchema, stmt.ReferTable)
		src, srcCached := c.getTableNoLock(c.tableKey(srcSchema, stmt.ReferTable.Name.O))
		if !srcKnown || !srcCached || src.AnchoredAt.Compare(ev.pos) >= 0 {
			return c.dropTablesNoLock(ev, stmt.Table)
		}
		meta = src.clone()
	case useColumnsFromCreate && stmt.Select == nil:
		var err error
		if meta, err = tableMetaFromDef(stmt, c.schemaCollationNoLock(schema, ev.pos)); err != nil {
			out := c.dropTablesNoLock(ev, stmt.Table)
			out.reasons = append(out.reasons, "cannot read CREATE TABLE: "+err.Error())
			return out
		}
	default:
		return c.dropTablesNoLock(ev, stmt.Table)
	}
	meta.AnchoredAt, meta.TableID, meta.ColumnTypes = ev.pos, 0, nil
	c.setTableNoLock(key, meta)
	return ddlOutcome{applied: []string{key}}
}

// dropTablesNoLock forgets each named table. A name that resolves to no database wipes the cache,
// since any cached table could be the one meant. It returns what it did. The caller holds c.mu.
func (c *schemaCache) dropTablesNoLock(ev ddlEvent, names ...*ast.TableName) ddlOutcome {
	var out ddlOutcome
	for _, name := range names {
		schema, ok := tableSchema(ev.defaultSchema, name)
		if !ok {
			c.resetNoLock()
			return ddlOutcome{reset: true, reasons: []string{"DDL names a table without a database"}}
		}
		key := c.tableKey(schema, name.Name.O)
		if meta, ok := c.getTableNoLock(key); ok {
			if meta.SchemaCollationID != 0 {
				c.schemaDefaults[c.tableKey(schema, "")] = meta.SchemaCollationID
			}
			c.deleteTableNoLock(key)
			out.dropped = append(out.dropped, key)
		}
	}
	return out
}

// dropSchemaNoLock forgets every cached table in a dropped database and returns them. The caller
// holds c.mu.
func (c *schemaCache) dropSchemaNoLock(schema string) ddlOutcome {
	var out ddlOutcome
	prefix := c.tableKey(schema, "")
	delete(c.schemaDefaults, prefix)
	for key := range c.tables {
		if strings.HasPrefix(key, prefix) {
			c.deleteTableNoLock(key)
			out.dropped = append(out.dropped, key)
		}
	}
	return out
}

// alterSchemaNoLock records a database's new default charset or collation on each of its cached
// tables, so a table created in it later without a charset takes the new default. An entry
// anchored at or after the ALTER was loaded later and already has it. It returns what it did. The
// caller holds c.mu.
func (c *schemaCache) alterSchemaNoLock(stmt *ast.AlterDatabaseStmt, ev ddlEvent) ddlOutcome {
	var cs, collate string
	for _, opt := range stmt.Options {
		switch opt.Tp {
		case ast.DatabaseOptionCharset:
			cs = opt.Value
		case ast.DatabaseOptionCollate:
			collate = opt.Value
		}
	}
	if cs == "" && collate == "" {
		return ddlOutcome{}
	}
	schema := stmt.Name.O
	if stmt.AlterDefaultDatabase || schema == "" {
		schema = ev.defaultSchema
	}
	id, err := collationID(cs, collate, false, 0)
	if err != nil {
		id = 0
	}
	var out ddlOutcome
	prefix := c.tableKey(schema, "")
	if _, ok := c.schemaDefaults[prefix]; ok {
		c.schemaDefaults[prefix] = id
	}
	for key, meta := range c.tables {
		if strings.HasPrefix(key, prefix) && meta.AnchoredAt.Compare(ev.pos) < 0 {
			next := meta.clone()
			next.SchemaCollationID, next.AnchoredAt = id, ev.pos
			c.setTableNoLock(key, next)
			out.applied = append(out.applied, key)
		}
	}
	return out
}

// schemaCollationNoLock returns a database's default collation as of pos, read from a cached table
// in it that is anchored before pos, else from its last dropped table, or 0 when neither has it.
// The caller holds c.mu.
func (c *schemaCache) schemaCollationNoLock(schema string, pos mysql.Position) uint64 {
	prefix := c.tableKey(schema, "")
	for key, meta := range c.tables {
		if strings.HasPrefix(key, prefix) && meta.SchemaCollationID != 0 && meta.AnchoredAt.Compare(pos) < 0 {
			return meta.SchemaCollationID
		}
	}
	return c.schemaDefaults[prefix]
}
