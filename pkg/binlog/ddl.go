package binlog

import (
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/datazip-inc/olake/utils/logger"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	tidbmysql "github.com/pingcap/tidb/pkg/parser/mysql"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver" // decodes literal values (DEFAULT expressions and the like)
)

// setStatementRe matches MariaDB's SET STATEMENT ... FOR prefix, which wraps the statement
// that actually runs.
var setStatementRe = regexp.MustCompile(`(?is)^\s*SET\s+STATEMENT\s+.+?\s+FOR\s+`)

// nonTableDDLRe matches DDL on objects that hold no table columns: accounts, views, stored
// code, events, servers, tablespaces and sequences. The object keyword must directly follow
// the verb and any OR REPLACE / ALGORITHM / DEFINER / SQL SECURITY clauses, so a table named
// `user` or `event` is never mistaken for one.
var nonTableDDLRe = regexp.MustCompile(`(?is)` + leadingComments + `(?:create|alter|drop|rename)\s+(?:or\s+replace\s+)?` +
	`(?:(?:algorithm|definer|sql\s+security)\s*=?\s*\S+\s+)*(?:aggregate\s+)?` +
	`(?:user|role|view|trigger|procedure|function|event|server|tablespace|logfile|sequence)\b`)

// tableDDLNameRe finds the table of an ALTER TABLE or CREATE TABLE the parser rejects, such as
// one using INVISIBLE, SRID, a SPATIAL INDEX or a POINT column, so only that table is dropped.
var tableDDLNameRe = regexp.MustCompile(`(?is)` + leadingComments +
	`(?:alter\s+(?:online\s+|ignore\s+)?|create\s+(?:temporary\s+)?)table\s+(?:if\s+not\s+exists\s+)?` +
	"(?:`([^`]+)`|(\\w+))(?:\\s*\\.\\s*(?:`([^`]+)`|(\\w+)))?")

// unparsedTableDDL returns the table an unparseable ALTER TABLE or CREATE TABLE names, if it is one.
func unparsedTableDDL(query string) (*ast.TableName, bool) {
	m := tableDDLNameRe.FindStringSubmatch(query)
	if m == nil {
		return nil, false
	}
	first, second := m[1]+m[2], m[3]+m[4]
	if second == "" {
		return &ast.TableName{Name: ast.NewCIStr(first)}, true
	}
	return &ast.TableName{Schema: ast.NewCIStr(first), Name: ast.NewCIStr(second)}, true
}

// applyDDL updates the cache for one QueryEvent, in binlog order, so each cached schema keeps
// describing its table at the reader's position. pos is the event's position and
// defaultSchema the database in use when the statement ran.
//
// It never guesses: a statement it cannot parse drops the table it alters or creates, or wipes the cache
// when that is unknown, and an ALTER it cannot apply drops just that table. Either way the
// table reloads lazily from information_schema, anchored by get().
func (c *schemaCache) applyDDL(query, defaultSchema string, pos mysql.Position) {
	// With binlog_row_metadata=FULL every row carries its own metadata, so nothing is ever
	// cached and there is nothing to update.
	c.mu.RLock()
	empty := len(c.tables) == 0
	c.mu.RUnlock()
	if empty {
		return
	}

	query = setStatementRe.ReplaceAllLiteralString(query, "")
	if !isDDL([]byte(query)) {
		return
	}
	// Any DDL, a trigger or an index included, may give a table a new TableMapEvent.TableID, so
	// the next id get sees is no sign of an unlogged change.
	c.forgetTableIDs()
	if nonTableDDLRe.MatchString(query) {
		return
	}

	stmts, _, err := parser.New().ParseSQL(query)
	if err != nil || len(stmts) != 1 {
		// Only a single statement the parser rejects can be pinned to its table: several
		// statements may alter several tables.
		if name, ok := unparsedTableDDL(query); ok && err != nil {
			logger.Warnf("cannot parse DDL, dropping cached column metadata for its table: %s", query)
			c.mu.Lock()
			c.dropTables(defaultSchema, query, name)
			c.mu.Unlock()
			return
		}
		logger.Warnf("cannot parse DDL, dropping all cached column metadata: %s", query)
		c.invalidate()
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	switch stmt := stmts[0].(type) {
	case *ast.AlterTableStmt:
		c.alterTable(stmt, defaultSchema, pos, query)
	case *ast.RenameTableStmt:
		// Forget both names of every pair: after a swap such as gh-ost's cutover
		// (RENAME TABLE t TO _t_del, _t_gho TO t) the new name holds a different layout.
		for _, pair := range stmt.TableToTables {
			c.dropTables(defaultSchema, query, pair.OldTable, pair.NewTable)
		}
	case *ast.DropTableStmt:
		if !stmt.IsView {
			c.dropTables(defaultSchema, query, stmt.Tables...)
		}
	case *ast.CreateTableStmt:
		// A table created under a cached name has a new layout. IF NOT EXISTS on an existing
		// table changes nothing, and apps that bootstrap their schema run it on every start.
		if !stmt.IfNotExists {
			c.dropTables(defaultSchema, query, stmt.Table)
		}
	case *ast.DropDatabaseStmt:
		c.dropSchema(stmt.Name.O, query)
	case *ast.CreateDatabaseStmt, *ast.AlterDatabaseStmt, *ast.CreateIndexStmt, *ast.DropIndexStmt, *ast.CreateViewStmt:
		// No table's columns change.
	default:
		// A statement type this file does not know: forget any cached table it names.
		c.dropTables(defaultSchema, query, referencedTables(stmts[0])...)
	}
}

// alterTable applies an ALTER TABLE to the cached schema of its table, if cached.
// The caller holds c.mu.
func (c *schemaCache) alterTable(stmt *ast.AlterTableStmt, defaultSchema string, pos mysql.Position, query string) {
	for _, spec := range stmt.Specs {
		if spec.Tp == ast.AlterTableRenameTable {
			// The table moves to a new name: forget both, so each reloads fresh if streamed.
			c.dropTables(defaultSchema, query, stmt.Table, spec.NewTable)
			return
		}
	}

	schema, ok := tableSchema(defaultSchema, stmt.Table)
	if !ok {
		c.dropTables(defaultSchema, query, stmt.Table)
		return
	}
	key := schema + "." + stmt.Table.Name.O
	meta, ok := c.tables[key]
	if !ok {
		return
	}

	// An entry anchored at or after this DDL was read from information_schema later, and may
	// already include it; applying the DDL again would corrupt the entry.
	if meta.AnchoredAt.Compare(pos) >= 0 {
		delete(c.tables, key)
		logger.Warnf("cached column metadata for %s may already include this DDL, dropping it: %s", key, query)
		return
	}

	// Copy on write: snapshot() shares *tableMeta with the saved state, so never mutate in place.
	next := meta.clone()
	for _, spec := range stmt.Specs {
		if err := next.applyAlterSpec(spec); err != nil {
			delete(c.tables, key)
			logger.Warnf("cannot apply DDL to cached column metadata for %s (%s), dropping it: %s", key, err, query)
			return
		}
	}
	next.AnchoredAt = pos
	next.TableID = 0 // the ALTER gives the table a new id, which the next row records
	c.tables[key] = next
	logger.Infof("applied DDL to cached column metadata for %s: %s", key, query)
}

// dropTables forgets each named table. A name that resolves to no database wipes the cache,
// since any cached table could be the one meant. The caller holds c.mu.
func (c *schemaCache) dropTables(defaultSchema, query string, names ...*ast.TableName) {
	for _, name := range names {
		schema, ok := tableSchema(defaultSchema, name)
		if !ok {
			logger.Warnf("DDL names a table without a database, dropping all cached column metadata: %s", query)
			c.tables = map[string]*tableMeta{}
			return
		}
		// Table names are case-sensitive on Linux, so only an exact match is the same table.
		key := schema + "." + name.Name.O
		if _, ok := c.tables[key]; ok {
			delete(c.tables, key)
			logger.Infof("DDL dropped cached column metadata for %s: %s", key, query)
		}
	}
}

// forgetTableIDs clears every entry's TableID, so get records the next one it sees instead of
// reloading. Entries are replaced, never edited, since snapshot shares them.
func (c *schemaCache) forgetTableIDs() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for key, meta := range c.tables {
		if meta.TableID != 0 {
			next := meta.clone()
			next.TableID = 0
			c.tables[key] = next
		}
	}
}

// dropSchema forgets every cached table in a dropped database. The caller holds c.mu.
func (c *schemaCache) dropSchema(schema, query string) {
	for key := range c.tables {
		if keySchema, _, _ := strings.Cut(key, "."); keySchema == schema {
			delete(c.tables, key)
			logger.Infof("DDL dropped cached column metadata for %s: %s", key, query)
		}
	}
}

// tableSchema returns the database a table name refers to: its own, or the one in use when
// the statement ran. It reports false when neither is known.
func tableSchema(defaultSchema string, name *ast.TableName) (string, bool) {
	if name.Schema.O != "" {
		return name.Schema.O, true
	}
	return defaultSchema, defaultSchema != ""
}

// clone returns a deep copy, so changing it never reaches a *tableMeta shared with a snapshot.
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

	case ast.AlterTableDropColumn:
		i := findColumn(m.Columns, spec.OldColumnName.Name.O)
		if i == -1 {
			if spec.IfExists {
				return nil
			}
			return fmt.Errorf("unknown column %q", spec.OldColumnName.Name.O)
		}
		m.Columns = slices.Delete(m.Columns, i, i+1)

	case ast.AlterTableModifyColumn, ast.AlterTableChangeColumn:
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
			col.Name = m.Columns[i].Name // MODIFY keeps the name as stored
		}
		m.Columns = slices.Delete(m.Columns, i, i+1)
		at, err := columnIndex(m.Columns, spec.Position, i)
		if err != nil {
			return err
		}
		m.Columns = slices.Insert(m.Columns, at, col)

	case ast.AlterTableRenameColumn:
		i := findColumn(m.Columns, spec.OldColumnName.Name.O)
		if i == -1 {
			return fmt.Errorf("unknown column %q", spec.OldColumnName.Name.O)
		}
		m.Columns[i].Name = spec.NewColumnName.Name.O

	case ast.AlterTableOption:
		return m.convertCharset(spec.Options)
	}
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

// columnMetaFromDef builds one column's decoding info from its definition in a DDL statement,
// matching what columnMetaFrom derives from information_schema.
func columnMetaFromDef(def *ast.ColumnDef, tableDefault uint64) (columnMeta, error) {
	ft := def.Tp
	col := columnMeta{
		Name: def.Name.Name.O,
		// MySQL makes every ZEROFILL column UNSIGNED; the parser records only ZEROFILL.
		Unsigned: tidbmysql.HasUnsignedFlag(ft.GetFlag()) || tidbmysql.HasZerofillFlag(ft.GetFlag()),
	}

	var collate string
	for _, opt := range def.Options {
		if opt.Tp == ast.ColumnOptionCollate {
			collate = opt.StrValue
		}
	}

	switch ft.GetType() {
	case tidbmysql.TypeEnum:
		col.EnumValues = slices.Clone(ft.GetElems())
	case tidbmysql.TypeSet:
		col.SetMembers = slices.Clone(ft.GetElems())
	case tidbmysql.TypeString, tidbmysql.TypeVarchar, tidbmysql.TypeVarString,
		tidbmysql.TypeTinyBlob, tidbmysql.TypeBlob, tidbmysql.TypeMediumBlob, tidbmysql.TypeLongBlob:
		id, err := collationID(ft.GetCharset(), collate, tidbmysql.HasBinaryFlag(ft.GetFlag()), tableDefault)
		if err != nil {
			return columnMeta{}, fmt.Errorf("column %q: %w", col.Name, err)
		}
		col.CollationID = id
	}
	return col, nil
}

// collationID resolves a character column's collation as MySQL does: an explicit COLLATE
// wins, then the charset's _bin collation for the BINARY attribute, then the charset's
// default. Only the charset matters for decoding, so any collation of the right charset works.
// With no charset the table default applies; it is an error only when that is unknown.
func collationID(cs, collate string, binaryAttr bool, tableDefault uint64) (uint64, error) {
	if cs == "" && collate == "" && tableDefault != 0 {
		// No charset or collation given: the column takes the table default.
		if !binaryAttr {
			return tableDefault, nil
		}
		coll, err := charset.GetCollationByID(int(tableDefault)) //nolint:gosec // collation IDs are small
		if err != nil {
			return 0, fmt.Errorf("unknown table collation %d", tableDefault)
		}
		cs = coll.CharsetName
	}
	if strings.EqualFold(cs, charset.CharsetBin) {
		return binaryCollationID, nil
	}
	if collate == "" {
		switch {
		case cs == "":
			return 0, fmt.Errorf("no character set given and the table default is unknown")
		case binaryAttr:
			collate = cs + "_bin"
		default:
			def, err := charset.GetDefaultCollation(cs)
			if err != nil {
				return 0, fmt.Errorf("unknown character set %q", cs)
			}
			collate = def
		}
	}
	coll, err := charset.GetCollationByName(collate)
	if err != nil || coll == nil || coll.ID < 0 {
		return 0, fmt.Errorf("unknown collation %q", collate)
	}
	return uint64(coll.ID), nil
}

// columnIndex resolves an optional FIRST / AFTER clause to an insert position. at is the
// default: the end for ADD, the column's own place for MODIFY and CHANGE.
func columnIndex(cols []columnMeta, pos *ast.ColumnPosition, at int) (int, error) {
	if pos == nil {
		return at, nil
	}
	switch pos.Tp {
	case ast.ColumnPositionFirst:
		return 0, nil
	case ast.ColumnPositionAfter:
		i := findColumn(cols, pos.RelativeColumn.Name.O)
		if i == -1 {
			return 0, fmt.Errorf("unknown column %q", pos.RelativeColumn.Name.O)
		}
		return i + 1, nil
	}
	return at, nil
}

// findColumn returns the index of a column, ignoring case as MySQL does for column names,
// or -1 when there is none.
func findColumn(cols []columnMeta, name string) int {
	return slices.IndexFunc(cols, func(col columnMeta) bool {
		return strings.EqualFold(col.Name, name)
	})
}

// referencedTables collects every table a statement names, for statement types applyDDL
// does not otherwise recognize.
func referencedTables(stmt ast.Node) []*ast.TableName {
	var v tableNameCollector
	stmt.Accept(&v)
	return v.names
}

type tableNameCollector struct {
	names []*ast.TableName
}

func (v *tableNameCollector) Enter(n ast.Node) (ast.Node, bool) {
	if name, ok := n.(*ast.TableName); ok {
		v.names = append(v.names, name)
	}
	return n, false
}

func (v *tableNameCollector) Leave(n ast.Node) (ast.Node, bool) {
	return n, true
}
