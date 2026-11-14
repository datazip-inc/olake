package binlog

import (
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/datazip-inc/olake/utils/logger"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	tidbmysql "github.com/pingcap/tidb/pkg/parser/mysql"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver" // decodes literal values (DEFAULT expressions and the like)
)

const (
	// maxLoggedQueryLen caps how much of a statement a log line carries: a big CREATE TABLE can run
	// to many kilobytes.
	maxLoggedQueryLen = 200

	// leadingComments matches what may precede a DDL keyword: block comments, # and "-- " line
	// comments, and the opening of a versioned comment such as /*!50700. It starts with ^, so ddlRe,
	// nonTableDDLRe and tableDDLNameRe, which are all built on it, match only at the start of a query.
	leadingComments = `^\s*(?:(?:/\*.*?\*/|(?:#|--[^\S\n])[^\n]*\n|--\n)\s*)*(?:/\*!\d*\s*)?`
)

var (
	// ddlRe matches DDL that can change a table's column layout, as a cheap filter before
	// applyDDL parses a statement. BEGIN, COMMIT, ROLLBACK and SAVEPOINT arrive as QueryEvents
	// too and are skipped, as is TRUNCATE, which removes rows, not columns. Leading comments are
	// skipped so annotated migrations match. CREATE stays in the list for a table recreated
	// under an existing name.
	ddlRe = regexp.MustCompile(`(?is)` + leadingComments + `(alter|rename|drop|create)\s`)

	// setStatementRe matches MariaDB's SET STATEMENT ... FOR prefix, which wraps the statement
	// that actually runs.
	setStatementRe = regexp.MustCompile(`(?is)^\s*SET\s+STATEMENT\s+.+?\s+FOR\s+`)

	// versionedCommentRe matches a statement wrapped whole in a versioned comment, MySQL's
	// /*!50700 ... */ or MariaDB's /*M!100500 ... */, which the server runs as the statement inside.
	// The parser reads MariaDB's form as a plain comment, so applyDDL unwraps both.
	versionedCommentRe = regexp.MustCompile(`(?s)^\s*/\*M?!\d*\s+((?:[^*]|\*[^/])*?)\s*\*/\s*;?\s*$`)

	// mariaDBTableKeywordRe matches MariaDB's ALTER ONLINE TABLE and CREATE OR REPLACE TABLE, which
	// the parser rejects. Without the keyword each is the statement it runs: ONLINE only asks for an
	// ALTER without locks, and CREATE OR REPLACE replaces the table the way a CREATE of a new one does.
	mariaDBTableKeywordRe = regexp.MustCompile(`(?is)(` + leadingComments + `(?:alter|create))\s+(?:online|or\s+replace)\s+((?:temporary\s+)?table\s)`)

	// nonTableDDLRe matches DDL on objects that hold no table columns: accounts, views, stored
	// code, events, servers, tablespaces and sequences. The object keyword must directly follow
	// the verb and any OR REPLACE / ALGORITHM / DEFINER / SQL SECURITY clauses, so a table named
	// `user` or `event` is never mistaken for one.
	nonTableDDLRe = regexp.MustCompile(`(?is)` + leadingComments + `(?:create|alter|drop|rename)\s+(?:or\s+replace\s+)?` +
		`(?:(?:algorithm|definer|sql\s+security)\s*=?\s*\S+\s+)*(?:aggregate\s+)?` +
		`(?:user|role|view|trigger|procedure|function|event|server|tablespace|logfile|sequence)\b`)

	// tableDDLNameRe finds the table of an ALTER TABLE or CREATE TABLE the parser rejects, such as
	// one using INVISIBLE, SRID, a SPATIAL INDEX or a POINT column, so only that table is dropped.
	// Unquoted names take any letter, digit, _ or $, as MySQL allows; a name that does not end at a
	// space, "(", ";" or the end does not match, so the caller wipes the cache instead of guessing.
	tableDDLNameRe = regexp.MustCompile(`(?is)` + leadingComments +
		`(?:alter\s+(?:online\s+|ignore\s+)?|create\s+(?:temporary\s+)?)table\s+(?:if\s+not\s+exists\s+)?` +
		"(?:`([^`]+)`|([\\pL\\pN_$]+))(?:\\s*\\.\\s*(?:`([^`]+)`|([\\pL\\pN_$]+)))?(?:[\\s(;]|$)")
)

// ddlEvent is one QueryEvent's DDL as the helpers below need it: its text for logs (shortened by
// shortQuery), the database in use when it ran (for unqualified names), and its binlog position
// (where an applied ALTER is anchored).
type ddlEvent struct {
	query         string
	defaultSchema string
	pos           mysql.Position
	gtid          mysql.GTIDSet
}

// ddlOutcome is what a DDL did to the cache. The helpers return it and applyDDL logs it once.
type ddlOutcome struct {
	applied []string // tables the DDL was applied to
	dropped []string // tables forgotten, reloaded from information_schema on next use
	reset   bool     // the whole cache was dropped
	reasons []string // why a table was dropped or the cache reset instead of applying the DDL
}

type tableNameCollector struct {
	names []*ast.TableName
}

// merge adds another statement's outcome, for a query that parses into several.
func (o *ddlOutcome) merge(other ddlOutcome) {
	o.applied = append(o.applied, other.applied...)
	o.dropped = append(o.dropped, other.dropped...)
	o.reset = o.reset || other.reset
	o.reasons = append(o.reasons, other.reasons...)
}

// log writes the outcome as one line: a warning when the whole cache was dropped, since every
// cached table then reloads.
func (o ddlOutcome) log(query string) {
	switch {
	case o.reset:
		logger.Warnf("DDL dropped all cached column metadata (%s): %s", strings.Join(o.reasons, "; "), query)
	case len(o.applied) > 0 || len(o.dropped) > 0:
		logger.Infof("DDL applied to %v, dropped %v %v: %s", o.applied, o.dropped, o.reasons, query)
	}
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

func isDDL(query string) bool {
	return ddlRe.MatchString(query)
}

// tableOfUnparsedDDL returns the table an unparseable ALTER TABLE or CREATE TABLE names, if it is one.
func tableOfUnparsedDDL(query string) (*ast.TableName, bool) {
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

// shortQuery returns query cut to maxLoggedQueryLen characters for logs, marked with "…" when cut.
func shortQuery(query string) string {
	if r := []rune(query); len(r) > maxLoggedQueryLen {
		return string(r[:maxLoggedQueryLen]) + "…"
	}
	return query
}

// tableSchema returns the database a table name refers to: its own, or the one in use when
// the statement ran. It reports false when neither is known.
func tableSchema(defaultSchema string, name *ast.TableName) (string, bool) {
	if name.Schema.O != "" {
		return name.Schema.O, true
	}
	return defaultSchema, defaultSchema != ""
}

// tableMetaFromDef builds a table's decoding info from a CREATE TABLE's column definitions and its
// DEFAULT CHARSET / COLLATE, matching what load reads from information_schema. schemaDefault is the
// database default, which the table takes when it names no charset; 0 when it is not known.
func tableMetaFromDef(stmt *ast.CreateTableStmt, schemaDefault uint64) (*tableMeta, error) {
	meta := &tableMeta{DefaultCollationID: schemaDefault, SchemaCollationID: schemaDefault}
	if err := meta.convertCharset(stmt.Options); err != nil {
		return nil, err
	}
	for _, def := range stmt.Cols {
		col, err := columnMetaFromDef(def, meta.DefaultCollationID)
		if err != nil {
			return nil, err
		}
		meta.Columns = append(meta.Columns, col)
	}
	return meta, nil
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
