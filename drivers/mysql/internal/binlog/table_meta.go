package binlog

import (
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
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
	// SchemaCollationID is the database default, which a table created without a charset takes.
	SchemaCollationID uint64 `json:"schema_collation_id,omitempty"`
	// TableID is the TableMapEvent.TableID last seen in this run, 0 until the table's first row and
	// after an ALTER applied to it. Not saved: MySQL reassigns ids on restart and FLUSH TABLES.
	TableID uint64 `json:"-"`
	// ColumnTypes are the TableMapEvent.ColumnType bytes recorded with TableID. Not saved either.
	ColumnTypes []byte `json:"-"`
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
