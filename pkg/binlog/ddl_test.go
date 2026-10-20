package binlog

import (
	"context"
	"maps"
	"slices"
	"sync"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ddlPos is a position in a single binlog file, so positions compare by offset alone.
func ddlPos(offset uint32) mysql.Position {
	return mysql.Position{Name: "mysql-bin.000010", Pos: offset}
}

// ddlCache returns a cache holding three tables, all anchored before the DDL under test.
func ddlCache() *schemaCache {
	c := newSchemaCache(nil, Binlog{}, false)
	c.tables["shop.orders"] = &tableMeta{
		Columns: []columnMeta{
			{Name: "id", Unsigned: true},
			{Name: "status", EnumValues: []string{"new", "paid"}},
			{Name: "name", CollationID: utf8mb4GeneralCI},
		},
		AnchoredAt:         ddlPos(100),
		DefaultCollationID: utf8mb4GeneralCI,
	}
	c.tables["shop.customers"] = &tableMeta{Columns: []columnMeta{{Name: "id"}}, AnchoredAt: ddlPos(100)}
	c.tables["sales.invoices"] = &tableMeta{Columns: []columnMeta{{Name: "id"}}, AnchoredAt: ddlPos(100)}
	return c
}

func columnNames(meta *tableMeta) []string {
	names := make([]string, len(meta.Columns))
	for i, col := range meta.Columns {
		names[i] = col.Name
	}
	return names
}

// column returns the cached column with this name, failing the test when there is none, so
// assertions do not depend on a column's position in the fixture.
func column(t *testing.T, meta *tableMeta, name string) columnMeta {
	t.Helper()
	i := findColumn(meta.Columns, name)
	require.NotEqual(t, -1, i, "no column %q in %v", name, columnNames(meta))
	return meta.Columns[i]
}

func cacheKeys(c *schemaCache) []string {
	return slices.Collect(maps.Keys(c.tables))
}

// charsetOf names the character set of a collation ID; decoding depends only on the charset,
// so tests assert it rather than a particular collation.
func charsetOf(t *testing.T, id uint64) string {
	t.Helper()
	coll, err := charset.GetCollationByID(int(id)) //nolint:gosec // test IDs are small
	require.NoError(t, err)
	return coll.CharsetName
}

// Two ALTERs on one table in a backlog: each row must decode with the schema between them,
// which a reload from information_schema (the server's present) cannot give.
func TestApplyDDLFollowsEachAlterInOrder(t *testing.T) {
	c := ddlCache()

	c.applyDDL("ALTER TABLE orders MODIFY status ENUM('new','pending','paid')", "shop", ddlPos(200))
	require.Contains(t, c.tables, "shop.orders")
	assert.Equal(t, []string{"new", "pending", "paid"}, column(t, c.tables["shop.orders"], "status").EnumValues)
	assert.Equal(t, ddlPos(200), c.tables["shop.orders"].AnchoredAt)

	c.applyDDL("ALTER TABLE orders MODIFY status ENUM('new','review','pending','paid')", "shop", ddlPos(300))
	assert.Equal(t, []string{"new", "review", "pending", "paid"}, column(t, c.tables["shop.orders"], "status").EnumValues)
	assert.Equal(t, ddlPos(300), c.tables["shop.orders"].AnchoredAt)
}

// snapshot() shares *tableMeta with the saved state, so an ALTER must replace the entry
// rather than mutate it. The live entry is checked too: it took the DDL, so the saved one
// staying put is copy-on-write, not a DDL silently ignored.
func TestApplyDDLCopiesOnWrite(t *testing.T) {
	c := ddlCache()
	saved := c.snapshot(ddlPos(200))

	c.applyDDL("ALTER TABLE orders MODIFY status ENUM('new','pending','paid'), DROP COLUMN name", "shop", ddlPos(200))

	assert.Equal(t, []string{"new", "paid"}, column(t, saved["shop.orders"], "status").EnumValues)
	assert.Equal(t, []string{"id", "status", "name"}, columnNames(saved["shop.orders"]))
	assert.Equal(t, ddlPos(100), saved["shop.orders"].AnchoredAt)

	assert.Equal(t, []string{"id", "status"}, columnNames(c.tables["shop.orders"]))
	assert.Equal(t, []string{"new", "pending", "paid"}, column(t, c.tables["shop.orders"], "status").EnumValues)
}

// Each ALTER clause reaches the cached columns. Every case also re-anchors the entry at the DDL,
// the index-only one included: an ALTER of a cached table leaves the entry known to be valid
// there even when no column changes, so that AnchoredAt check is intentional.
func TestApplyDDLColumnChanges(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  []string
	}{
		{"add at end", "ALTER TABLE orders ADD COLUMN qty INT", []string{"id", "status", "name", "qty"}},
		{"add first", "ALTER TABLE orders ADD qty INT FIRST", []string{"qty", "id", "status", "name"}},
		{"add after", "ALTER TABLE orders ADD qty INT AFTER id", []string{"id", "qty", "status", "name"}},
		{"add several", "ALTER TABLE orders ADD (a INT, b INT)", []string{"id", "status", "name", "a", "b"}},
		{"drop", "ALTER TABLE orders DROP COLUMN status", []string{"id", "name"}},
		{"modify keeps place", "ALTER TABLE orders MODIFY id BIGINT UNSIGNED", []string{"id", "status", "name"}},
		{"modify moves", "ALTER TABLE orders MODIFY id BIGINT UNSIGNED AFTER name", []string{"status", "name", "id"}},
		{"modify keeps stored name", "ALTER TABLE orders MODIFY ID BIGINT", []string{"id", "status", "name"}},
		{"change renames and moves", "ALTER TABLE orders CHANGE name full_name VARCHAR(20) COLLATE utf8mb4_bin FIRST", []string{"full_name", "id", "status"}},
		{"rename column", "ALTER TABLE orders RENAME COLUMN status TO state", []string{"id", "state", "name"}},
		{"case-insensitive column names", "ALTER TABLE orders DROP COLUMN STATUS", []string{"id", "name"}},
		{"changes apply in order", "ALTER TABLE orders ADD qty INT AFTER status, DROP COLUMN status", []string{"id", "qty", "name"}},
		{"qualified name", "ALTER TABLE shop.orders DROP COLUMN name", []string{"id", "status"}},
		{"index-only alter", "ALTER TABLE orders ADD INDEX i (name)", []string{"id", "status", "name"}},
		{"add generated", "ALTER TABLE orders ADD total INT AS (id + 1) VIRTUAL", []string{"id", "status", "name", "total"}},
		{"dash comment", "-- migration 42\nALTER TABLE orders DROP COLUMN name", []string{"id", "status"}},
		{"hash comment", "# migration 42\nALTER TABLE orders DROP COLUMN name", []string{"id", "status"}},
		{"versioned comment", "/*!50700 ALTER TABLE orders DROP COLUMN name */", []string{"id", "status"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := ddlCache()
			c.applyDDL(tt.query, "shop", ddlPos(200))
			require.Contains(t, c.tables, "shop.orders")
			assert.Equal(t, tt.want, columnNames(c.tables["shop.orders"]))
			assert.Equal(t, ddlPos(200), c.tables["shop.orders"].AnchoredAt)
		})
	}
}

// Column details an added column carries: ZEROFILL implies UNSIGNED, charsets and collations,
// SET members, and BLOB as binary. TiDB's parser gives BLOB the binary charset; without it the
// column would take the table default, so that case guards the dependency.
func TestApplyDDLColumnDetails(t *testing.T) {
	c := ddlCache()
	c.applyDDL("ALTER TABLE orders "+
		"ADD z INT ZEROFILL, "+
		"ADD u BIGINT UNSIGNED, "+
		"ADD b VARBINARY(4), "+
		"ADD l VARCHAR(5) CHARACTER SET latin1, "+
		"ADD bin VARCHAR(5) CHARACTER SET utf8mb4 BINARY, "+
		"ADD c VARCHAR(5) COLLATE utf8mb4_bin, "+
		"ADD tags SET('a','b'), "+
		"ADD data BLOB", "shop", ddlPos(200))

	meta := c.tables["shop.orders"]
	require.NotNil(t, meta)
	assert.True(t, column(t, meta, "z").Unsigned, "ZEROFILL implies UNSIGNED")
	assert.True(t, column(t, meta, "u").Unsigned)
	assert.Equal(t, uint64(binaryCollationID), column(t, meta, "b").CollationID)
	assert.Equal(t, "latin1", charsetOf(t, column(t, meta, "l").CollationID))
	assert.Equal(t, mustCollationID(t, "utf8mb4_bin"), column(t, meta, "bin").CollationID, "BINARY attribute takes the _bin collation")
	assert.Equal(t, mustCollationID(t, "utf8mb4_bin"), column(t, meta, "c").CollationID)
	assert.Equal(t, []string{"a", "b"}, column(t, meta, "tags").SetMembers)
	assert.Equal(t, uint64(binaryCollationID), column(t, meta, "data").CollationID, "BLOB is binary")
}

func TestApplyDDLConvertCharset(t *testing.T) {
	c := ddlCache()
	c.tables["shop.orders"].Columns = append(c.tables["shop.orders"].Columns,
		columnMeta{Name: "raw", CollationID: binaryCollationID})

	c.applyDDL("ALTER TABLE orders CONVERT TO CHARACTER SET latin1", "shop", ddlPos(200))

	meta := c.tables["shop.orders"]
	assert.Equal(t, uint64(0), column(t, meta, "id").CollationID, "non-character columns are untouched")
	assert.Equal(t, "latin1", charsetOf(t, column(t, meta, "name").CollationID))
	assert.Equal(t, uint64(binaryCollationID), column(t, meta, "raw").CollationID, "binary columns are untouched")
}

// The other table charset options: COLLATE alone changes only the default, CONVERT TO ... COLLATE
// converts every character column to that collation, and CONVERT TO binary would change column
// types, so the table is dropped instead.
func TestApplyDDLCharsetOptions(t *testing.T) {
	tests := []struct {
		name        string
		query       string
		dropped     bool
		wantDefault uint64 // the table default after the DDL
		wantName    uint64 // the collation of the existing character column "name"
	}{
		{
			name:        "COLLATE only changes the default",
			query:       "ALTER TABLE orders COLLATE utf8mb4_bin",
			wantDefault: mustCollationID(t, "utf8mb4_bin"),
			wantName:    utf8mb4GeneralCI,
		},
		{
			name:        "CONVERT TO with COLLATE",
			query:       "ALTER TABLE orders CONVERT TO CHARACTER SET latin1 COLLATE latin1_general_ci",
			wantDefault: mustCollationID(t, "latin1_general_ci"),
			wantName:    mustCollationID(t, "latin1_general_ci"),
		},
		{
			name:    "CONVERT TO binary drops the table",
			query:   "ALTER TABLE orders CONVERT TO CHARACTER SET binary",
			dropped: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := ddlCache()
			c.applyDDL(tt.query, "shop", ddlPos(200))

			if tt.dropped {
				assert.NotContains(t, c.tables, "shop.orders")
				return
			}
			meta := c.tables["shop.orders"]
			require.NotNil(t, meta)
			assert.Equal(t, tt.wantDefault, meta.DefaultCollationID)
			assert.Equal(t, tt.wantName, column(t, meta, "name").CollationID)
		})
	}
}

func TestApplyDDLIfExists(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{"drop missing", "ALTER TABLE orders DROP COLUMN IF EXISTS missing"},
		{"modify missing", "ALTER TABLE orders MODIFY COLUMN IF EXISTS missing INT"},
		{"change missing", "ALTER TABLE orders CHANGE COLUMN IF EXISTS missing other INT"},
		{"add existing", "ALTER TABLE orders ADD COLUMN IF NOT EXISTS id INT"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := ddlCache()
			c.applyDDL(tt.query, "shop", ddlPos(200))
			require.Contains(t, c.tables, "shop.orders")
			assert.Equal(t, []string{"id", "status", "name"}, columnNames(c.tables["shop.orders"]))
		})
	}
}

// Statements that change no captured table's columns must leave the cache as it is. The test
// compares the whole map against a snapshot, not just column names, so it also fails when an
// ignored statement drops or adds an entry, re-anchors one, or clears its TableID.
func TestApplyDDLIgnores(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{"transaction", "BEGIN"},
		{"truncate", "TRUNCATE TABLE orders"},
		{"insert", "INSERT INTO orders VALUES (1)"},
		{"partition on other table", "ALTER TABLE logs DROP PARTITION p1"},
		{"alter uncached table", "ALTER TABLE logs ADD c INT"},
		{"create index", "CREATE INDEX i ON orders (id)"},
		{"drop index", "DROP INDEX i ON orders"},
		{"create database", "CREATE DATABASE other"},
		{"trigger with definer", "CREATE DEFINER=`root`@`%` TRIGGER trg BEFORE INSERT ON orders FOR EACH ROW SET NEW.id = 1"},
		{"view", "CREATE OR REPLACE VIEW v AS SELECT id FROM orders"},
		{"drop user", "DROP USER bob"},
		{"grant", "GRANT SELECT ON shop.* TO bob"},
		{"commented trigger", "-- cleanup\nDROP TRIGGER trg"},
		{"create if not exists", "CREATE TABLE IF NOT EXISTS orders (id INT)"},
		{"unparseable create of another table", "CREATE TABLE geo (id INT PRIMARY KEY, g POINT NOT NULL)"},
		{"unparseable alter of a $ name", "ALTER TABLE orders$archive WIBBLE"},
		{"unparseable alter of a non-ASCII name", "ALTER TABLE ordersé WIBBLE"},
		{"other-case alter on a case-sensitive server", "ALTER TABLE ORDERS ADD c INT"},
		{"other-case drop on a case-sensitive server", "DROP TABLE shop.CUSTOMERS"},
		{"unparseable alter of another table", "ALTER TABLE logs WIBBLE"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := ddlCache()
			before := c.snapshot(ddlPos(200))
			c.applyDDL(tt.query, "shop", ddlPos(200))
			assert.Equal(t, before, c.tables)
		})
	}
}

// A table named like an object keyword (user, event, view, ...) is still a table: nonTableDDLRe
// only matches the keyword right after the verb, so ALTER TABLE user is applied, not skipped.
func TestApplyDDLTableNamedLikeKeyword(t *testing.T) {
	c := ddlCache()
	c.tables["shop.user"] = &tableMeta{Columns: []columnMeta{{Name: "id"}}, AnchoredAt: ddlPos(100)}

	c.applyDDL("ALTER TABLE user ADD c INT", "shop", ddlPos(200))

	require.Contains(t, c.tables, "shop.user")
	assert.Equal(t, []string{"id", "c"}, columnNames(c.tables["shop.user"]))
}

// Statements that give a table a new layout, or a new name, forget only the tables named.
func TestApplyDDLDropsNamedTables(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		schema   string
		wantKeys []string
	}{
		{"drop table", "DROP TABLE customers", "shop", []string{"shop.orders", "sales.invoices"}},
		{"drop table list", "/* gh-ost */ DROP TABLE IF EXISTS shop.customers, sales.invoices", "x", []string{"shop.orders"}},
		{"drop database", "DROP DATABASE shop", "", []string{"sales.invoices"}},
		{"create table select", "CREATE TABLE customers SELECT id FROM orders", "shop", []string{"shop.orders", "sales.invoices"}},
		{"create like uncached table", "CREATE TABLE customers LIKE archive", "shop", []string{"shop.orders", "sales.invoices"}},
		{"gh-ost cutover with uncached shadow", "RENAME TABLE orders TO _orders_del, _orders_gho TO orders", "shop", []string{"shop._orders_del", "shop.customers", "sales.invoices"}},
		{"rename to cached name", "RENAME TABLE archive TO customers", "shop", []string{"shop.orders", "sales.invoices"}},
		{"alter rename", "ALTER TABLE orders RENAME TO orders_old", "shop", []string{"shop.customers", "sales.invoices"}},
		{"set statement prefix", "SET STATEMENT lock_wait_timeout=5 FOR DROP TABLE customers", "shop", []string{"shop.orders", "sales.invoices"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := ddlCache()
			c.applyDDL(tt.query, tt.schema, ddlPos(200))
			assert.ElementsMatch(t, tt.wantKeys, cacheKeys(c))
		})
	}
}

// With lower_case_table_names 1 or 2 names match in any case: an ALTER is applied to the cached
// entry, not dropped, and a DROP removes it.
func TestApplyDDLFoldCase(t *testing.T) {
	c := newSchemaCache(nil, Binlog{}, true)
	c.tables["shop.orders"] = &tableMeta{Columns: []columnMeta{{Name: "id"}}, AnchoredAt: ddlPos(100)}
	c.tables["shop.customers"] = &tableMeta{Columns: []columnMeta{{Name: "id"}}, AnchoredAt: ddlPos(100)}

	c.applyDDL("ALTER TABLE Shop.ORDERS ADD COLUMN c INT", "", ddlPos(200))
	assert.Equal(t, []string{"id", "c"}, columnNames(c.tables["shop.orders"]))

	c.applyDDL("DROP TABLE SHOP.Customers", "", ddlPos(300))
	assert.Equal(t, []string{"shop.orders"}, cacheKeys(c))
}

// MariaDB's /*M!...*/ versioned comment, ALTER ONLINE and CREATE OR REPLACE are applied as the
// statement they run, as are MySQL's /*!...*/ comments.
func TestApplyDDLMariaDBSyntax(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  []string
	}{
		{name: "mariadb versioned comment", query: "/*M!100500 ALTER TABLE orders DROP COLUMN name */", want: []string{"id", "status"}},
		{name: "mysql versioned comment", query: "/*!50700 ALTER TABLE orders DROP COLUMN name */", want: []string{"id", "status"}},
		{name: "alter online", query: "ALTER ONLINE TABLE orders DROP COLUMN name", want: []string{"id", "status"}},
		{name: "create or replace", query: "CREATE OR REPLACE TABLE orders (id INT, note VARCHAR(10) CHARACTER SET utf8mb4)", want: []string{"id", "note"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := ddlCache()
			c.applyDDL(tt.query, "shop", ddlPos(200))
			c.applyDDL("ALTER TABLE orders ADD COLUMN later INT", "shop", ddlPos(300))
			assert.Equal(t, append(tt.want, "later"), columnNames(c.tables["shop.orders"]))
		})
	}
}

// A CREATE of a cached name is read from its definition, so a later ALTER in the backlog applies to
// it. With no charset on a text column and no table default it cannot be read, so the name drops.
func TestApplyDDLCreateTable(t *testing.T) {
	c := ddlCache()
	c.applyDDL("CREATE TABLE customers (id INT UNSIGNED, kind ENUM('a','b'), note TEXT) DEFAULT CHARSET=latin1", "shop", ddlPos(200))
	got := c.tables["shop.customers"]
	require.NotNil(t, got)
	assert.Equal(t, []string{"id", "kind", "note"}, columnNames(got))
	assert.True(t, got.Columns[0].Unsigned)
	assert.Equal(t, []string{"a", "b"}, got.Columns[1].EnumValues)
	assert.Equal(t, "latin1", charsetOf(t, got.Columns[2].CollationID))
	assert.Equal(t, ddlPos(200), got.AnchoredAt)

	c.applyDDL("CREATE TABLE customers (id INT, note TEXT)", "shop", ddlPos(300))
	assert.NotContains(t, cacheKeys(c), "shop.customers")
}

// gh-ost and pt-online-schema-change copy the table with CREATE ... LIKE, alter the copy and swap
// it in with RENAME. The cache follows the copy, so a later ALTER of t applies to the new layout.
func TestApplyDDLOnlineSchemaChange(t *testing.T) {
	c := ddlCache()
	pos := uint32(200)
	for _, query := range []string{
		"CREATE TABLE _orders_gho LIKE orders",
		"ALTER TABLE _orders_gho ADD COLUMN x INT",
		"RENAME TABLE orders TO _orders_del, _orders_gho TO orders",
		"DROP TABLE _orders_del",
		"ALTER TABLE orders ADD COLUMN y INT",
	} {
		c.applyDDL(query, "shop", ddlPos(pos))
		pos++
	}
	assert.Equal(t, []string{"id", "status", "name", "x", "y"}, columnNames(c.tables["shop.orders"]))
	assert.ElementsMatch(t, []string{"shop.orders", "shop.customers", "sales.invoices"}, cacheKeys(c))
}

// An ALTER that cannot be applied without guessing drops that table alone, to reload later.
func TestApplyDDLDropsTableItCannotApply(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{"unknown column", "ALTER TABLE orders DROP COLUMN missing"},
		{"unknown AFTER column", "ALTER TABLE orders ADD c INT AFTER missing"},
		{"duplicate column", "ALTER TABLE orders ADD id INT"},
		{"convert to default", "ALTER TABLE orders CONVERT TO CHARACTER SET DEFAULT"},
		{"one bad change of many", "ALTER TABLE orders ADD a INT, DROP COLUMN missing"},
		{"unparseable alter", "ALTER TABLE orders WIBBLE"},
		{"invisible column", "ALTER TABLE orders ADD COLUMN c INT INVISIBLE"},
		{"qualified unparseable alter", "ALTER TABLE `shop`.`orders` ADD c POINT SRID 4326"},
		{"unparseable create", "CREATE TABLE orders (id INT PRIMARY KEY, g POINT NOT NULL)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := ddlCache()
			c.applyDDL(tt.query, "shop", ddlPos(200))
			assert.ElementsMatch(t, []string{"shop.customers", "sales.invoices"}, cacheKeys(c))
		})
	}
}

// A column added without a charset takes the table default, which DEFAULT CHARSET changes.
func TestApplyDDLTableDefaultCharset(t *testing.T) {
	c := ddlCache()
	c.applyDDL("ALTER TABLE orders ADD note VARCHAR(255)", "shop", ddlPos(200))
	require.Contains(t, c.tables, "shop.orders")
	assert.Equal(t, uint64(utf8mb4GeneralCI), column(t, c.tables["shop.orders"], "note").CollationID)

	c.applyDDL("ALTER TABLE orders DEFAULT CHARSET=latin1", "shop", ddlPos(300))
	c.applyDDL("ALTER TABLE orders ADD memo TEXT", "shop", ddlPos(400))
	meta := c.tables["shop.orders"]
	assert.Equal(t, "utf8mb4", charsetOf(t, column(t, meta, "name").CollationID), "existing columns keep their charset")
	assert.Equal(t, "latin1", charsetOf(t, column(t, meta, "memo").CollationID))
}

func TestCollationID(t *testing.T) {
	tests := []struct {
		name         string
		cs, collate  string
		binaryAttr   bool
		tableDefault uint64
		want         uint64
		wantErr      string
	}{
		{name: "no charset and no table default", wantErr: "table default is unknown"},
		{name: "no charset takes the table default", tableDefault: utf8mb4GeneralCI, want: utf8mb4GeneralCI},
		{name: "BINARY takes the table charset's _bin", binaryAttr: true, tableDefault: utf8mb4GeneralCI, want: mustCollationID(t, "utf8mb4_bin")},
		{name: "explicit COLLATE wins", collate: "utf8mb4_bin", tableDefault: utf8mb4GeneralCI, want: mustCollationID(t, "utf8mb4_bin")},
		{name: "unknown charset", cs: "nope", wantErr: `unknown character set "nope"`},
		{name: "unknown collation", collate: "nope_ci", wantErr: `unknown collation "nope_ci"`},
		{name: "binary short-circuits", cs: "binary", tableDefault: utf8mb4GeneralCI, want: binaryCollationID},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := collationID(tt.cs, tt.collate, tt.binaryAttr, tt.tableDefault)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// An entry loaded from information_schema after this DDL may already include it, so it is
// dropped rather than altered a second time. One anchored exactly at the DDL was read right
// after it, so >= is deliberate and that case is dropped too.
func TestApplyDDLDropsEntryNewerThanDDL(t *testing.T) {
	tests := []struct {
		name   string
		anchor mysql.Position
	}{
		{"anchored after the DDL", ddlPos(500)},
		{"anchored at the DDL", ddlPos(400)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := ddlCache()
			c.tables["shop.orders"].AnchoredAt = tt.anchor

			c.applyDDL("ALTER TABLE orders ADD c INT", "shop", ddlPos(400))

			assert.NotContains(t, c.tables, "shop.orders")
			assert.Contains(t, c.tables, "shop.customers")
		})
	}
}

// The helpers report what they did instead of logging it.
func TestDDLOutcome(t *testing.T) {
	c := ddlCache()
	stmts, _, err := parser.New().ParseSQL("ALTER TABLE orders ADD c INT; DROP TABLE customers")
	require.NoError(t, err)

	var out ddlOutcome
	for _, stmt := range stmts {
		out.merge(c.applyStmtLocked(stmt, ddlEvent{defaultSchema: "shop", pos: ddlPos(200)}))
	}
	assert.Equal(t, ddlOutcome{applied: []string{"shop.orders"}, dropped: []string{"shop.customers"}}, out)
}

// When a statement cannot be read, or names a table with no database, any cached table
// could be affected, so the whole cache is dropped.
func TestApplyDDLWipesWhenUnsure(t *testing.T) {
	tests := []struct {
		name   string
		query  string
		schema string
	}{
		{"unparseable non-alter", "CREATE WIBBLE orders", "shop"},
		{"no database", "DROP TABLE orders", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := ddlCache()
			c.applyDDL(tt.query, tt.schema, ddlPos(200))
			assert.Empty(t, c.tables)
		})
	}
}

// A query that parses into several statements is applied statement by statement.
func TestApplyDDLSeveralStatements(t *testing.T) {
	c := ddlCache()
	c.applyDDL("ALTER TABLE orders ADD a INT; ALTER TABLE customers ADD b INT", "shop", ddlPos(200))
	assert.Equal(t, []string{"id", "status", "name", "a"}, columnNames(c.tables["shop.orders"]))
	assert.Equal(t, []string{"id", "b"}, columnNames(c.tables["shop.customers"]))
}

// snapshot and maxAnchor (State and the stop check) and get (every row) can read the cache while
// applyDDL changes it. Running them together lets go test -race catch any access outside c.mu.
// get records TableID 7 on shop.customers with setTable while applyDDL replaces shop.orders: both
// paths write the cache.
func TestSchemaCacheConcurrentAccess(t *testing.T) {
	c := ddlCache()
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		for i := uint32(0); i < 200; i++ {
			c.applyDDL("ALTER TABLE orders MODIFY status ENUM('new','paid')", "shop", ddlPos(200+i))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			_ = c.snapshot(ddlPos(1000))
			_ = c.maxAnchor()
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			if _, err := c.get(context.Background(), "shop", "customers", 7, []byte{mysql.MYSQL_TYPE_LONG}); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	wg.Wait()
}
