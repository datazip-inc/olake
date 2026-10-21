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

// TestSchemaCacheApplyDDL covers applyDDL, one subtest per behavior; each subtest's doc says what it checks.
func TestSchemaCacheApplyDDL(t *testing.T) {
	t.Run("follows each alter in order", testApplyDDLFollowsEachAlterInOrder)
	t.Run("copies on write", testApplyDDLCopiesOnWrite)
	t.Run("column changes", testApplyDDLColumnChanges)
	t.Run("column details", testApplyDDLColumnDetails)
	t.Run("convert charset", testApplyDDLConvertCharset)
	t.Run("charset options", testApplyDDLCharsetOptions)
	t.Run("if exists", testApplyDDLIfExists)
	t.Run("ignores", testApplyDDLIgnores)
	t.Run("table named like keyword", testApplyDDLTableNamedLikeKeyword)
	t.Run("drops named tables", testApplyDDLDropsNamedTables)
	t.Run("fold case", testApplyDDLFoldCase)
	t.Run("mariadb syntax", testApplyDDLMariaDBSyntax)
	t.Run("create table", testApplyDDLCreateTable)
	t.Run("online schema change", testApplyDDLOnlineSchemaChange)
	t.Run("drops table it cannot apply", testApplyDDLDropsTableItCannotApply)
	t.Run("table default charset", testApplyDDLTableDefaultCharset)
	t.Run("drops entry newer than ddl", testApplyDDLDropsEntryNewerThanDDL)
	t.Run("wipes when unsure", testApplyDDLWipesWhenUnsure)
	t.Run("several statements", testApplyDDLSeveralStatements)
	t.Run("recreates the only cached table", testApplyDDLRecreatesOnlyCachedTable)
	t.Run("create if not exists of an uncached table", testApplyDDLCreateIfNotExistsUncached)
	t.Run("keeps the database default after the last drop", testApplyDDLKeepsSchemaDefault)
	t.Run("reports its outcome", testDDLOutcome)
	t.Run("GTID checkpoints apply DDL across physical positions", testApplyDDLAfterGTIDCheckpoint)
	t.Run("GTID loads retain their anchor after DDL", testApplyDDLPreservesGTIDAnchor)
}

func testApplyDDLAfterGTIDCheckpoint(t *testing.T) {
	saved := testSID + ":1-2"
	transaction, err := parseGTIDSet(testSID + ":3")
	require.NoError(t, err)
	for _, tt := range []struct {
		name string
		pos  mysql.Position
	}{
		{"promoted server", mysql.Position{Name: "mysql-bin.000001", Pos: 200}},
		{"reordered transaction", ddlPos(50)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			schemas := ddlCache().tables
			c := newSchemaCache(nil, Binlog{Position: ddlPos(500), GTIDSet: &saved, Schemas: schemas}, false)
			c.applyDDL("ALTER TABLE orders ADD c INT", "shop", tt.pos, transaction)
			require.Contains(t, c.tables, "shop.orders")
			assert.Equal(t, []string{"id", "status", "name", "c"}, columnNames(c.tables["shop.orders"]))
			assert.Equal(t, []string{"id", "status", "name"}, columnNames(schemas["shop.orders"]))
		})
	}
}

func testApplyDDLPreservesGTIDAnchor(t *testing.T) {
	anchor, err := parseGTIDSet(testSID + ":1-2:4")
	require.NoError(t, err)
	transaction, err := parseGTIDSet(testSID + ":3")
	require.NoError(t, err)
	processed, err := parseGTIDSet(testSID + ":1-3")
	require.NoError(t, err)
	for _, query := range []string{
		"ALTER TABLE orders ADD c INT",
		"CREATE TABLE copied LIKE orders",
		"DROP TABLE orders; CREATE TABLE orders (id INT, note TEXT)",
	} {
		t.Run(query, func(t *testing.T) {
			c := ddlCache()
			c.gtidMode = true
			c.streamed["shop.orders"] = true
			c.tables = map[string]*tableMeta{"shop.orders": c.tables["shop.orders"]}
			c.tables["shop.orders"].SchemaCollationID = latin1SwedishCI
			c.tables["shop.orders"].gtidAnchor = anchor
			c.applyDDL(query, "shop", ddlPos(900), transaction)
			require.NotEmpty(t, c.tables)
			// Applying an included DDL cannot prove the excluded transaction seen by the SQL load.
			assert.Empty(t, c.snapshot(ddlPos(1000), processed))
		})
	}
	t.Run("DDL already present in SQL metadata is not applied twice", func(t *testing.T) {
		c := ddlCache()
		c.gtidMode = true
		c.tables["shop.orders"].gtidAnchor = transaction
		c.applyDDL("ALTER TABLE orders ADD c INT", "shop", ddlPos(200), transaction)
		assert.NotContains(t, c.tables, "shop.orders")
	})
}

// Two ALTERs on one table in a backlog: each row must decode with the schema between them,
// which a reload from information_schema (the server's present) cannot give.
func testApplyDDLFollowsEachAlterInOrder(t *testing.T) {
	c := ddlCache()

	c.applyDDL("ALTER TABLE orders MODIFY status ENUM('new','pending','paid')", "shop", ddlPos(200), nil)
	require.Contains(t, c.tables, "shop.orders")
	assert.Equal(t, []string{"new", "pending", "paid"}, column(t, c.tables["shop.orders"], "status").EnumValues)
	assert.Equal(t, ddlPos(200), c.tables["shop.orders"].AnchoredAt)

	c.applyDDL("ALTER TABLE orders MODIFY status ENUM('new','review','pending','paid')", "shop", ddlPos(300), nil)
	assert.Equal(t, []string{"new", "review", "pending", "paid"}, column(t, c.tables["shop.orders"], "status").EnumValues)
	assert.Equal(t, ddlPos(300), c.tables["shop.orders"].AnchoredAt)
}

// snapshot() shares *tableMeta with the saved state, so an ALTER must replace the entry
// rather than mutate it. The live entry is checked too: it took the DDL, so the saved one
// staying put is copy-on-write, not a DDL silently ignored.
func testApplyDDLCopiesOnWrite(t *testing.T) {
	c := ddlCache()
	saved := c.snapshot(ddlPos(200), nil)

	c.applyDDL("ALTER TABLE orders MODIFY status ENUM('new','pending','paid'), DROP COLUMN name", "shop", ddlPos(200), nil)

	assert.Equal(t, []string{"new", "paid"}, column(t, saved["shop.orders"], "status").EnumValues)
	assert.Equal(t, []string{"id", "status", "name"}, columnNames(saved["shop.orders"]))
	assert.Equal(t, ddlPos(100), saved["shop.orders"].AnchoredAt)

	assert.Equal(t, []string{"id", "status"}, columnNames(c.tables["shop.orders"]))
	assert.Equal(t, []string{"new", "pending", "paid"}, column(t, c.tables["shop.orders"], "status").EnumValues)
}

// Each ALTER clause reaches the cached columns. Every case also re-anchors the entry at the DDL,
// the index-only one included: an ALTER of a cached table leaves the entry known to be valid
// there even when no column changes, so that AnchoredAt check is intentional.
func testApplyDDLColumnChanges(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  []string
	}{
		{name: "add at end", query: "ALTER TABLE orders ADD COLUMN qty INT", want: []string{"id", "status", "name", "qty"}},
		{name: "add first", query: "ALTER TABLE orders ADD qty INT FIRST", want: []string{"qty", "id", "status", "name"}},
		{name: "add after", query: "ALTER TABLE orders ADD qty INT AFTER id", want: []string{"id", "qty", "status", "name"}},
		{name: "add several", query: "ALTER TABLE orders ADD (a INT, b INT)", want: []string{"id", "status", "name", "a", "b"}},
		{name: "drop", query: "ALTER TABLE orders DROP COLUMN status", want: []string{"id", "name"}},
		{name: "modify keeps place", query: "ALTER TABLE orders MODIFY id BIGINT UNSIGNED", want: []string{"id", "status", "name"}},
		{name: "modify moves", query: "ALTER TABLE orders MODIFY id BIGINT UNSIGNED AFTER name", want: []string{"status", "name", "id"}},
		{name: "modify keeps stored name", query: "ALTER TABLE orders MODIFY ID BIGINT", want: []string{"id", "status", "name"}},
		{name: "change renames and moves", query: "ALTER TABLE orders CHANGE name full_name VARCHAR(20) COLLATE utf8mb4_bin FIRST", want: []string{"full_name", "id", "status"}},
		{name: "rename column", query: "ALTER TABLE orders RENAME COLUMN status TO state", want: []string{"id", "state", "name"}},
		{name: "case-insensitive column names", query: "ALTER TABLE orders DROP COLUMN STATUS", want: []string{"id", "name"}},
		{name: "changes apply in order", query: "ALTER TABLE orders ADD qty INT AFTER status, DROP COLUMN status", want: []string{"id", "qty", "name"}},
		{name: "qualified name", query: "ALTER TABLE shop.orders DROP COLUMN name", want: []string{"id", "status"}},
		{name: "index-only alter", query: "ALTER TABLE orders ADD INDEX i (name)", want: []string{"id", "status", "name"}},
		{name: "add generated", query: "ALTER TABLE orders ADD total INT AS (id + 1) VIRTUAL", want: []string{"id", "status", "name", "total"}},
		{name: "dash comment", query: "-- migration 42\nALTER TABLE orders DROP COLUMN name", want: []string{"id", "status"}},
		{name: "hash comment", query: "# migration 42\nALTER TABLE orders DROP COLUMN name", want: []string{"id", "status"}},
		{name: "versioned comment", query: "/*!50700 ALTER TABLE orders DROP COLUMN name */", want: []string{"id", "status"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := ddlCache()
			c.applyDDL(tt.query, "shop", ddlPos(200), nil)
			require.Contains(t, c.tables, "shop.orders")
			assert.Equal(t, tt.want, columnNames(c.tables["shop.orders"]))
			assert.Equal(t, ddlPos(200), c.tables["shop.orders"].AnchoredAt)
		})
	}
}

// Column details an added column carries: ZEROFILL implies UNSIGNED, charsets and collations,
// SET members, and BLOB as binary. TiDB's parser gives BLOB the binary charset; without it the
// column would take the table default, so that case guards the dependency.
func testApplyDDLColumnDetails(t *testing.T) {
	c := ddlCache()
	c.applyDDL("ALTER TABLE orders "+
		"ADD z INT ZEROFILL, "+
		"ADD u BIGINT UNSIGNED, "+
		"ADD b VARBINARY(4), "+
		"ADD l VARCHAR(5) CHARACTER SET latin1, "+
		"ADD bin VARCHAR(5) CHARACTER SET utf8mb4 BINARY, "+
		"ADD c VARCHAR(5) COLLATE utf8mb4_bin, "+
		"ADD tags SET('a','b'), "+
		"ADD data BLOB", "shop", ddlPos(200), nil)

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

func testApplyDDLConvertCharset(t *testing.T) {
	c := ddlCache()
	c.tables["shop.orders"].Columns = append(c.tables["shop.orders"].Columns,
		columnMeta{Name: "raw", CollationID: binaryCollationID})

	c.applyDDL("ALTER TABLE orders CONVERT TO CHARACTER SET latin1", "shop", ddlPos(200), nil)

	meta := c.tables["shop.orders"]
	assert.Equal(t, uint64(0), column(t, meta, "id").CollationID, "non-character columns are untouched")
	assert.Equal(t, "latin1", charsetOf(t, column(t, meta, "name").CollationID))
	assert.Equal(t, uint64(binaryCollationID), column(t, meta, "raw").CollationID, "binary columns are untouched")
}

// The other table charset options: COLLATE alone changes only the default, CONVERT TO ... COLLATE
// converts every character column to that collation, and CONVERT TO binary would change column
// types, so the table is dropped instead.
func testApplyDDLCharsetOptions(t *testing.T) {
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
			c.applyDDL(tt.query, "shop", ddlPos(200), nil)

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

func testApplyDDLIfExists(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{name: "drop missing", query: "ALTER TABLE orders DROP COLUMN IF EXISTS missing"},
		{name: "modify missing", query: "ALTER TABLE orders MODIFY COLUMN IF EXISTS missing INT"},
		{name: "change missing", query: "ALTER TABLE orders CHANGE COLUMN IF EXISTS missing other INT"},
		{name: "add existing", query: "ALTER TABLE orders ADD COLUMN IF NOT EXISTS id INT"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := ddlCache()
			c.applyDDL(tt.query, "shop", ddlPos(200), nil)
			require.Contains(t, c.tables, "shop.orders")
			assert.Equal(t, []string{"id", "status", "name"}, columnNames(c.tables["shop.orders"]))
		})
	}
}

// Statements that change no captured table's columns must leave the cache as it is. The test
// compares the whole map against a snapshot, not just column names, so it also fails when an
// ignored statement drops or adds an entry, re-anchors one, or clears its TableID.
func testApplyDDLIgnores(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{name: "transaction", query: "BEGIN"},
		{name: "truncate", query: "TRUNCATE TABLE orders"},
		{name: "insert", query: "INSERT INTO orders VALUES (1)"},
		{name: "partition on other table", query: "ALTER TABLE logs DROP PARTITION p1"},
		{name: "alter uncached table", query: "ALTER TABLE logs ADD c INT"},
		{name: "create index", query: "CREATE INDEX i ON orders (id)"},
		{name: "drop index", query: "DROP INDEX i ON orders"},
		{name: "create database", query: "CREATE DATABASE other"},
		{name: "trigger with definer", query: "CREATE DEFINER=`root`@`%` TRIGGER trg BEFORE INSERT ON orders FOR EACH ROW SET NEW.id = 1"},
		{name: "view", query: "CREATE OR REPLACE VIEW v AS SELECT id FROM orders"},
		{name: "drop user", query: "DROP USER bob"},
		{name: "grant", query: "GRANT SELECT ON shop.* TO bob"},
		{name: "commented trigger", query: "-- cleanup\nDROP TRIGGER trg"},
		{name: "create if not exists", query: "CREATE TABLE IF NOT EXISTS orders (id INT)"},
		{name: "unparseable create of another table", query: "CREATE TABLE geo (id INT PRIMARY KEY, g POINT NOT NULL)"},
		{name: "unparseable alter of a $ name", query: "ALTER TABLE orders$archive WIBBLE"},
		{name: "unparseable alter of a non-ASCII name", query: "ALTER TABLE ordersé WIBBLE"},
		{name: "other-case alter on a case-sensitive server", query: "ALTER TABLE ORDERS ADD c INT"},
		{name: "other-case drop on a case-sensitive server", query: "DROP TABLE shop.CUSTOMERS"},
		{name: "unparseable alter of another table", query: "ALTER TABLE logs WIBBLE"},
		{name: "drop temporary table", query: "DROP TEMPORARY TABLE IF EXISTS orders"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := ddlCache()
			before := c.snapshot(ddlPos(200), nil)
			c.applyDDL(tt.query, "shop", ddlPos(200), nil)
			assert.Equal(t, before, c.tables)
		})
	}
}

// A table named like an object keyword (user, event, view, ...) is still a table: nonTableDDLRe
// only matches the keyword right after the verb, so ALTER TABLE user is applied, not skipped.
func testApplyDDLTableNamedLikeKeyword(t *testing.T) {
	c := ddlCache()
	c.tables["shop.user"] = &tableMeta{Columns: []columnMeta{{Name: "id"}}, AnchoredAt: ddlPos(100)}

	c.applyDDL("ALTER TABLE user ADD c INT", "shop", ddlPos(200), nil)

	require.Contains(t, c.tables, "shop.user")
	assert.Equal(t, []string{"id", "c"}, columnNames(c.tables["shop.user"]))
}

// Statements that give a table a new layout, or a new name, forget only the tables named.
func testApplyDDLDropsNamedTables(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		schema   string
		wantKeys []string
	}{
		{name: "drop table", query: "DROP TABLE customers", schema: "shop", wantKeys: []string{"shop.orders", "sales.invoices"}},
		{name: "drop table list", query: "/* gh-ost */ DROP TABLE IF EXISTS shop.customers, sales.invoices", schema: "x", wantKeys: []string{"shop.orders"}},
		{name: "drop database", query: "DROP DATABASE shop", schema: "", wantKeys: []string{"sales.invoices"}},
		{name: "create table select", query: "CREATE TABLE customers SELECT id FROM orders", schema: "shop", wantKeys: []string{"shop.orders", "sales.invoices"}},
		{name: "create like uncached table", query: "CREATE TABLE customers LIKE archive", schema: "shop", wantKeys: []string{"shop.orders", "sales.invoices"}},
		{name: "gh-ost cutover with uncached shadow", query: "RENAME TABLE orders TO _orders_del, _orders_gho TO orders", schema: "shop", wantKeys: []string{"shop._orders_del", "shop.customers", "sales.invoices"}},
		{name: "rename to cached name", query: "RENAME TABLE archive TO customers", schema: "shop", wantKeys: []string{"shop.orders", "sales.invoices"}},
		{name: "alter rename", query: "ALTER TABLE orders RENAME TO orders_old", schema: "shop", wantKeys: []string{"shop.customers", "sales.invoices"}},
		{name: "set statement prefix", query: "SET STATEMENT lock_wait_timeout=5 FOR DROP TABLE customers", schema: "shop", wantKeys: []string{"shop.orders", "sales.invoices"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := ddlCache()
			c.applyDDL(tt.query, tt.schema, ddlPos(200), nil)
			assert.ElementsMatch(t, tt.wantKeys, cacheKeys(c))
		})
	}
}

// With lower_case_table_names 1 or 2 names match in any case: an ALTER is applied to the cached
// entry, not dropped, and a DROP removes it.
func testApplyDDLFoldCase(t *testing.T) {
	c := newSchemaCache(nil, Binlog{}, true)
	c.tables["shop.orders"] = &tableMeta{Columns: []columnMeta{{Name: "id"}}, AnchoredAt: ddlPos(100)}
	c.tables["shop.customers"] = &tableMeta{Columns: []columnMeta{{Name: "id"}}, AnchoredAt: ddlPos(100)}

	c.applyDDL("ALTER TABLE Shop.ORDERS ADD COLUMN c INT", "", ddlPos(200), nil)
	assert.Equal(t, []string{"id", "c"}, columnNames(c.tables["shop.orders"]))

	c.applyDDL("DROP TABLE SHOP.Customers", "", ddlPos(300), nil)
	assert.Equal(t, []string{"shop.orders"}, cacheKeys(c))
}

// MariaDB's /*M!...*/ versioned comment, ALTER ONLINE and CREATE OR REPLACE are applied as the
// statement they run, as are MySQL's /*!...*/ comments.
func testApplyDDLMariaDBSyntax(t *testing.T) {
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
			c.applyDDL(tt.query, "shop", ddlPos(200), nil)
			c.applyDDL("ALTER TABLE orders ADD COLUMN later INT", "shop", ddlPos(300), nil)
			assert.Equal(t, append(tt.want, "later"), columnNames(c.tables["shop.orders"]))
		})
	}
}

// A CREATE of a cached name is read from its definition, so a later ALTER in the backlog applies to
// it. With no charset on a text column and no table default it cannot be read, so the name drops.
func testApplyDDLCreateTable(t *testing.T) {
	c := ddlCache()
	c.applyDDL("CREATE TABLE customers (id INT UNSIGNED, kind ENUM('a','b'), note TEXT) DEFAULT CHARSET=latin1", "shop", ddlPos(200), nil)
	got := c.tables["shop.customers"]
	require.NotNil(t, got)
	assert.Equal(t, []string{"id", "kind", "note"}, columnNames(got))
	assert.True(t, got.Columns[0].Unsigned)
	assert.Equal(t, []string{"a", "b"}, got.Columns[1].EnumValues)
	assert.Equal(t, "latin1", charsetOf(t, got.Columns[2].CollationID))
	assert.Equal(t, ddlPos(200), got.AnchoredAt)

	c.applyDDL("CREATE TABLE customers (id INT, note TEXT)", "shop", ddlPos(300), nil)
	assert.NotContains(t, cacheKeys(c), "shop.customers")

	c.streamed["shop.customers"] = true
	c.applyDDL("CREATE TABLE customers (id INT, b INT)", "shop", ddlPos(400), nil)
	assert.Equal(t, []string{"id", "b"}, columnNames(c.tables["shop.customers"]), "a streamed table is read from its definition even when not cached")

	c.tables["shop.orders"].SchemaCollationID = mustCollationID(t, "latin1_swedish_ci")
	c.applyDDL("DROP TABLE customers", "shop", ddlPos(500), nil)
	c.applyDDL("CREATE TABLE customers (id INT, note VARCHAR(10))", "shop", ddlPos(501), nil)
	require.Contains(t, cacheKeys(c), "shop.customers")
	assert.Equal(t, "latin1", charsetOf(t, c.tables["shop.customers"].Columns[1].CollationID), "a column with no charset takes the database default")

	c.applyDDL("ALTER DATABASE shop CHARACTER SET utf8mb4", "", ddlPos(600), nil)
	c.applyDDL("DROP TABLE customers", "shop", ddlPos(601), nil)
	c.applyDDL("CREATE TABLE customers (id INT, note VARCHAR(10))", "shop", ddlPos(602), nil)
	assert.Equal(t, "utf8mb4", charsetOf(t, c.tables["shop.customers"].Columns[1].CollationID), "ALTER DATABASE changes the default later tables take")
}

// gh-ost and pt-online-schema-change copy the table with CREATE ... LIKE, alter the copy and swap
// it in with RENAME. The cache follows the copy, so a later ALTER of t applies to the new layout.
func testApplyDDLOnlineSchemaChange(t *testing.T) {
	c := ddlCache()
	pos := uint32(200)
	for _, query := range []string{
		"CREATE TABLE _orders_gho LIKE orders",
		"ALTER TABLE _orders_gho ADD COLUMN x INT",
		"RENAME TABLE orders TO _orders_del, _orders_gho TO orders",
		"DROP TABLE _orders_del",
		"ALTER TABLE orders ADD COLUMN y INT",
	} {
		c.applyDDL(query, "shop", ddlPos(pos), nil)
		pos++
	}
	assert.Equal(t, []string{"id", "status", "name", "x", "y"}, columnNames(c.tables["shop.orders"]))
	assert.ElementsMatch(t, []string{"shop.orders", "shop.customers", "sales.invoices"}, cacheKeys(c))
}

// An ALTER that cannot be applied without guessing drops that table alone, to reload later.
func testApplyDDLDropsTableItCannotApply(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{name: "unknown column", query: "ALTER TABLE orders DROP COLUMN missing"},
		{name: "unknown AFTER column", query: "ALTER TABLE orders ADD c INT AFTER missing"},
		{name: "duplicate column", query: "ALTER TABLE orders ADD id INT"},
		{name: "convert to default", query: "ALTER TABLE orders CONVERT TO CHARACTER SET DEFAULT"},
		{name: "one bad change of many", query: "ALTER TABLE orders ADD a INT, DROP COLUMN missing"},
		{name: "unparseable alter", query: "ALTER TABLE orders WIBBLE"},
		{name: "invisible column", query: "ALTER TABLE orders ADD COLUMN c INT INVISIBLE"},
		{name: "qualified unparseable alter", query: "ALTER TABLE `shop`.`orders` ADD c POINT SRID 4326"},
		{name: "unparseable create", query: "CREATE TABLE orders (id INT PRIMARY KEY, g POINT NOT NULL)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := ddlCache()
			c.applyDDL(tt.query, "shop", ddlPos(200), nil)
			assert.ElementsMatch(t, []string{"shop.customers", "sales.invoices"}, cacheKeys(c))
		})
	}
}

// A column added without a charset takes the table default, which DEFAULT CHARSET changes.
func testApplyDDLTableDefaultCharset(t *testing.T) {
	c := ddlCache()
	c.applyDDL("ALTER TABLE orders ADD note VARCHAR(255)", "shop", ddlPos(200), nil)
	require.Contains(t, c.tables, "shop.orders")
	assert.Equal(t, uint64(utf8mb4GeneralCI), column(t, c.tables["shop.orders"], "note").CollationID)

	c.applyDDL("ALTER TABLE orders DEFAULT CHARSET=latin1", "shop", ddlPos(300), nil)
	c.applyDDL("ALTER TABLE orders ADD memo TEXT", "shop", ddlPos(400), nil)
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
func testApplyDDLDropsEntryNewerThanDDL(t *testing.T) {
	tests := []struct {
		name   string
		anchor mysql.Position
	}{
		{name: "anchored after the DDL", anchor: ddlPos(500)},
		{name: "anchored at the DDL", anchor: ddlPos(400)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := ddlCache()
			c.tables["shop.orders"].AnchoredAt = tt.anchor

			c.applyDDL("ALTER TABLE orders ADD c INT", "shop", ddlPos(400), nil)

			assert.NotContains(t, c.tables, "shop.orders")
			assert.Contains(t, c.tables, "shop.customers")
		})
	}
}

// The helpers report what they did instead of logging it.
func testDDLOutcome(t *testing.T) {
	c := ddlCache()
	stmts, _, err := parser.New().ParseSQL("ALTER TABLE orders ADD c INT; DROP TABLE customers")
	require.NoError(t, err)

	var out ddlOutcome
	for _, stmt := range stmts {
		out.merge(c.applyStmtNoLock(stmt, ddlEvent{defaultSchema: "shop", pos: ddlPos(200)}))
	}
	assert.Equal(t, ddlOutcome{applied: []string{"shop.orders"}, dropped: []string{"shop.customers"}}, out)
}

// When a statement cannot be read, or names a table with no database, any cached table
// could be affected, so the whole cache is dropped.
func testApplyDDLWipesWhenUnsure(t *testing.T) {
	tests := []struct {
		name   string
		query  string
		schema string
	}{
		{name: "unparseable non-alter", query: "CREATE WIBBLE orders", schema: "shop"},
		{name: "no database", query: "DROP TABLE orders", schema: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := ddlCache()
			c.applyDDL(tt.query, tt.schema, ddlPos(200), nil)
			assert.Empty(t, c.tables)
		})
	}
}

// A query that parses into several statements is applied statement by statement.
func testApplyDDLSeveralStatements(t *testing.T) {
	c := ddlCache()
	c.applyDDL("ALTER TABLE orders ADD a INT; ALTER TABLE customers ADD b INT", "shop", ddlPos(200), nil)
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
			c.applyDDL("ALTER TABLE orders MODIFY status ENUM('new','paid')", "shop", ddlPos(200+i), nil)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			_ = c.snapshot(ddlPos(1000), nil)
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

// DROP TABLE t can leave the cache empty when t was its only entry. The CREATE TABLE t and ALTER
// after it must still be applied, or the next row reloads a schema that already has the ALTER.
func testApplyDDLRecreatesOnlyCachedTable(t *testing.T) {
	c := newSchemaCache(nil, Binlog{}, false)
	c.streamed["shop.t"] = true
	c.setTable("shop.t", &tableMeta{Columns: []columnMeta{{Name: "id"}, {Name: "a"}, {Name: "b"}}, AnchoredAt: ddlPos(100)})

	c.applyDDL("DROP TABLE t", "shop", ddlPos(200), nil)
	c.applyDDL("CREATE TABLE t (id INT PRIMARY KEY, b INT, a INT)", "shop", ddlPos(201), nil)
	assert.Equal(t, []string{"id", "b", "a"}, columnNames(c.tables["shop.t"]))

	c.applyDDL("ALTER TABLE t ADD COLUMN c INT AFTER id", "shop", ddlPos(202), nil)
	assert.Equal(t, []string{"id", "c", "b", "a"}, columnNames(c.tables["shop.t"]))
}

// MySQL logs CREATE TABLE IF NOT EXISTS even when the table exists, where it changes nothing, so it
// stands in for an uncached table only right after a DROP of it, when it must have created it.
func testApplyDDLCreateIfNotExistsUncached(t *testing.T) {
	c := newSchemaCache(nil, Binlog{}, false)
	c.streamed["shop.t"] = true
	c.setTable("shop.t", &tableMeta{Columns: []columnMeta{{Name: "id"}, {Name: "b"}, {Name: "a"}}, AnchoredAt: ddlPos(100)})

	c.applyDDL("ALTER TABLE t CONVERT TO CHARACTER SET DEFAULT", "shop", ddlPos(200), nil)
	require.NotContains(t, cacheKeys(c), "shop.t")
	c.applyDDL("CREATE TABLE IF NOT EXISTS t (id INT PRIMARY KEY, a INT, b INT)", "shop", ddlPos(201), nil)
	assert.NotContains(t, cacheKeys(c), "shop.t", "t still exists, so the CREATE changed nothing")

	c.setTable("shop.t", &tableMeta{Columns: []columnMeta{{Name: "id"}, {Name: "b"}, {Name: "a"}}, AnchoredAt: ddlPos(300)})
	c.applyDDL("DROP TABLE t", "shop", ddlPos(400), nil)
	c.applyDDL("CREATE TABLE IF NOT EXISTS t (id INT PRIMARY KEY, a INT, b INT)", "shop", ddlPos(401), nil)
	assert.Equal(t, []string{"id", "a", "b"}, columnNames(c.tables["shop.t"]), "right after the DROP the CREATE made t")
}

// A database's default collation outlives its last cached table, so a CREATE after DROP TABLE of
// that table can still read a text column that names no charset. DROP DATABASE forgets it.
func testApplyDDLKeepsSchemaDefault(t *testing.T) {
	c := newSchemaCache(nil, Binlog{}, false)
	c.streamed["shop.t"] = true
	c.setTable("shop.t", &tableMeta{
		Columns:           []columnMeta{{Name: "id"}},
		AnchoredAt:        ddlPos(100),
		SchemaCollationID: mustCollationID(t, "latin1_swedish_ci"),
	})

	c.applyDDL("DROP TABLE t", "shop", ddlPos(200), nil)
	c.applyDDL("CREATE TABLE t (id INT PRIMARY KEY, note VARCHAR(10))", "shop", ddlPos(201), nil)
	require.Contains(t, cacheKeys(c), "shop.t")
	assert.Equal(t, "latin1", charsetOf(t, column(t, c.tables["shop.t"], "note").CollationID))

	c.applyDDL("DROP TABLE t", "shop", ddlPos(300), nil)
	c.applyDDL("DROP DATABASE shop", "", ddlPos(301), nil)
	c.applyDDL("CREATE TABLE shop.t (id INT PRIMARY KEY, note VARCHAR(10))", "", ddlPos(302), nil)
	assert.NotContains(t, cacheKeys(c), "shop.t", "a recreated database may have another default")
}
