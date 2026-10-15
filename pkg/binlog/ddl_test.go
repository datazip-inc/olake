package binlog

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
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
	c := newSchemaCache(nil, Binlog{})
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

func cacheKeys(c *schemaCache) []string {
	keys := make([]string, 0, len(c.tables))
	for key := range c.tables {
		keys = append(keys, key)
	}
	return keys
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
	assert.Equal(t, []string{"new", "pending", "paid"}, c.tables["shop.orders"].Columns[1].EnumValues)
	assert.Equal(t, ddlPos(200), c.tables["shop.orders"].AnchoredAt)

	c.applyDDL("ALTER TABLE orders MODIFY status ENUM('new','review','pending','paid')", "shop", ddlPos(300))
	assert.Equal(t, []string{"new", "review", "pending", "paid"}, c.tables["shop.orders"].Columns[1].EnumValues)
	assert.Equal(t, ddlPos(300), c.tables["shop.orders"].AnchoredAt)
}

// snapshot() shares *tableMeta with the saved state, so an ALTER must replace the entry
// rather than mutate it.
func TestApplyDDLCopiesOnWrite(t *testing.T) {
	c := ddlCache()
	saved := c.snapshot(ddlPos(200))

	c.applyDDL("ALTER TABLE orders MODIFY status ENUM('new','pending','paid'), DROP COLUMN name", "shop", ddlPos(200))

	assert.Equal(t, []string{"new", "paid"}, saved["shop.orders"].Columns[1].EnumValues)
	assert.Equal(t, []string{"id", "status", "name"}, columnNames(saved["shop.orders"]))
	assert.Equal(t, ddlPos(100), saved["shop.orders"].AnchoredAt)
}

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

func TestApplyDDLColumnDetails(t *testing.T) {
	c := ddlCache()
	c.applyDDL("ALTER TABLE orders "+
		"ADD z INT ZEROFILL, "+
		"ADD u BIGINT UNSIGNED, "+
		"ADD b VARBINARY(4), "+
		"ADD l VARCHAR(5) CHARACTER SET latin1, "+
		"ADD bin VARCHAR(5) CHARACTER SET utf8mb4 BINARY, "+
		"ADD c VARCHAR(5) COLLATE utf8mb4_bin, "+
		"ADD tags SET('a','b')", "shop", ddlPos(200))

	meta := c.tables["shop.orders"]
	require.NotNil(t, meta)
	cols := meta.Columns[3:]

	assert.True(t, cols[0].Unsigned, "ZEROFILL implies UNSIGNED")
	assert.True(t, cols[1].Unsigned)
	assert.Equal(t, uint64(binaryCollationID), cols[2].CollationID)
	assert.Equal(t, "latin1", charsetOf(t, cols[3].CollationID))
	assert.Equal(t, mustCollationID(t, "utf8mb4_bin"), cols[4].CollationID, "BINARY attribute takes the _bin collation")
	assert.Equal(t, mustCollationID(t, "utf8mb4_bin"), cols[5].CollationID)
	assert.Equal(t, []string{"a", "b"}, cols[6].SetMembers)
}

func TestApplyDDLConvertCharset(t *testing.T) {
	c := ddlCache()
	c.tables["shop.orders"].Columns = append(c.tables["shop.orders"].Columns,
		columnMeta{Name: "raw", CollationID: binaryCollationID})

	c.applyDDL("ALTER TABLE orders CONVERT TO CHARACTER SET latin1", "shop", ddlPos(200))

	cols := c.tables["shop.orders"].Columns
	assert.Equal(t, uint64(0), cols[0].CollationID, "non-character columns are untouched")
	assert.Equal(t, "latin1", charsetOf(t, cols[2].CollationID))
	assert.Equal(t, uint64(binaryCollationID), cols[3].CollationID, "binary columns are untouched")
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

// Statements that change no captured table's columns must leave the cache as it is.
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
		{"other-case table name", "ALTER TABLE ORDERS ADD c INT"},
		{"unparseable create of another table", "CREATE TABLE geo (id INT PRIMARY KEY, g POINT NOT NULL)"},
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
		{"create table", "CREATE TABLE customers (id INT)", "shop", []string{"shop.orders", "sales.invoices"}},
		{"gh-ost cutover", "RENAME TABLE orders TO _orders_del, _orders_gho TO orders", "shop", []string{"shop.customers", "sales.invoices"}},
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
	assert.Equal(t, uint64(utf8mb4GeneralCI), c.tables["shop.orders"].Columns[3].CollationID)

	c.applyDDL("ALTER TABLE orders DEFAULT CHARSET=latin1", "shop", ddlPos(300))
	c.applyDDL("ALTER TABLE orders ADD memo TEXT", "shop", ddlPos(400))
	cols := c.tables["shop.orders"].Columns
	assert.Equal(t, "utf8mb4", charsetOf(t, cols[2].CollationID), "existing columns keep their charset")
	assert.Equal(t, "latin1", charsetOf(t, cols[4].CollationID))
}

// An entry loaded from information_schema after this DDL may already include it, so it is
// dropped rather than altered a second time.
func TestApplyDDLDropsEntryNewerThanDDL(t *testing.T) {
	c := ddlCache()
	c.tables["shop.orders"].AnchoredAt = ddlPos(500)

	c.applyDDL("ALTER TABLE orders ADD c INT", "shop", ddlPos(400))

	assert.NotContains(t, c.tables, "shop.orders")
	assert.Contains(t, c.tables, "shop.customers")
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
		{"several statements", "ALTER TABLE orders ADD a INT; ALTER TABLE customers ADD b INT", "shop"},
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

// Any DDL, a trigger included, may give a table a new TableID, so it clears every recorded one.
func TestApplyDDLForgetsTableIDs(t *testing.T) {
	c := ddlCache()
	c.tables["shop.orders"].TableID = 7
	saved := c.snapshot(ddlPos(200))

	c.applyDDL("CREATE DEFINER=`root`@`%` TRIGGER trg AFTER INSERT ON orders FOR EACH ROW SET @x = 1", "shop", ddlPos(200))

	assert.Equal(t, uint64(0), c.tables["shop.orders"].TableID)
	assert.Equal(t, []string{"id", "status", "name"}, columnNames(c.tables["shop.orders"]))
	assert.Equal(t, uint64(7), saved["shop.orders"].TableID, "the saved entry is not edited")
}
