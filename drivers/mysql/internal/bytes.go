package driver

import (
	"database/sql"

	"github.com/datazip-inc/olake/pkg/binlog"
)

// mysqlRowBytes returns the approximate InnoDB on-disk byte sum for a row
// scanned via database/sql. Uses the SQL type name for fixed-width types and
// the actual byte length of the Go value for variable-width types.
func mysqlRowBytes(vals []any, colTypes []*sql.ColumnType) int64 {
	var total int64
	for i, v := range vals {
		total += binlog.MysqlColumnBytes(v, colTypes[i].DatabaseTypeName())
	}
	return total
}

// addRowBytes is the row-level callback passed to jdbc.MapScan /
// jdbc.MapScanConcurrent. It is called once per complete row with all raw
// (pre-conversion) column values so that byte counting adds zero extra loops.
func (m *MySQL) addRowBytes(vals []any, colTypes []*sql.ColumnType) {
	m.bytesRead.Add(mysqlRowBytes(vals, colTypes))
}
