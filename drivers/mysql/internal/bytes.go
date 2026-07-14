package driver

import (
	"database/sql"

	"github.com/datazip-inc/olake/pkg/binlog"
)

// mysqlRowBytes returns the approximate InnoDB on-disk byte size of a single column
// value scanned via database/sql, using the SQL type name for fixed-width types and
// the actual byte length of the Go value for variable-width types. It is the
// per-column entry point used by incremental via MapScan, which re-derives column types on every row.
func mysqlRowBytes(colType *sql.ColumnType, v any) int64 {
	return binlog.MysqlColumnBytes(v, colType.DatabaseTypeName())
}

// mysqlRowSizer classifies every column once and returns a per-column sizing function.
// Backfill passes it to MapScanConcurrent, where column types are constant for
// the whole result set, so the hot loop avoids re-dispatching on type names.
func mysqlRowSizer(colTypes []*sql.ColumnType) func(i int, v any) int64 {
	widths := make([]int64, len(colTypes))
	fixed := make([]bool, len(colTypes))
	for i, ct := range colTypes {
		widths[i], fixed[i] = binlog.MysqlFixedWidth(ct.DatabaseTypeName())
	}
	return func(i int, v any) int64 {
		if v == nil {
			return 0
		}
		if fixed[i] {
			return widths[i]
		}
		return binlog.MysqlValueBytes(v)
	}
}
