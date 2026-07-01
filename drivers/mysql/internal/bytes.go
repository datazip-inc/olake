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
