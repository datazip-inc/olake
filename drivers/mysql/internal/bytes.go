package driver

import (
	"database/sql"

	"github.com/datazip-inc/olake/drivers/mysql/pkg/binlog"
)

// mysqlColumnSizer returns a function that sizes a single non-NULL value of the given
// column, using the InnoDB on-disk data size: fixed-width types return a constant
// width, variable-width types use the actual byte length of the value. The type is
// classified once per column and the returned function is reused for every row.
// MysqlFixedWidth / MysqlValueBytes live in pkg/binlog so the CDC path can share them.
func mysqlColumnSizer(colType *sql.ColumnType) func(v any) int64 {
	if width, ok := binlog.MysqlFixedWidth(colType.DatabaseTypeName()); ok {
		return func(any) int64 { return width }
	}
	return binlog.MysqlValueBytes
}
