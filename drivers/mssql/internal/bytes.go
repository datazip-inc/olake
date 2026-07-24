package driver

import (
	"database/sql"
	"fmt"
	"strings"
)

// mssqlColumnSizer returns a function that sizes a single non-NULL value of the given
// column, using the documented SQL Server on-disk storage size. Fixed-width types
// return a constant width; NCHAR/NVARCHAR/NTEXT are UTF-16 (2 bytes per BMP rune); all
// other (variable-width) types use the actual byte length. The type is classified once
// per column and the returned function is reused for every row.
//
// DatabaseTypeName() from go-mssqldb returns uppercase names (BIGINT, INT, etc.).
func mssqlColumnSizer(colType *sql.ColumnType) func(v any) int64 {
	switch strings.ToUpper(strings.TrimSpace(colType.DatabaseTypeName())) {
	case "TINYINT", "BIT":
		return func(any) int64 { return 1 }
	case "SMALLINT":
		return func(any) int64 { return 2 }
	case "SMALLMONEY", "INT", "REAL", "SMALLDATETIME":
		return func(any) int64 { return 4 }
	case "DATE":
		return func(any) int64 { return 3 }
	case "TIME":
		return func(any) int64 { return 5 } // TIME(7) max
	case "BIGINT", "MONEY", "FLOAT", "DATETIME", "DATETIME2", "ROWVERSION", "TIMESTAMP":
		return func(any) int64 { return 8 } // TIMESTAMP is the rowversion synonym in SQL Server
	case "DATETIMEOFFSET":
		return func(any) int64 { return 10 } // DATETIMEOFFSET(7) max
	case "UNIQUEIDENTIFIER":
		return func(any) int64 { return 16 }
	case "NCHAR", "NVARCHAR", "NTEXT":
		return mssqlNTextBytes
	default:
		// CHAR, VARCHAR, TEXT, BINARY, VARBINARY, IMAGE, DECIMAL, NUMERIC, XML,
		// SQL_VARIANT, GEOMETRY, GEOGRAPHY, HIERARCHYID, SYSNAME, JSON, unknown types.
		return mssqlVariableBytes
	}
}

// mssqlCDCColumnSizer is mssqlColumnSizer for CDC change-table rows: the four CDC
// metadata columns (__$operation, __$start_lsn, __$seqval, __$update_mask) are sized
// as 0 so only the actual data columns count toward the row total.
func mssqlCDCColumnSizer(colType *sql.ColumnType) func(v any) int64 {
	if strings.HasPrefix(colType.Name(), "__$") {
		return func(any) int64 { return 0 }
	}
	return mssqlColumnSizer(colType)
}

// mssqlNTextBytes sizes an N-type value: SQL Server stores N-types as UTF-16LE,
// 2 bytes per BMP rune.
func mssqlNTextBytes(rawVal any) int64 {
	if s, ok := rawVal.(string); ok {
		return int64(len([]rune(s))) * 2
	}
	return int64(len(fmt.Sprintf("%v", rawVal)))
}

// mssqlVariableBytes sizes a variable-width value by its actual byte length.
func mssqlVariableBytes(rawVal any) int64 {
	switch v := rawVal.(type) {
	case string:
		return int64(len(v))
	case []byte:
		return int64(len(v))
	default:
		return int64(len(fmt.Sprintf("%v", v)))
	}
}
