package driver

import (
	"database/sql"
	"fmt"
	"strings"
)

// Sizing classes for a SQL Server column, precomputed by mssqlClassifyCol so
// the hot per-row path never re-dispatches on the type name.
const (
	mssqlColFixed uint8 = iota
	mssqlColNText
	mssqlColVariable
)

// mssqlClassifyCol maps a SQL Server type name to a sizing class and, for
// fixed-width types, their documented SQL Server storage sizes.
//
// NCHAR/NVARCHAR/NTEXT are stored as UTF-16 in SQL Server: len([]rune)*2 bytes per rune.
// Variable-width types (varchar, binary, etc.) use actual byte length.
//
// DatabaseTypeName() from go-mssqldb returns uppercase names (BIGINT, INT, etc.).
func mssqlClassifyCol(typeName string) (uint8, int64) {
	t := strings.ToUpper(strings.TrimSpace(typeName))
	switch t {
	case "TINYINT":
		return mssqlColFixed, 1
	case "SMALLINT":
		return mssqlColFixed, 2
	case "SMALLMONEY":
		return mssqlColFixed, 4
	case "INT":
		return mssqlColFixed, 4
	case "BIGINT", "MONEY", "FLOAT", "DATETIME", "DATETIME2":
		return mssqlColFixed, 8
	case "REAL":
		return mssqlColFixed, 4
	case "BIT":
		return mssqlColFixed, 1
	case "DATE":
		return mssqlColFixed, 3
	case "TIME":
		return mssqlColFixed, 5 // TIME(7) max
	case "SMALLDATETIME":
		return mssqlColFixed, 4
	case "DATETIMEOFFSET":
		return mssqlColFixed, 10 // DATETIMEOFFSET(7) max
	case "UNIQUEIDENTIFIER":
		return mssqlColFixed, 16
	case "ROWVERSION", "TIMESTAMP": // TIMESTAMP is rowversion synonym in SQL Server
		return mssqlColFixed, 8
	case "NCHAR", "NVARCHAR", "NTEXT":
		return mssqlColNText, 0
	default:
		// CHAR, VARCHAR, TEXT, BINARY, VARBINARY, IMAGE,
		// DECIMAL, NUMERIC, XML, SQL_VARIANT, GEOMETRY, GEOGRAPHY,
		// HIERARCHYID, SYSNAME, JSON and any unknown types.
		return mssqlColVariable, 0
	}
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

// mssqlRowBytes returns the SQL Server on-disk byte size of a single column value
// scanned via database/sql; NULL returns 0. It is the per-column entry point used by
// incremental via MapScan, which re-derives column types on every row so no per-query
// cache can apply.
func mssqlRowBytes(colType *sql.ColumnType, v any) int64 {
	if v == nil {
		return 0
	}
	kind, width := mssqlClassifyCol(colType.DatabaseTypeName())
	switch kind {
	case mssqlColFixed:
		return width
	case mssqlColNText:
		return mssqlNTextBytes(v)
	default:
		return mssqlVariableBytes(v)
	}
}

// mssqlRowSizer classifies every column once and returns a per-column sizing function.
// Backfill passes it to MapScanConcurrent, where column types are constant for
// the whole result set, so the hot loop is an index + int-switch per column.
func mssqlRowSizer(colTypes []*sql.ColumnType) func(i int, v any) int64 {
	kinds := make([]uint8, len(colTypes))
	widths := make([]int64, len(colTypes))
	for i, ct := range colTypes {
		kinds[i], widths[i] = mssqlClassifyCol(ct.DatabaseTypeName())
	}
	return func(i int, v any) int64 {
		if v == nil {
			return 0
		}
		switch kinds[i] {
		case mssqlColFixed:
			return widths[i]
		case mssqlColNText:
			return mssqlNTextBytes(v)
		default:
			return mssqlVariableBytes(v)
		}
	}
}

// mssqlCDCRowBytes returns the on-disk bytes of a single CDC data column, returning 0
// for the CDC metadata columns (__$operation, __$start_lsn, __$seqval, __$update_mask)
// so they are excluded from the row total.
func mssqlCDCRowBytes(colType *sql.ColumnType, v any) int64 {
	if strings.HasPrefix(colType.Name(), "__$") {
		return 0
	}
	return mssqlRowBytes(colType, v)
}
