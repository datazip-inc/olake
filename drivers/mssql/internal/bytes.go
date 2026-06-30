package driver

import (
	"database/sql"
	"fmt"
	"strings"
	"sync/atomic"
)

// mssqlColumnBytes returns the SQL Server on-disk byte count for a single column value.
//
// Fixed-width types use their documented SQL Server storage sizes.
// Variable-width types (varchar, nvarchar, binary, etc.) use actual byte length.
// NCHAR/NVARCHAR are stored as UTF-16 in SQL Server: len([]rune)*2 bytes per rune.
// NULL values return 0 — SQL Server stores no data bytes for NULL columns.
//
// DatabaseTypeName() from go-mssqldb returns uppercase names (BIGINT, INT, etc.).
func mssqlColumnBytes(rawVal any, typeName string) int64 {
	if rawVal == nil {
		return 0
	}
	t := strings.ToUpper(strings.TrimSpace(typeName))
	switch t {
	case "TINYINT":
		return 1
	case "SMALLINT", "SMALLMONEY":
		return 2
	case "INT":
		return 4
	case "BIGINT", "MONEY", "FLOAT", "DATETIME", "DATETIME2":
		return 8
	case "REAL":
		return 4
	case "BIT":
		return 1
	case "DATE":
		return 3
	case "TIME":
		return 5 // TIME(7) max
	case "SMALLDATETIME":
		return 4
	case "DATETIMEOFFSET":
		return 10 // DATETIMEOFFSET(7) max
	case "UNIQUEIDENTIFIER":
		return 16
	case "ROWVERSION", "TIMESTAMP": // TIMESTAMP is rowversion synonym in SQL Server
		return 8
	case "NCHAR", "NVARCHAR", "NTEXT":
		// SQL Server stores N-types as UTF-16LE: 2 bytes per BMP rune.
		if s, ok := rawVal.(string); ok {
			return int64(len([]rune(s))) * 2
		}
		return int64(len(fmt.Sprintf("%v", rawVal)))
	default:
		// CHAR, VARCHAR, TEXT, BINARY, VARBINARY, IMAGE,
		// DECIMAL, NUMERIC, XML, SQL_VARIANT, GEOMETRY, GEOGRAPHY,
		// HIERARCHYID, SYSNAME, JSON and any unknown types.
		switch v := rawVal.(type) {
		case string:
			return int64(len(v))
		case []byte:
			return int64(len(v))
		default:
			return int64(len(fmt.Sprintf("%v", v)))
		}
	}
}

// mssqlRowBytes sums column bytes for a complete row scanned via database/sql.
func mssqlRowBytes(vals []any, colTypes []*sql.ColumnType) int64 {
	var total int64
	for i, v := range vals {
		total += mssqlColumnBytes(v, colTypes[i].DatabaseTypeName())
	}
	return total
}

// makeLocalAddRowBytes returns the MapScan/MapScanConcurrent callback for backfill
// and incremental. It accumulates row bytes into a caller-owned local int64 that
// lives on the ChunkIterator / StreamIncrementalChanges call stack, so it resets
// to 0 on every retry (no double counting).
func makeLocalAddRowBytes(local *int64) func([]any, []*sql.ColumnType) {
	return func(vals []any, colTypes []*sql.ColumnType) {
		// atomic: MapScanConcurrent invokes this from the producer goroutine.
		atomic.AddInt64(local, mssqlRowBytes(vals, colTypes))
	}
}

// mssqlCDCRowBytes sums the on-disk bytes of a CDC row's actual data columns,
// excluding the four CDC metadata columns (__$operation, __$start_lsn,
// __$seqval, __$update_mask). Before-image rows (op-code 3) are skipped by the
// caller before emit, so they never reach CDCChange.Bytes.
func mssqlCDCRowBytes(vals []any, colTypes []*sql.ColumnType) int64 {
	var total int64
	for i, v := range vals {
		if strings.HasPrefix(colTypes[i].Name(), "__$") {
			continue
		}
		total += mssqlColumnBytes(v, colTypes[i].DatabaseTypeName())
	}
	return total
}
