package driver

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// db2ColumnBytes returns the IBM DB2 on-disk byte count for a single column value.
//
// Fixed-width types use their IBM DB2 storage sizes:
//   - SMALLINT = 2, INTEGER/INT = 4, BIGINT = 8
//   - REAL = 4, FLOAT/DOUBLE = 8
//   - BOOLEAN = 1
//   - DATE = 4, TIME = 3, TIMESTAMP = 10
//
// For DECIMAL/NUMERIC: go-ibm-db returns these as string or float64. We use
// len(string) as the byte count (the text representation).
//
// DECFLOAT arrives as float64 (8 bytes). GRAPHIC/VARGRAPHIC (DBCS) arrives as
// string — len(str) approximates the 2-byte-per-char encoding.
//
// NULL values return 0 — DB2 stores no bytes for NULL columns.
func db2ColumnBytes(rawVal any, typeName string) int64 {
	if rawVal == nil {
		return 0
	}
	t := strings.ToUpper(strings.TrimSpace(typeName))
	// Strip precision/scale: "DECIMAL(10,2)" → "DECIMAL"
	if idx := strings.IndexByte(t, '('); idx >= 0 {
		t = strings.TrimSpace(t[:idx])
	}

	switch t {
	case "SMALLINT":
		return 2
	case "INTEGER", "INT":
		return 4
	case "BIGINT":
		return 8
	case "REAL":
		return 4
	case "FLOAT", "DOUBLE":
		return 8
	case "BOOLEAN":
		return 1
	case "DATE":
		return 4 // DB2 DATE = 4 bytes
	case "TIME":
		return 3 // DB2 TIME = 3 bytes
	case "TIMESTAMP":
		return 10 // DB2 TIMESTAMP = 10 bytes
	}

	// Variable-length and remaining types: DECIMAL, NUMERIC, DECFLOAT,
	// CHAR, VARCHAR, LONG VARCHAR, CLOB, DBCLOB,
	// GRAPHIC, VARGRAPHIC, LONG VARGRAPHIC,
	// BLOB, BINARY, VARBINARY, XML, ARRAY, ROW.
	// Also handles any unknown type safely.
	switch v := rawVal.(type) {
	case int32:
		return 4
	case int64:
		return 8
	case float32:
		return 4
	case float64:
		return 8
	case bool:
		return 1
	case string:
		return int64(len(v))
	case []byte:
		return int64(len(v))
	case time.Time:
		// Fallback for any unmatched time value — use TIMESTAMP size
		return 10
	default:
		return int64(len(fmt.Sprintf("%v", v)))
	}
}

// db2RowBytes sums column bytes for a complete row scanned via database/sql.
func db2RowBytes(vals []any, colTypes []*sql.ColumnType) int64 {
	var total int64
	for i, v := range vals {
		total += db2ColumnBytes(v, colTypes[i].DatabaseTypeName())
	}
	return total
}
