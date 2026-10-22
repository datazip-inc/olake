package binlog

import (
	"fmt"
	"strings"
)

// mysqlCDCRowBytes returns the approximate InnoDB on-disk byte sum for a
// binlog row, using the raw decoded Go values and their MySQL type names.
func mysqlCDCRowBytes(row []interface{}, columnTypes []string) int64 {
	var total int64
	for i, v := range row {
		typeName := ""
		if i < len(columnTypes) {
			typeName = columnTypes[i]
		}
		total += MysqlColumnBytes(v, typeName)
	}
	return total
}

// MysqlFixedWidth returns the InnoDB storage width for a fixed-width MySQL
// type name (as returned by mysqlTypeName or database/sql ColumnType.DatabaseTypeName)
// and ok=false for variable-length types.
func MysqlFixedWidth(typeName string) (int64, bool) {
	t := strings.ToUpper(strings.TrimSpace(typeName))
	// Strip UNSIGNED prefix (e.g. "UNSIGNED BIGINT" → "BIGINT")
	t = strings.TrimPrefix(t, "UNSIGNED ")
	switch t {
	case "TINYINT", "BOOL", "BOOLEAN", "YEAR":
		return 1, true
	case "SMALLINT":
		return 2, true
	case "MEDIUMINT":
		return 3, true
	case "INT", "INTEGER", "FLOAT":
		return 4, true
	case "BIGINT", "DOUBLE", "REAL":
		return 8, true
	case "DATE":
		return 3, true
	// Conservative max sizes — fractional seconds can add 1-3 bytes beyond the base.
	case "TIME":
		return 6, true // TIME(6) max
	case "TIMESTAMP":
		return 7, true // TIMESTAMP(6) max
	case "DATETIME":
		return 8, true // DATETIME(6) max
	default:
		// Variable-length: VARCHAR, CHAR, TEXT*, BLOB*, DECIMAL, NUMERIC,
		// JSON, BIT, ENUM, SET, GEOMETRY, and any unknown type.
		return 0, false
	}
}

// MysqlValueBytes returns the byte length of a variable-width column value.
func MysqlValueBytes(rawVal any) int64 {
	switch v := rawVal.(type) {
	case string:
		return int64(len(v))
	case []byte:
		return int64(len(v))
	default:
		return int64(len(fmt.Sprintf("%v", v)))
	}
}

// MysqlColumnBytes returns the InnoDB on-disk byte count for a single column
// value identified by its MySQL type name. Fixed-width types use their
// InnoDB storage size; variable-width types use the actual byte length of the Go value.
func MysqlColumnBytes(rawVal any, typeName string) int64 {
	if rawVal == nil {
		return 0
	}
	if width, ok := MysqlFixedWidth(typeName); ok {
		return width
	}
	return MysqlValueBytes(rawVal)
}
