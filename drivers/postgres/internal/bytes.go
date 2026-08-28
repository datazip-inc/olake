package driver

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/datazip-inc/olake/drivers/postgres/pkg/waljs"
)

// pgColumnSizer returns a function that sizes a single non-NULL value of the given
// column, using the PostgreSQL on-disk data size:
//
//	INT2 / SMALLINT            → 2 bytes
//	INT4 / INTEGER             → 4 bytes
//	INT8 / BIGINT              → 8 bytes
//	FLOAT4 / REAL              → 4 bytes
//	FLOAT8 / DOUBLE PRECISION  → 8 bytes
//	BOOL                       → 1 byte
//	DATE                       → 4 bytes
//	TIME                       → 8 bytes
//	TIMETZ                     → 12 bytes
//	TIMESTAMP / TIMESTAMPTZ    → 8 bytes
//	UUID                       → 16 bytes
//	OID                        → 4 bytes
//	MONEY                      → 8 bytes
//
// NUMERIC / DECIMAL use PostgreSQL's base-10000 digit encoding; all other
// (variable-width) types use the actual length of the value. The type is classified
// once per column and the returned function is reused for every row of that column.
func pgColumnSizer(colType *sql.ColumnType) func(v any) int64 {
	switch strings.ToUpper(colType.DatabaseTypeName()) {
	case "INT2", "SMALLINT", "SMALLSERIAL":
		return func(any) int64 { return 2 }
	case "INT4", "INT", "INTEGER", "SERIAL":
		return func(any) int64 { return 4 }
	case "INT8", "BIGINT", "BIGSERIAL":
		return func(any) int64 { return 8 }
	case "FLOAT4", "REAL":
		return func(any) int64 { return 4 }
	case "FLOAT8", "FLOAT", "DOUBLE PRECISION":
		return func(any) int64 { return 8 }
	case "BOOL", "BOOLEAN":
		return func(any) int64 { return 1 }
	case "DATE":
		return func(any) int64 { return 4 }
	case "TIME":
		return func(any) int64 { return 8 }
	case "TIMETZ":
		return func(any) int64 { return 12 } // 8 time + 4 zone offset
	case "TIMESTAMP", "TIMESTAMPTZ":
		return func(any) int64 { return 8 }
	case "UUID":
		return func(any) int64 { return 16 }
	case "OID":
		return func(any) int64 { return 4 }
	case "MONEY":
		return func(any) int64 { return 8 }
	case "NUMERIC", "DECIMAL":
		return pgNumericBytes
	default:
		// Variable-width: VARCHAR, BPCHAR, TEXT, BYTEA, JSON, JSONB, arrays, ranges, etc.
		return pgVariableBytes
	}
}

// pgNumericBytes sizes a NUMERIC/DECIMAL value using PostgreSQL's base-10000
// digit encoding; pgx scans it as text.
func pgNumericBytes(rawVal any) int64 {
	var s string
	switch v := rawVal.(type) {
	case string:
		s = v
	case []byte:
		s = string(v)
	default:
		s = fmt.Sprintf("%v", rawVal)
	}
	return waljs.NumericBinaryBytes(s)
}

// pgVariableBytes sizes a variable-width value by its actual length.
func pgVariableBytes(rawVal any) int64 {
	switch v := rawVal.(type) {
	case string:
		return int64(len(v))
	case []byte:
		return int64(len(v))
	default:
		return int64(len(fmt.Sprintf("%v", v)))
	}
}
