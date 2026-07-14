package driver

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/datazip-inc/olake/pkg/waljs"
)

// Sizing classes for a PostgreSQL column, precomputed by pgClassifyCol so the
// hot per-row path never re-dispatches on the type name.
const (
	pgColFixed uint8 = iota
	pgColNumeric
	pgColVariable
)

// pgClassifyCol maps a PostgreSQL type name to a sizing class and, for
// fixed-width types, the natural width of the type:
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
// NUMERIC / DECIMAL use PostgreSQL's base-10000 digit encoding. All other
// variable-width types (VARCHAR, TEXT, BYTEA, JSON, JSONB, arrays …) use the actual
// length of the value.
func pgClassifyCol(typeName string) (uint8, int64) {
	switch strings.ToUpper(typeName) {
	case "INT2", "SMALLINT", "SMALLSERIAL":
		return pgColFixed, 2
	case "INT4", "INT", "INTEGER", "SERIAL":
		return pgColFixed, 4
	case "INT8", "BIGINT", "BIGSERIAL":
		return pgColFixed, 8
	case "FLOAT4", "REAL":
		return pgColFixed, 4
	case "FLOAT8", "FLOAT", "DOUBLE PRECISION":
		return pgColFixed, 8
	case "BOOL", "BOOLEAN":
		return pgColFixed, 1
	case "DATE":
		return pgColFixed, 4
	case "TIME":
		return pgColFixed, 8
	case "TIMETZ":
		return pgColFixed, 12 // 8 time + 4 zone offset
	case "TIMESTAMP", "TIMESTAMPTZ":
		return pgColFixed, 8
	case "UUID":
		return pgColFixed, 16
	case "OID":
		return pgColFixed, 4
	case "MONEY":
		return pgColFixed, 8
	case "NUMERIC", "DECIMAL":
		return pgColNumeric, 0
	default:
		// Variable-width: VARCHAR, BPCHAR, TEXT, BYTEA, JSON, JSONB, arrays, ranges, etc.
		return pgColVariable, 0
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

// pgRowBytes returns the data bytes of a single column value; NULL returns 0. It is
// the per-column entry point used by incremental via MapScan, which re-derives column
// types on every row so no per-query cache can apply.
func pgRowBytes(colType *sql.ColumnType, v any) int64 {
	if v == nil {
		return 0
	}
	kind, width := pgClassifyCol(colType.DatabaseTypeName())
	switch kind {
	case pgColFixed:
		return width
	case pgColNumeric:
		return pgNumericBytes(v)
	default:
		return pgVariableBytes(v)
	}
}

// pgRowSizer classifies every column once and returns a per-column sizing function.
// Backfill passes it to MapScanConcurrent, where column types are constant for the
// whole result set, so classification is done once and the sizer is an index +
// int-switch per column, summed inline in the scan's conversion loop.
func pgRowSizer(colTypes []*sql.ColumnType) func(i int, v any) int64 {
	kinds := make([]uint8, len(colTypes))
	widths := make([]int64, len(colTypes))
	for i, ct := range colTypes {
		kinds[i], widths[i] = pgClassifyCol(ct.DatabaseTypeName())
	}
	return func(i int, v any) int64 {
		if v == nil {
			return 0 // NULL columns carry no data
		}
		switch kinds[i] {
		case pgColFixed:
			return widths[i]
		case pgColNumeric:
			return pgNumericBytes(v)
		default:
			return pgVariableBytes(v)
		}
	}
}
