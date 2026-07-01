package driver

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/datazip-inc/olake/pkg/waljs"
)

// Fixed-width types use their natural width regardless of value:
//
//	INT2 / SMALLINT            → 2 bytes
//	INT4 / INTEGER             → 4 bytes
//	INT8 / BIGINT              → 8 bytes
//	FLOAT4 / REAL              → 4 bytes
//	FLOAT8 / DOUBLE PRECISION  → 8 bytes
//	BOOL                       → 1 byte
//	DATE                       → 4 bytes
//	TIME / TIMETZ              → 8 bytes
//	TIMESTAMP / TIMESTAMPTZ    → 8 bytes
//	UUID                       → 16 bytes
//	OID                        → 4 bytes
//	MONEY                      → 8 bytes
//
// NUMERIC / DECIMAL use PostgreSQL's base-10000 digit encoding. All other
// variable-width types (VARCHAR, TEXT, BYTEA, JSON, JSONB, arrays …) use the actual
// length of the value. NULL values return 0.
func pgStorageBytes(rawVal any, colType *sql.ColumnType) int64 {
	if rawVal == nil {
		return 0
	}

	switch strings.ToUpper(colType.DatabaseTypeName()) {
	case "INT2", "SMALLINT", "SMALLSERIAL":
		return 2
	case "INT4", "INT", "INTEGER", "SERIAL":
		return 4
	case "INT8", "BIGINT", "BIGSERIAL":
		return 8
	case "FLOAT4", "REAL":
		return 4
	case "FLOAT8", "FLOAT", "DOUBLE PRECISION":
		return 8
	case "BOOL", "BOOLEAN":
		return 1
	case "DATE":
		return 4
	case "TIME", "TIMETZ":
		return 8
	case "TIMESTAMP", "TIMESTAMPTZ":
		return 8
	case "UUID":
		return 16
	case "OID":
		return 4
	case "MONEY":
		return 8
	case "NUMERIC", "DECIMAL":
		// NUMERIC uses PostgreSQL's base-10000 digit encoding; pgx scans it as text.
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
	default:
		// Variable-width: VARCHAR, BPCHAR, TEXT, BYTEA, JSON, JSONB, arrays, ranges, etc.
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

// pgCompositeRowBytes sums the data bytes of every non-NULL column in a row scanned
// via database/sql. It is the per-row entry point used by backfill and incremental.
func pgCompositeRowBytes(vals []any, colTypes []*sql.ColumnType) int64 {
	var total int64
	for i, v := range vals {
		if v == nil {
			continue // NULL columns carry no data
		}
		total += pgStorageBytes(v, colTypes[i])
	}
	return total
}
