package driver

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// oracleFixedWidth returns the Oracle storage width for fixed-width type names
// and ok=false for NUMBER and variable-length types.
func oracleFixedWidth(typeName string) (int64, bool) {
	t := strings.ToUpper(strings.TrimSpace(typeName))
	// Strip precision/scale suffix: "NUMBER(10,2)" → "NUMBER", "TIMESTAMP(6)" → "TIMESTAMP"
	if idx := strings.IndexByte(t, '('); idx >= 0 {
		t = strings.TrimSpace(t[:idx])
	}

	switch {
	case t == "DATE":
		return 7, true
	case strings.HasPrefix(t, "TIMESTAMP"):
		return 11, true // TIMESTAMP(9) max = 7 date + 4 nanosecond bytes
	case strings.HasPrefix(t, "INTERVAL YEAR") || t == "INTERVALYM_DTY":
		return 5, true // INTERVAL YEAR TO MONTH = 5 bytes
	case strings.HasPrefix(t, "INTERVAL DAY") || t == "INTERVALDS_DTY":
		return 11, true // INTERVAL DAY TO SECOND = 11 bytes
	case t == "BINARY_FLOAT":
		return 4, true
	case t == "BINARY_DOUBLE", t == "FLOAT":
		return 8, true
	}
	return 0, false
}

// oracleValueBytes sizes NUMBER and all variable-length types by the Go value type/length.
// go-ora returns:
//
//	NUMBER(p,0) p≤9  → int32  (4 bytes)
//	NUMBER(p,0) p≤18 → int64  (8 bytes)
//	NUMBER with scale → float64 (8 bytes)
//	VARCHAR2/CHAR/CLOB/NCLOB/LONG → string
//	RAW/LONG RAW/BLOB → []byte
//	XMLTYPE → string
func oracleValueBytes(rawVal any) int64 {
	switch v := rawVal.(type) {
	case int32:
		return 4
	case int64:
		return 8
	case float32:
		return 4
	case float64:
		return 8
	case string:
		return int64(len(v))
	case []byte:
		return int64(len(v))
	case time.Time:
		// Unmatched time value — fall back to DATE size
		return 7
	default:
		return int64(len(fmt.Sprintf("%v", v)))
	}
}

// oracleRowBytes returns the Oracle on-disk byte size of a single column value scanned
// via database/sql; NULL returns 0. Fixed-width types use their Oracle storage size
// (DATE=7, TIMESTAMP=11 max, BINARY_FLOAT=4, BINARY_DOUBLE/FLOAT=8); NUMBER and
// variable types (VARCHAR2, CLOB, RAW, BLOB, …) use the Go value's byte length. It is
// the per-column entry point used by incremental via MapScan, which re-derives column types on every row.
func oracleRowBytes(colType *sql.ColumnType, v any) int64 {
	if v == nil {
		return 0
	}
	if width, ok := oracleFixedWidth(colType.DatabaseTypeName()); ok {
		return width
	}
	return oracleValueBytes(v)
}

// oracleRowSizer classifies every column once and returns a per-column sizing function.
// Backfill passes it to MapScanConcurrent, where column types are constant for
// the whole result set, so the hot loop avoids re-dispatching on type names.
func oracleRowSizer(colTypes []*sql.ColumnType) func(i int, v any) int64 {
	widths := make([]int64, len(colTypes))
	fixed := make([]bool, len(colTypes))
	for i, ct := range colTypes {
		widths[i], fixed[i] = oracleFixedWidth(ct.DatabaseTypeName())
	}
	return func(i int, v any) int64 {
		if v == nil {
			return 0
		}
		if fixed[i] {
			return widths[i]
		}
		return oracleValueBytes(v)
	}
}
