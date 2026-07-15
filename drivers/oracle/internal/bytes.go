package driver

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// oracleColumnSizer returns a function that sizes a single non-NULL value of the given
// column, using the Oracle on-disk data size. Fixed-width types return a constant
// (DATE=7, TIMESTAMP=11 max, INTERVAL YEAR=5, INTERVAL DAY=11, BINARY_FLOAT=4,
// BINARY_DOUBLE/FLOAT=8); NUMBER and variable types (VARCHAR2, CLOB, RAW, BLOB, …) use
// the Go value's type/length. The type is classified once per column and the returned
// function is reused for every row.
func oracleColumnSizer(colType *sql.ColumnType) func(v any) int64 {
	t := strings.ToUpper(strings.TrimSpace(colType.DatabaseTypeName()))
	// Strip precision/scale suffix: "NUMBER(10,2)" → "NUMBER", "TIMESTAMP(6)" → "TIMESTAMP"
	if idx := strings.IndexByte(t, '('); idx >= 0 {
		t = strings.TrimSpace(t[:idx])
	}
	switch {
	case t == "DATE":
		return func(any) int64 { return 7 }
	case strings.HasPrefix(t, "TIMESTAMP"):
		return func(any) int64 { return 11 } // TIMESTAMP(9) max = 7 date + 4 nanosecond bytes
	case strings.HasPrefix(t, "INTERVAL YEAR") || t == "INTERVALYM_DTY":
		return func(any) int64 { return 5 } // INTERVAL YEAR TO MONTH
	case strings.HasPrefix(t, "INTERVAL DAY") || t == "INTERVALDS_DTY":
		return func(any) int64 { return 11 } // INTERVAL DAY TO SECOND
	case t == "BINARY_FLOAT":
		return func(any) int64 { return 4 }
	case t == "BINARY_DOUBLE", t == "FLOAT":
		return func(any) int64 { return 8 }
	default:
		// NUMBER and variable-length types (VARCHAR2, CHAR, CLOB, RAW, BLOB, XMLTYPE, …).
		return oracleValueBytes
	}
}

// oracleValueBytes sizes NUMBER and all variable-length types by the Go value type/length.
// NUMBER shares a single type name but go-ora resolves its precision/scale to a concrete
// Go type at scan time, so we size the value that actually came back.
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
