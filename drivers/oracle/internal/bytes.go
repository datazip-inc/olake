package driver

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// oracleColumnBytes returns the Oracle on-disk byte count for a single column value.
//
// Fixed-width types use their Oracle storage sizes:
//   - DATE = 7 bytes (century, year, month, day, hour, minute, second)
//   - TIMESTAMP(*) = 11 bytes max (7 date bytes + 4 fractional-second bytes)
//   - BINARY_FLOAT = 4, BINARY_DOUBLE/FLOAT = 8
//
// For NUMBER: go-ora resolves precision/scale before calling our converter, so
// the raw scan value already carries the correct Go type (int32=4, int64=8,
// float64=8). We use the Go type size directly.
//
// Variable types (VARCHAR2, NVARCHAR2, CHAR, CLOB, RAW, BLOB, XMLTYPE, etc.)
// use the actual byte length of the Go value (len(string) or len([]byte)).
//
// NULL values return 0 — Oracle stores no bytes for NULL columns.
func oracleColumnBytes(rawVal any, typeName string) int64 {
	if rawVal == nil {
		return 0
	}
	t := strings.ToUpper(strings.TrimSpace(typeName))
	// Strip precision/scale suffix: "NUMBER(10,2)" → "NUMBER", "TIMESTAMP(6)" → "TIMESTAMP"
	if idx := strings.IndexByte(t, '('); idx >= 0 {
		t = strings.TrimSpace(t[:idx])
	}

	switch {
	case t == "DATE":
		return 7
	case strings.HasPrefix(t, "TIMESTAMP"):
		return 11 // TIMESTAMP(9) max = 7 date + 4 nanosecond bytes
	case strings.HasPrefix(t, "INTERVAL YEAR") || t == "INTERVALYM_DTY":
		return 5 // INTERVAL YEAR TO MONTH = 5 bytes
	case strings.HasPrefix(t, "INTERVAL DAY") || t == "INTERVALDS_DTY":
		return 11 // INTERVAL DAY TO SECOND = 11 bytes
	case t == "BINARY_FLOAT":
		return 4
	case t == "BINARY_DOUBLE", t == "FLOAT":
		return 8
	}

	// For NUMBER and all variable-length types, use the Go value type/length.
	// go-ora returns:
	//   NUMBER(p,0) p≤9  → int32  (4 bytes)
	//   NUMBER(p,0) p≤18 → int64  (8 bytes)
	//   NUMBER with scale → float64 (8 bytes)
	//   VARCHAR2/CHAR/CLOB/NCLOB/LONG → string
	//   RAW/LONG RAW/BLOB → []byte
	//   XMLTYPE → string
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

// oracleRowBytes sums column bytes for a complete row scanned via database/sql.
func oracleRowBytes(vals []any, colTypes []*sql.ColumnType) int64 {
	var total int64
	for i, v := range vals {
		total += oracleColumnBytes(v, colTypes[i].DatabaseTypeName())
	}
	return total
}

// addRowBytes is the MapScan/MapScanConcurrent callback for backfill and incremental.
func (o *Oracle) addRowBytes(vals []any, colTypes []*sql.ColumnType) {
	o.bytesRead.Add(oracleRowBytes(vals, colTypes))
}
