package driver

import (
	"database/sql"
	"fmt"
	"strings"
	"sync/atomic"
)

// pgNumericBinaryBytes computes the number of bytes PostgreSQL uses to store a
// NUMERIC value on disk, given its text representation (as returned by pgx).
//
// PostgreSQL NUMERIC on-disk format (short varlena path, used for almost all values):
//
//	1 byte  – short varlena header
//	2 bytes – n_header (encodes ndigits, sign, weight, dscale)
//	2 bytes × ndigits
//
// where ndigits = ceil(integer_decimal_digits / 4) + ceil(fractional_decimal_digits / 4)
// after stripping leading zeros from the integer part and trailing zeros from the
// fractional part (PostgreSQL does not store unnecessary trailing zero groups).
func pgNumericBinaryBytes(s string) int64 {
	if s == "" {
		return 3 // degenerate / empty
	}
	// Strip optional sign
	if s[0] == '+' || s[0] == '-' {
		s = s[1:]
	}
	// Special values: NaN, Infinity
	sl := strings.ToLower(s)
	if sl == "nan" || sl == "infinity" || sl == "inf" || sl == "-infinity" {
		return 3 // 1-byte header + 2-byte n_header, 0 digits
	}

	var intPart, fracPart string
	if dot := strings.IndexByte(s, '.'); dot >= 0 {
		intPart = s[:dot]
		fracPart = s[dot+1:]
	} else {
		intPart = s
	}

	// Leading zeros in the integer part do not produce extra base-10000 digits.
	intPart = strings.TrimLeft(intPart, "0")
	// Trailing zeros in the fractional part are not stored as extra base-10000 digits
	// (they are captured in dscale, not in the digit array).
	fracPart = strings.TrimRight(fracPart, "0")

	// Number of base-10000 digits required for each part.
	intDigits := (len(intPart) + 3) / 4   // ceiling division
	fracDigits := (len(fracPart) + 3) / 4 // ceiling division
	ndigits := int64(intDigits + fracDigits)

	// 1-byte short-varlena header + 2-byte n_header + 2 bytes per digit.
	// This short form is used whenever the total stored bytes ≤ 127, which is
	// true for all realistic NUMERIC values (would need > 62 base-10000 digits
	// = 248+ decimal digits to require the long form).
	return 3 + 2*ndigits
}

// pgStorageBytes returns the number of bytes PostgreSQL uses to store the given
// column value on disk — matching what pg_column_size() returns for individual
// columns.
//
// Fixed-width types always use the same number of bytes regardless of value:
//
//	INT2 / SMALLINT                   → 2 bytes
//	INT4 / INTEGER                    → 4 bytes
//	INT8 / BIGINT                     → 8 bytes
//	FLOAT4 / REAL                     → 4 bytes
//	FLOAT8 / DOUBLE PRECISION         → 8 bytes
//	BOOL                              → 1 byte
//	DATE                              → 4 bytes
//	TIME / TIMETZ                     → 8 bytes
//	TIMESTAMP / TIMESTAMPTZ           → 8 bytes
//	UUID                              → 16 bytes
//	OID                               → 4 bytes
//	MONEY                             → 8 bytes
//
// NUMERIC / DECIMAL use PostgreSQL's variable-length base-10000 digit encoding.
//
// All other variable-width types (VARCHAR, TEXT, BYTEA, JSON, JSONB, arrays …)
// include a varlena header that PostgreSQL prepends on disk:
//
//	1-byte header for values ≤ 127 bytes total (short varlena)
//	4-byte header for longer values
//
// NULL values return 0 (matching coalesce(pg_column_size(null), 0)).
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
		// NUMERIC uses PostgreSQL's variable-length base-10000 digit encoding.
		// pgx scans NUMERIC into a string via database/sql.
		var s string
		switch v := rawVal.(type) {
		case string:
			s = v
		case []byte:
			s = string(v)
		default:
			s = fmt.Sprintf("%v", rawVal)
		}
		return pgNumericBinaryBytes(s)
	default:
		// Variable-width: VARCHAR, BPCHAR, TEXT, BYTEA, JSON, JSONB, arrays, ranges, etc.
		var dataLen int64
		switch v := rawVal.(type) {
		case string:
			dataLen = int64(len(v))
		case []byte:
			dataLen = int64(len(v))
		default:
			dataLen = int64(len(fmt.Sprintf("%v", v)))
		}
		// PostgreSQL uses a 1-byte varlena header for total stored size ≤ 127 bytes,
		// and a 4-byte header for longer values.
		if dataLen < 127 {
			return dataLen + 1
		}
		return dataLen + 4
	}
}

// pgColAlignment returns the alignment boundary (in bytes) PostgreSQL uses when
// laying out a column value inside a heap tuple — needed to reproduce the
// inter-column padding that pg_column_size(row.*) includes.
//
//	8-byte aligned: INT8, FLOAT8, TIMESTAMP, TIMESTAMPTZ, TIME, TIMETZ, MONEY, INTERVAL
//	4-byte aligned: INT4, FLOAT4, DATE, OID
//	               + long varlena (storedBytes > 127)
//	2-byte aligned: INT2
//	1-byte aligned: BOOL + short varlena (storedBytes ≤ 127), NUMERIC
func pgColAlignment(colType *sql.ColumnType, storedBytes int64) int64 {
	switch strings.ToUpper(colType.DatabaseTypeName()) {
	case "INT8", "BIGINT", "BIGSERIAL",
		"FLOAT8", "FLOAT", "DOUBLE PRECISION",
		"TIMESTAMP", "TIMESTAMPTZ",
		"TIME", "TIMETZ",
		"MONEY", "INTERVAL":
		return 8
	case "INT4", "INT", "INTEGER", "SERIAL",
		"FLOAT4", "REAL",
		"DATE", "OID":
		return 4
	case "INT2", "SMALLINT", "SMALLSERIAL":
		return 2
	default:
		// Variable-length types.
		// Short varlena (≤ 127 total bytes) → 1-byte aligned.
		// Long varlena (> 127 total bytes)  → 4-byte aligned.
		// BOOL (1 byte) falls here too, and storedBytes=1 ≤ 127, so alignment=1 ✓.
		if storedBytes <= 127 {
			return 1
		}
		return 4
	}
}

// pgCompositeRowBytes returns the total number of bytes that pg_column_size(row.*)
// would report for a complete row.  It reproduces the PostgreSQL HeapTuple layout:
//
//  1. HeapTupleHeader  (23 bytes for no-NULL rows; 23 + ⌈natts/8⌉ bytes when any
//     column is NULL), padded to the next MAXALIGN(8) boundary → t_hoff.
//  2. Column data laid out with inter-column alignment padding exactly as
//     PostgreSQL's heap_compute_data_size() does.
//
// The sum is effectively what you get from:
//
//	SELECT sum(pg_column_size(t.*)) FROM tbl t
func pgCompositeRowBytes(vals []any, colTypes []*sql.ColumnType) int64 {
	n := len(vals)

	// ── 1. HeapTupleHeader ──────────────────────────────────────────────────
	hasNull := false
	for _, v := range vals {
		if v == nil {
			hasNull = true
			break
		}
	}

	headerSize := 23 // minimal HeapTupleHeaderData
	if hasNull {
		headerSize += (n + 7) / 8 // null bitmap: 1 bit per attribute, rounded up
	}
	// MAXALIGN to 8 bytes (64-bit PostgreSQL builds)
	tHoff := int64((headerSize + 7) &^ 7)

	// ── 2. Column data with inter-column alignment padding ──────────────────
	dataOffset := int64(0)
	for i, v := range vals {
		if v == nil {
			continue // NULL columns occupy no data space
		}

		colBytes := pgStorageBytes(v, colTypes[i])
		align := pgColAlignment(colTypes[i], colBytes)

		// Pad dataOffset up to the column's alignment boundary
		if align > 1 {
			if rem := dataOffset % align; rem != 0 {
				dataOffset += align - rem
			}
		}
		dataOffset += colBytes
	}

	return tHoff + dataOffset
}

// makeLocalAddRowBytes returns a MapScanConcurrent/MapScan callback that
// accumulates bytes into a caller-owned local int64.
// The local variable lives on the ChunkIterator/StreamIncrementalChanges call
// stack, so it resets naturally on every retry. The caller returns the final
// value, which handleWriterCleanup passes to WriterThread.Close so it is
// committed to Stats.BytesCommitted only after a successful destination commit.
func makeLocalAddRowBytes(local *int64) func([]any, []*sql.ColumnType) {
	return func(vals []any, colTypes []*sql.ColumnType) {
		// atomic because MapScanConcurrent calls this from the producer goroutine
		// while the consumer goroutine is running concurrently.
		atomic.AddInt64(local, pgCompositeRowBytes(vals, colTypes))
	}
}
