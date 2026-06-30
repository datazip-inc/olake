package driver

import (
	"fmt"
	"time"
)

// recordDataBytes returns the approximate UNCOMPRESSED size (in bytes) of a parsed
// record's data — the sum of its value sizes. This is the s3 analogue of the
// per-row source size other drivers report (postgres pg_column_size, mongo BSON
// doc size): it measures the actual decompressed data volume of the record, NOT
// the compressed S3 object size. Billing on this reflects the data actually read
// and materialized (the memory/compute cost), instead of under-billing by the
// file's on-disk compressed size.
//
// Keys (column names) are excluded — they are schema, not data — matching how the
// SQL drivers size a row (pg_column_size sums column values, not column names).
func recordDataBytes(record map[string]any) int64 {
	var total int64
	for _, v := range record {
		total += valueBytes(v)
	}
	return total
}

// valueBytes returns the in-memory data size of a single decoded value. Fixed-width
// types use their natural width; variable-width types (string, []byte) use their
// actual length; nested maps/slices recurse.
func valueBytes(v any) int64 {
	switch x := v.(type) {
	case nil:
		return 0
	case string:
		return int64(len(x))
	case []byte:
		return int64(len(x))
	case bool, int8, uint8:
		return 1
	case int16, uint16:
		return 2
	case int32, uint32, float32:
		return 4
	case int, int64, uint, uint64, float64:
		return 8
	case time.Time:
		return 8
	case map[string]any:
		var total int64
		for _, e := range x {
			total += valueBytes(e)
		}
		return total
	case []any:
		var total int64
		for _, e := range x {
			total += valueBytes(e)
		}
		return total
	default:
		// Fallback for uncommon types: textual length is a reasonable proxy.
		return int64(len(fmt.Sprintf("%v", x)))
	}
}
