package driver

import (
	"fmt"
	"time"
)

// recordDataBytes returns the approximate UNCOMPRESSED size (in bytes) of a parsed
// record's data — the sum of its value sizes. it measures the actual decompressed data volume of the record, NOT
// the compressed S3 object size, so the metric reflects the data actually read and materialized rather than the file's on-disk compressed size.
//
// Keys (column names) are excluded — they are schema, not data
func recordDataBytes(record map[string]any) int64 {
	return valueBytes(record)
}

// valueBytes returns the in-memory data size of a single decoded value. Fixed-width
// types use their natural width; variable-width types (string, []byte) use their
// actual length; nested maps/slices are traversed iteratively to prevent deep recursion.
func valueBytes(v any) int64 {
	var total int64
	var stackBuf [32]any
	stack := append(stackBuf[:0], v)

	for len(stack) > 0 {
		lastIdx := len(stack) - 1
		curr := stack[lastIdx]
		stack = stack[:lastIdx]

		switch x := curr.(type) {
		case nil:
		case string:
			total += int64(len(x))
		case []byte:
			total += int64(len(x))
		case bool, int8, uint8:
			total += 1
		case int16, uint16:
			total += 2
		case int32, uint32, float32:
			total += 4
		case int, int64, uint, uint64, float64:
			total += 8
		case time.Time:
			total += 8
		case map[string]any:
			for _, e := range x {
				stack = append(stack, e)
			}
		case []any:
			for _, e := range x {
				stack = append(stack, e)
			}
		default:
			total += int64(len(fmt.Sprintf("%v", x)))
		}
	}

	return total
}
