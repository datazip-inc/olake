package typeutils

import (
	"math"
	"time"

	"github.com/datazip-inc/olake/types"
)

const (
	maxInt64 = int64(1<<63 - 1)
	minInt64 = int64(-1 << 63)
)

type JSONTypeTrack struct {
	String    bool
	Boolean   bool
	Int64     bool
	Float64   bool
	Timestamp bool
	Other     bool
}

func DetectJSONType(v interface{}, stats *JSONTypeTrack) {
	switch val := v.(type) {
	case nil:
		return
	case bool:
		stats.Boolean = true
	case int, int8, int16, int32, int64:
		stats.Int64 = true
	case float64: // JSON numbers always decode as float64
		if math.IsNaN(val) || math.IsInf(val, 0) {
			stats.Other = true
			return
		}

		if val == math.Trunc(val) {
			if val >= float64(minInt64) && val <= float64(maxInt64) {
				stats.Int64 = true
			} else {
				// out of int64 range, must be float64
				stats.Float64 = true
			}
		} else {
			stats.Float64 = true
		}
	case string:
		if isTimestampString(val) {
			stats.Timestamp = true
		} else {
			stats.String = true
		}
	case time.Time:
		stats.Timestamp = true
	case map[string]interface{}, []interface{}:
		stats.Other = true
	default:
		stats.String = true
	}
}

func InferJSONType(valType JSONTypeTrack) types.DataType {
	if valType.Other {
		return types.String
	}

	typeCount := 0
	switch {
	case valType.Boolean:
		typeCount++

	case valType.Int64:
		typeCount++

	case valType.Float64:
		typeCount++

	case valType.Timestamp:
		typeCount++

	case valType.String:
		typeCount++
	}

	// if got more than one type, use string
	if typeCount > 1 {
		return types.String
	}

	switch {
	case valType.Boolean:
		return types.Bool

	case valType.Float64:
		return types.Float64

	case valType.Int64:
		return types.Int64

	case valType.Timestamp:
		return types.Timestamp

	case valType.String:
		return types.String
	}

	return types.String
}

// if string is in timestamp format
func isTimestampString(s string) bool {
	_, err := parseStringTimestamp(s)
	return err == nil
}
