// Package typeutils holds the shared type-conversion helper used by both the olake product
// (which re-exports it) and the tests tree.
package typeutils

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// ReformatFloat64 converts a value to float64.
func ReformatFloat64(v interface{}) (float64, error) {
	switch v := v.(type) {
	case json.Number:
		return v.Float64()
	case []uint8:
		// Convert byte slice to string first
		strVal := string(v)
		f, err := strconv.ParseFloat(strVal, 64)
		if err != nil {
			return float64(0), fmt.Errorf("failed to change []byte %v to float64: %v", v, err)
		}
		return f, nil
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case int:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case bool:
		if v {
			return float64(1.0), nil
		}
		return 0.0, nil
	case string:
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return float64(0), fmt.Errorf("failed to change string %v to float64: %v", v, err)
		}
		return f, nil
	}

	return float64(0), fmt.Errorf("failed to change %v (type:%T) to float64", v, v)
}
