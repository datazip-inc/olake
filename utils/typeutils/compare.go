package typeutils

import (
	"fmt"
	"reflect"
	"strings"
	"time"
)

// return 0 for equal, -1 if a < b else 1 if a>b
func Compare(a, b any) int {
	// Handle nil cases first
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	switch aVal := a.(type) {
	case uint, uint8, uint16, uint32, uint64:
		aUint := reflect.ValueOf(a).Convert(reflect.TypeOf(uint64(0))).Uint()
		bUint := reflect.ValueOf(b).Convert(reflect.TypeOf(uint64(0))).Uint()
		if aUint < bUint {
			return -1
		} else if aUint > bUint {
			return 1
		}
		return 0
	case int, int8, int16, int32, int64:
		aInt := reflect.ValueOf(a).Convert(reflect.TypeOf(int64(0))).Int()
		bInt := reflect.ValueOf(b).Convert(reflect.TypeOf(int64(0))).Int()
		if aInt < bInt {
			return -1
		} else if aInt > bInt {
			return 1
		}
		return 0
	case float32, float64:
		aFloat := reflect.ValueOf(a).Convert(reflect.TypeOf(float64(0))).Float()
		bFloat := reflect.ValueOf(b).Convert(reflect.TypeOf(float64(0))).Float()
		if aFloat < bFloat {
			return -1
		} else if aFloat > bFloat {
			return 1
		}
		return 0
	case time.Time:
		var bTime time.Time
		if t, ok := b.(time.Time); ok {
			bTime = t
		} else if t, ok := b.(Time); ok {
			bTime = t.Time
		}

		if aVal.Before(bTime) {
			return -1
		} else if aVal.After(bTime) {
			return 1
		}
		return 0
	case bool:
		bBool := b.(bool)
		// false < true
		if !aVal && bBool {
			return -1
		} else if aVal && !bBool {
			return 1
		}
		return 0
	default:
		// check for custom timestamp
		aTime, aOk := a.(Time)
		bTime, bOk := b.(Time)

		if aOk && bOk {
			return aTime.Compare(bTime)
		}
		// For any other types, convert to string for comparison
		return strings.Compare(fmt.Sprintf("%v", a), fmt.Sprintf("%v", b))
	}
}
