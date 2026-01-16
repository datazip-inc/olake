package typeutils

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
)

var timeType = reflect.TypeOf(time.Time{})

func TypeFromValue(v interface{}) types.DataType {
	if v == nil {
		return types.Null
	}

	switch val := v.(type) {
	case bool:
		return types.Bool
	case int, int8, int16, int32, uint, uint8, uint16, uint32:
		return types.Int32
	case int64, uint64:
		return types.Int64
	case float32:
		return types.Float32
	case float64:
		return types.Float64
	case string:
		t, err := ReformatDate(v, false)
		if err == nil {
			return detectTimestampPrecision(t)
		}
		return types.String
	case []byte:
		return types.String
	case time.Time:
		return detectTimestampPrecision(val)
	case []any:
		return types.Array
	case map[string]any:
		return types.Object
	case *bool:
		if val == nil {
			return types.Null
		}
		return types.Bool
	case *int:
		if val == nil {
			return types.Null
		}
		return types.Int32
	case *int32:
		if val == nil {
			return types.Null
		}
		return types.Int32
	case *int64:
		if val == nil {
			return types.Null
		}
		return types.Int64
	case *float32:
		if val == nil {
			return types.Null
		}
		return types.Float32
	case *float64:
		if val == nil {
			return types.Null
		}
		return types.Float64
	case *string:
		if val == nil {
			return types.Null
		}
		t, err := ReformatDate(*val, false)
		if err == nil {
			return detectTimestampPrecision(t)
		}
		return types.String
	case *time.Time:
		if val == nil {
			return types.Null
		}
		return detectTimestampPrecision(*val)
	}

	return typeFromValueReflect(v)
}

// typeFromValueReflect handles types that require reflection
func typeFromValueReflect(v interface{}) types.DataType {
	valType := reflect.TypeOf(v)
	if valType == nil {
		return types.Null
	}
	// Handle pointers
	if valType.Kind() == reflect.Pointer {
		val := reflect.ValueOf(v)
		if val.IsNil() {
			return types.Null
		}
		return TypeFromValue(val.Elem().Interface())
	}

	// Handle json.Number type (when using json.Decoder with UseNumber())
	// in case of reflect, json.Number is detected as string so we need to handle it for integer and float
	if num, ok := v.(json.Number); ok {
		// If the number is an integer then -> int64
		if _, err := num.Int64(); err == nil {
			return types.Int64
		}
		return types.Float64
	}

	switch valType.Kind() {
	case reflect.Invalid:
		return types.Null
	case reflect.Bool:
		return types.Bool
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return types.Int32
	case reflect.Int64, reflect.Uint64:
		return types.Int64
	case reflect.Float32:
		return types.Float32
	case reflect.Float64:
		return types.Float64
	case reflect.String:
		// NOTE: If the string is in correct datetime format, it will be detected as timestamp and returned as timestamp datatype
		t, err := ReformatDate(v, false)
		if err == nil {
			return detectTimestampPrecision(t)
		}
		return types.String
	case reflect.Slice, reflect.Array:
		return types.Array
	case reflect.Map:
		return types.Object
	default:
		// Check if the type is time.Time for timestamp detection
		if valType == timeType {
			return detectTimestampPrecision(v.(time.Time))
		}
		return types.Unknown
	}
}

func MaximumOnDataType[T any](typ types.DataType, a, b T) (T, error) {
	switch typ {
	case types.Timestamp:
		adate, err := ReformatDate(a, true)
		if err != nil {
			return a, fmt.Errorf("failed to reformat[%v] while comparing: %s", a, err)
		}
		bdate, err := ReformatDate(b, true)
		if err != nil {
			return a, fmt.Errorf("failed to reformat[%v] while comparing: %s", b, err)
		}

		if utils.MaxDate(adate, bdate) == adate {
			return a, nil
		}

		return b, nil
	case types.Int64:
		aint, err := ReformatInt64(a)
		if err != nil {
			return a, fmt.Errorf("failed to reformat[%v] while comparing: %s", a, err)
		}

		bint, err := ReformatInt64(b)
		if err != nil {
			return a, fmt.Errorf("failed to reformat[%v] while comparing: %s", b, err)
		}

		if aint > bint {
			return a, nil
		}

		return b, nil
	default:
		return a, fmt.Errorf("comparison not available for data types %v now", typ)
	}
}

// Detect timestamp precision depending on time value
func detectTimestampPrecision(t time.Time) types.DataType {
	nanos := t.Nanosecond()
	if nanos == 0 { // if their is no nanosecond
		return types.Timestamp
	}
	switch {
	case nanos%int(time.Millisecond) == 0:
		return types.TimestampMilli // store in milliseconds
	case nanos%int(time.Microsecond) == 0:
		return types.TimestampMicro // store in microseconds
	default:
		return types.TimestampNano // store in nanoseconds
	}
}

func ExtractAndMapColumnType(columnType string, typeMapping map[string]types.DataType) types.DataType {
	// extracts the base type (e.g., varchar(50) -> varchar)
	baseType := strings.ToLower(strings.TrimSpace(strings.Split(columnType, "(")[0]))
	return typeMapping[baseType]
}
