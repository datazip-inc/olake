package typeutils

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
)

func TypeFromValue(v interface{}) types.DataType {
	if v == nil {
		return types.Null
	}

	// Check if v is a pointer and get the underlying element type if it is
	valType := reflect.TypeOf(v)
	if valType.Kind() == reflect.Pointer {
		val := reflect.ValueOf(v)
		if val.IsNil() {
			return types.Null
		}
		return TypeFromValue(val.Elem().Interface())
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
		t, err := ReformatDate(v)
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
		if valType == reflect.TypeOf(time.Time{}) {
			return detectTimestampPrecision(v.(time.Time))
		}

		return types.Unknown
	}
}

func MaximumOnDataType[T any](typ types.DataType, a, b T) (T, error) {
	switch typ {
	case types.Timestamp:
		adate, err := ReformatDate(a)
		if err != nil {
			return a, fmt.Errorf("failed to reformat[%v] while comparing: %s", a, err)
		}
		bdate, err := ReformatDate(b)
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
	baseType := strings.ToLower(strings.TrimSpace(strings.Split(columnType, "(")[0]))

	// Check if the key exists in the map
	if val, ok := typeMapping[baseType]; ok {
		return val
	}

	// If not found, explicitly return Unknown instead of an empty string
	return types.Unknown
}
