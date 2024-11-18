package typeutils

import (
	"fmt"
	"reflect"
	"time"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
)

// TypeFromValue return [types.DataType] from v type
func TypeFromValue(v interface{}) types.DataType {
	if v == nil {
		return types.NULL
	}

	// Check if v is a pointer and get the underlying element type if it is
	valType := reflect.TypeOf(v)
	if valType.Kind() == reflect.Pointer {
		if valType.Elem() != nil {
			return TypeFromValue(reflect.ValueOf(v).Elem().Interface())
		}

		return types.NULL // Handle nil pointers
	}

	switch reflect.TypeOf(v).Kind() {
	case reflect.Invalid:
		return types.NULL
	case reflect.Bool:
		return types.BOOL
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return types.INT64
	case reflect.Float32, reflect.Float64:
		return types.FLOAT64
	case reflect.String:
		return types.STRING
	case reflect.Slice, reflect.Array:
		return types.ARRAY
	case reflect.Map:
		return types.OBJECT
	default:
		// Check if the type is time.Time for timestamp detection
		if reflect.TypeOf(v) == reflect.TypeOf(time.Time{}) {
			return detectTimestampPrecision(v.(time.Time))
		}

		return types.UNKNOWN
	}
}

func MaximumOnDataType[T any](typ types.DataType, a, b T) (T, error) {
	switch typ {
	case types.TIMESTAMP:
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
	case types.INT64:
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
		return types.TIMESTAMP
	}
	switch {
	case nanos%int(time.Millisecond) == 0:
		return types.TIMESTAMP_MILLI // store in milliseconds
	case nanos%int(time.Microsecond) == 0:
		return types.TIMESTAMP_MICRO // store in microseconds
	default:
		return types.TIMESTAMP_NANO // store in nanoseconds
	}
}
