package typeutils

import (
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/datazip-inc/olake/types"
)

type StringInterface interface {
	String() string
}

var DateTimeFormats = []string{
	"2006-01-02",
	"2006-01-02 15:04:05",
	"2006-01-02 15:04:05 -07:00",
	"2006-01-02 15:04:05-07:00",
	"2006-01-02T15:04:05",
	"2006-01-02T15:04:05.000000",
	"2006-01-02T15:04:05.999999999Z07:00",
	"2006-01-02T15:04:05+0000",
	"2020-08-17T05:50:22.895Z",
	"2006-01-02 15:04:05.999999-07",
	"2006-01-02 15:04:05.999999+00",
}

func getFirstNotNullType(datatypes []types.DataType) types.DataType {
	for _, datatype := range datatypes {
		if datatype != types.NULL {
			return datatype
		}
	}

	return types.NULL
}

func ReformatRecord(fields Fields, record types.Record) error {
	for key, val := range record {
		field, found := fields[key]
		if !found {
			return fmt.Errorf("missing field [%s]", key)
		}

		updated, err := ReformatValue(field.getType(), val)
		if err != nil {
			return fmt.Errorf("failed to reformat value[%s] to datatype[%s] for key[%s]: %s", val, field.getType(), key, err)
		}
		record[key] = updated
	}

	return nil
}

func ReformatValueOnDataTypes(datatypes []types.DataType, v any) (any, error) {
	return ReformatValue(getFirstNotNullType(datatypes), v)
}

func ReformatValue(dataType types.DataType, v any) (any, error) {
	switch dataType {
	case types.NULL:
		return nil, nil
	case types.BOOL:
		switch booleanValue := v.(type) {
		case bool:
			return booleanValue, nil
		case string:
			switch booleanValue {
			case "1", "t", "T", "true", "TRUE", "True", "YES", "Yes", "yes":
				return true, nil
			case "0", "f", "F", "false", "FALSE", "False", "NO", "No", "no":
				return false, nil
			}
		case int, int16, int32, int64, int8:
			switch booleanValue {
			case 1:
				return true, nil
			case 0:
				return true, nil
			default:
				return nil, fmt.Errorf("found to be boolean, but value is not boolean : %v", v)
			}
		default:
			return nil, fmt.Errorf("found to be boolean, but value is not boolean : %v", v)
		}

		return nil, fmt.Errorf("found to be boolean, but value is not boolean : %v", v)
	case types.INT64:
		return ReformatInt64(v)
	case types.TIMESTAMP:
		return ReformatDate(v)
	case types.STRING:
		switch v := v.(type) {
		case int, int8, int16, int32, int64:
			return fmt.Sprintf("%d", v), nil
		case uint, uint8, uint16, uint32, uint64:
			return fmt.Sprintf("%d", v), nil
		case float32, float64:
			return fmt.Sprintf("%d", v), nil
		case string:
			return v, nil
		case bool:
			return fmt.Sprintf("%t", v), nil
		case []byte: // byte slice
			return string(v), nil
		default:
			return fmt.Sprintf("%v", v), nil
		}
	case types.FLOAT64:
		return ReformatFloat64(v)
	case types.ARRAY:
		if value, isArray := v.([]any); isArray {
			return value, nil
		}

		// make it an array
		return []any{v}, nil
	default:
		return v, nil
	}
}

// reformat date
func ReformatDate(v interface{}) (time.Time, error) {
	parsed, err := func() (time.Time, error) {
		switch v := v.(type) {
		// we assume int64 is in seconds and don't currently scale to the precision
		case int64:
			return time.Unix(v, 0), nil
		case *int64:
			switch {
			case v != nil:
				return time.Unix(*v, 0), nil
			default:
				return time.Time{}, fmt.Errorf("null time passed")
			}
		case time.Time:
			return v, nil
		case *time.Time:
			switch {
			case v != nil:
				return *v, nil
			default:
				return time.Time{}, fmt.Errorf("null time passed")
			}
		case sql.NullTime:
			switch v.Valid {
			case true:
				return v.Time, nil
			default:
				return time.Time{}, fmt.Errorf("invalid null time")
			}
		case *sql.NullTime:
			switch v.Valid {
			case true:
				return v.Time, nil
			default:
				return time.Time{}, fmt.Errorf("invalid null time")
			}
		case nil:
			return time.Time{}, nil
		case string:
			return parseStringTimestamp(v)
		case *string:
			if v == nil || *v == "" {
				return time.Time{}, fmt.Errorf("empty string passed")
			} else {
				return parseStringTimestamp(*v)
			}
		case *any:
			return ReformatDate(*v)
		}
		return time.Time{}, fmt.Errorf("unhandled type[%T] passed: unable to parse into time", v)
	}()
	if err != nil {
		return time.Time{}, err
	}

	// manage year limit
	// even after data being parsed if year doesn't lie in range [0,9999] it failed to get marshalled
	if parsed.Year() < 0 {
		parsed = parsed.AddDate(0-parsed.Year(), 0, 0)
	} else if parsed.Year() > 9999 {
		parsed = parsed.AddDate(-(parsed.Year() - 9999), 0, 0)
	}

	return parsed, nil
}

func parseStringTimestamp(value string) (time.Time, error) {
	var tv time.Time
	var err error
	for _, layout := range DateTimeFormats {
		tv, err = time.Parse(layout, value)
		if err == nil {
			return time.Date(
				tv.Year(), tv.Month(), tv.Day(), tv.Hour(), tv.Minute(), tv.Second(), tv.Nanosecond(), tv.Location(),
			), nil
		}
	}

	return time.Time{}, fmt.Errorf("failed to parse datetime from available formats: %s", err)
}

func ReformatInt64(v any) (int64, error) {
	switch v := v.(type) {
	case float32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case int:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return int64(v), nil
	case uint:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		return int64(v), nil
	case bool:
		return 1, nil
	case *any:
		return ReformatInt64(*v)
	}

	return int64(0), fmt.Errorf("failed to change %v (type:%T) to int64", v, v)
}

func ReformatFloat64(v interface{}) (interface{}, error) {
	switch v := v.(type) {
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
		return 1.0, nil
	case string:
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return float64(0), fmt.Errorf("failed to change string %v to float64: %w", v, err)
		}
		return f, nil
	}

	return float64(0), fmt.Errorf("failed to change %v (type:%T) to float64", v, v)
}

func ReformatByteArraysToString(data map[string]any) map[string]any {
	for key, value := range data {
		switch value := value.(type) {
		case map[string]any:
			data[key] = ReformatByteArraysToString(value)
		case []byte:
			data[key] = string(value)
		case []map[string]any:
			decryptedArray := []map[string]any{}
			for _, element := range value {
				decryptedArray = append(decryptedArray, ReformatByteArraysToString(element))
			}

			data[key] = decryptedArray
		case []any:
			decryptedArray := []any{}
			for _, element := range value {
				switch element := element.(type) {
				case map[string]any:
					decryptedArray = append(decryptedArray, ReformatByteArraysToString(element))
				case []byte:
					decryptedArray = append(decryptedArray, string(element))
				default:
					decryptedArray = append(decryptedArray, element)
				}
			}

			data[key] = decryptedArray
		}
	}
	return data
}
