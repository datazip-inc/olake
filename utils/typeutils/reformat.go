package typeutils

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/paulmach/orb/encoding/wkb"
	"github.com/paulmach/orb/encoding/wkt"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type StringInterface interface {
	String() string
}

var (
	ErrNullValue = fmt.Errorf("null value")
)

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

var GeospatialTypes = []string{"geometry", "point", "polygon", "linestring", "multi"}

func getFirstNotNullType(datatypes []types.DataType) types.DataType {
	for _, datatype := range datatypes {
		if datatype != types.Null {
			return datatype
		}
	}

	return types.Null
}

func ReformatRecord(fields Fields, record types.Record) error {
	for key, val := range record {
		field, found := fields[key]
		if !found {
			return fmt.Errorf("missing field [%s]", key)
		}
		updated, err := ReformatValue(field.getType(), val)
		if err != nil && err != ErrNullValue {
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
	if v == nil {
		return v, nil
	}

	// Handle json.Number type
	if num, ok := v.(json.Number); ok {
		switch dataType {
		case types.Int32, types.Int64:
			intVal, err := num.Int64()
			if err != nil {
				return nil, fmt.Errorf("failed to parse integer: %v", err)
			}
			if dataType == types.Int32 {
				//nolint:gosec,G115
				return int32(intVal), nil
			}
			return intVal, nil
		case types.Float32, types.Float64:
			floatVal, err := num.Float64()
			if err != nil {
				return nil, fmt.Errorf("failed to parse float: %v", err)
			}
			if dataType == types.Float32 {
				return float32(floatVal), nil
			}
			return floatVal, nil
		default:
			v = num.String()
		}
	}

	switch dataType {
	case types.Null:
		return nil, ErrNullValue
	case types.Bool:
		return ReformatBool(v)
	case types.Int64:
		return ReformatInt64(v)
	case types.Int32:
		return ReformatInt32(v)
	case types.Timestamp, types.TimestampMilli, types.TimestampMicro, types.TimestampNano:
		return ReformatDate(v)
	case types.String:
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
	case types.Float32:
		return ReformatFloat32(v)
	case types.Float64:
		return ReformatFloat64(v)
	case types.Array:
		if value, isArray := v.([]any); isArray {
			return value, nil
		}

		// make it an array
		return []any{v}, nil
	default:
		return v, nil
	}
}

func ReformatBool(v interface{}) (bool, error) {
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
			return false, nil
		default:
			return false, fmt.Errorf("found to be boolean, but value is not boolean : %v", v)
		}
	default:
		return false, fmt.Errorf("found to be boolean, but value is not boolean : %v", v)
	}

	return false, fmt.Errorf("found to be boolean, but value is not boolean : %v", v)
}

// reformat date
func ReformatDate(v interface{}) (time.Time, error) {
	parsed, err := func() (time.Time, error) {
		switch v := v.(type) {
		case []uint8:
			strVal := string(v)
			return parseStringTimestamp(strVal)
		case []int8:
			b := make([]byte, 0, len(v))
			for _, i := range v {
				b = append(b, byte(i))
			}
			strVal := string(b)
			return parseStringTimestamp(strVal)
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
			}
			return parseStringTimestamp(*v)
		case primitive.DateTime:
			return v.Time(), nil
		case *any:
			return ReformatDate(*v)
		}
		return time.Time{}, fmt.Errorf("unhandled type[%T] passed: unable to parse into time", v)
	}()
	if err != nil {
		return time.Time{}, err
	}

	// manage year limit
	// even after data being parsed if year doesn't lie in range [0,9999] it failed to get marshaled
	// Check if year is 0000 (not supported by Spark)
	// Spark only supports years from 1 to 9999, we are converting year 0000 to epoch start time
	if parsed.Year() < 1 {
		logger.Debugf("Detected invalid year %d (year 0000 or negative). Converting to epoch start time (1970-01-01 00:00:00 UTC)", parsed.Year())
		parsed = time.Unix(0, 0).UTC()
	} else if parsed.Year() > 9999 {
		parsed = parsed.AddDate(-(parsed.Year() - 9999), 0, 0)
	}

	return parsed, nil
}

func parseStringTimestamp(value string) (time.Time, error) {
	// Check if the string starts with a date pattern (YYYY-MM-DD)
	startsWithDatePattern := func(value string) bool {
		if len(value) < 10 {
			return false
		}

		datePart := value[:10]
		parts := strings.Split(datePart, "-")
		if len(parts) != 3 {
			return false
		}

		for _, part := range parts {
			if len(part) < 1 || len(part) > 4 {
				return false
			}
			for _, char := range part {
				if char < '0' || char > '9' {
					return false
				}
			}
		}

		return true
	}

	if !startsWithDatePattern(value) {
		return time.Time{}, fmt.Errorf("string does not start with date pattern (YYYY-MM-DD)")
	}

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

	logger.Warnf("Invalid datetime detected, failed to parse: %s. Converting to epoch start time (1970-01-01 00:00:00 UTC)", value)
	return time.Unix(0, 0).UTC(), nil
}

func ReformatInt64(v any) (int64, error) {
	switch v := v.(type) {
	case float32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return int64(v), nil
	case uint:
		//nolint:gosec,G115
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		//nolint:gosec,G115
		return int64(v), nil
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	case string:
		intValue, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return int64(0), fmt.Errorf("failed to change string %v to int64: %v", v, err)
		}
		return intValue, nil
	case *any:
		return ReformatInt64(*v)
	}

	return int64(0), fmt.Errorf("failed to change %v (type:%T) to int64", v, v)
}

func ReformatInt32(v any) (int32, error) {
	switch v := v.(type) {
	case float32:
		return int32(v), nil
	case float64:
		return int32(v), nil
	case int:
		//nolint:gosec,G115
		return int32(v), nil
	case int8:
		return int32(v), nil
	case int16:
		return int32(v), nil
	case int32:
		return v, nil
	case int64:
		//nolint:gosec,G115
		return int32(v), nil
	case uint:
		//nolint:gosec,G115
		return int32(v), nil
	case uint8:
		return int32(v), nil
	case uint16:
		return int32(v), nil
	case uint32:
		//nolint:gosec,G115
		return int32(v), nil
	case uint64:
		//nolint:gosec,G115
		return int32(v), nil
	case bool:
		if v {
			return int32(1), nil
		}
		return int32(0), nil
	case string:
		intValue, err := strconv.ParseInt(v, 10, 32)
		if err != nil {
			return 0, fmt.Errorf("failed to change string %v to int32: %v", v, err)
		}
		return int32(intValue), nil
	case []uint8:
		if len(v) == 1 {
			return int32(v[0]), nil
		}
		return 0, fmt.Errorf("unsupported []uint8 of length %d: %v", len(v), v)
	case *any:
		return ReformatInt32(*v)
	}

	return int32(0), fmt.Errorf("failed to change %v (type:%T) to int32", v, v)
}

func ReformatFloat64(v interface{}) (float64, error) {
	switch v := v.(type) {
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

func ReformatFloat32(v interface{}) (float32, error) {
	switch v := v.(type) {
	case []uint8:
		// Convert byte slice to string first
		strVal := string(v)
		f64, err := strconv.ParseFloat(strVal, 32)
		f := float32(f64)
		if err != nil {
			return float32(0), fmt.Errorf("failed to change []byte %v to float32: %v", v, err)
		}
		return f, nil
	case float32:
		return v, nil
	case float64:
		return float32(v), nil
	case int:
		return float32(v), nil
	case int8:
		return float32(v), nil
	case int16:
		return float32(v), nil
	case int32:
		return float32(v), nil
	case int64:
		return float32(v), nil
	case uint:
		return float32(v), nil
	case uint8:
		return float32(v), nil
	case uint16:
		return float32(v), nil
	case uint32:
		return float32(v), nil
	case uint64:
		return float32(v), nil
	case bool:
		if v {
			return float32(1.0), nil
		}
		return float32(0.0), nil
	case string:
		f64, err := strconv.ParseFloat(v, 32)
		if err != nil {
			return float32(0), fmt.Errorf("failed to change string %s to float32: %v", v, err)
		}
		return float32(f64), nil
	}

	return float32(0), fmt.Errorf("failed to change %v (type:%T) to float32", v, v)
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

func ReformatGeoType(v any) (any, error) {
	if v == nil {
		return nil, ErrNullValue
	}

	geoValue := func(b []byte) (any, error) {
		// skipping 4-byte SRID prefix (mysql stores 25-byte wkb including SRID)
		if len(b) > 4 {
			// Well Known Binary (WKB) unmarshal -> Well Known Text (WKT)
			if geom, err := wkb.Unmarshal(b[4:]); err == nil {
				if s := wkt.MarshalString(geom); s != "" {
					return s, nil
				}
			}
		}

		return fmt.Sprintf("%x", b), nil
	}

	switch vv := v.(type) {
	case string:
		// already textual WKT or similar
		return vv, nil
	case []uint8:
		return geoValue([]byte(vv))
	case *any:
		if vv == nil {
			return nil, ErrNullValue
		}
		return ReformatGeoType(*vv)
	default:
		return fmt.Sprintf("%v", v), nil
	}
}
