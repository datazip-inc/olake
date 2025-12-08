package internal

import (
	"encoding/binary"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/twmb/murmur3"
)

var (
	NULL    = "null"
	epochTM = time.Unix(0, 0).UTC()
)

// The current transforms logic is limited to the data types handled by OLake
// As we start handling more data types, we will update the transformations logic here

func identityTransform(val any, colType string) (string, error) {
	switch v := val.(type) {
	case bool:
		return strconv.FormatBool(v), nil
	default:
		switch colType {
		case "timestamptz":
			switch t := val.(type) {
			case time.Time:
				return t.Format("2006-01-02T15:04:05.999999"), nil
			default:
				return "", fmt.Errorf("cannot apply identity transform for col: %v", colType)
			}
		}

		return fmt.Sprintf("%v", v), nil
	}
}

func timeTransform(val any, transform string, colType string) (string, error) {
	if colType != "timestamptz" {
		return "", fmt.Errorf("cannot apply %v transform to a %v column", transform, colType)
	}

	t := val.(time.Time).UTC()

	switch transform {
	case "year":
		return fmt.Sprintf("%d", t.Year()), nil
	case "month":
		return t.Format("2006-01"), nil
	case "day":
		return t.Format("2006-01-02"), nil
	case "hour":
		return t.Format("2006-01-02-15"), nil
	}

	return "", nil
}

func hashFunction[T ~int32 | ~int64](v any) uint32 {
	var (
		val = uint64(v.(T))
		buf [8]byte
		b   = buf[:]
	)

	binary.LittleEndian.PutUint64(b, val)

	return murmur3.Sum32(b)
}

func bucketTransform(val any, num int, colType string) (string, error) {
	var hash uint32

	switch colType {
	case "int", "long":
		switch v := val.(type) {
		case int32:
			hash = hashFunction[int64](int64(v))
		case int64:
			hash = hashFunction[int64](v)
		default:
			return "", fmt.Errorf("unsupported value type %T for colType %s", val, colType)
		}
	case "timestamptz":
		tm, ok := val.(time.Time)
		if !ok {
			return "", fmt.Errorf("expected time.Time for colType %s, got %T", colType, val)
		}
		hash = hashFunction[int64](tm.UnixMicro())

	case "string":
		str, ok := val.(string)
		if !ok {
			return "", fmt.Errorf("expected string for colType %s, got %T", colType, val)
		}
		hash = murmur3.Sum32(unsafe.Slice(unsafe.StringData(str), len(str)))

	default:
		return "", fmt.Errorf("cannot apply bucket transformation for colType %s", colType)
	}

	bucketValue := int32(hash&0x7FFFFFFF) % int32(num)

	return fmt.Sprintf("%v", bucketValue), nil
}

func truncateTransform(val any, num int, colType string) (string, error) {
	switch colType {
	case "int":
		v := val.(int32)
		tval := v - (v % int32(num))
		return fmt.Sprintf("%v", tval), nil
	case "long":
		v := val.(int64)
		tVal := v - (v % int64(num))
		return fmt.Sprintf("%v", tVal), nil
	case "string":
		v := val.(string)
		tVal := v[:min(len(v), num)]
		return fmt.Sprintf("%v", tVal), nil
	default:
		return "", fmt.Errorf("cannot apply truncate transformation for col: %v", colType)
	}
}

func ConstructColPath(tVal, field, transform string) string {
	if transform == "identity" {
		return fmt.Sprintf("%s=%s", field, tVal)
	}

	re := regexp.MustCompile(`^([a-zA-Z]+)(\[\d+\])?$`)
	matches := re.FindStringSubmatch(transform)
	if len(matches) == 0 {
		return fmt.Sprintf("%s_%s=%s", field, transform, tVal)
	}

	base := strings.ToLower(matches[1])

	switch base {
	case "bucket":
		return fmt.Sprintf("%s_bucket=%s", field, tVal)
	case "truncate":
		return fmt.Sprintf("%s_trunc=%s", field, tVal)
	default:
		return fmt.Sprintf("%s_%s=%s", field, base, tVal)
	}
}

func TransformValue(val any, transform string, colType string) (any, error) {
	var value any
	if val == nil {
		return NULL, nil
	}

	if strings.HasPrefix(transform, "bucket") {
		num, err := strconv.Atoi(transform[7 : len(transform)-1])
		if err != nil {
			return nil, err
		}

		value, err = bucketTransform(val, num, colType)
		if err != nil {
			return nil, err
		}
	} else if strings.HasPrefix(transform, "truncate") {
		num, err := strconv.Atoi(transform[9 : len(transform)-1])
		if err != nil {
			return nil, err
		}

		value, err = truncateTransform(val, num, colType)
		if err != nil {
			return nil, err
		}
	} else {
		switch transform {
		case "identity":
			return identityTransform(val, colType)
		case "year", "month", "day", "hour":
			return timeTransform(val, transform, colType)
		case "void":
			return NULL, nil
		default:
			return nil, fmt.Errorf("unknown iceberg partition transformation: %v", transform)
		}
	}

	return value, nil
}
