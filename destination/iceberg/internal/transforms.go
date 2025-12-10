package internal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/murmur3"
)

// The current transform logic is limited to the data types handled by OLake.
// As OLake starts supporting more data types, we will update the transformations logic here.
//
// Supported Transforms:
//   - Identity, Void: 		All data types
//   - Bucket: 			int, long, string, timestamptz
//   - Truncate: int, long, string
//   - Year, Month, Day, Hour: timestamptz


const NULL = "null"

var transformPattern = regexp.MustCompile(`^([a-zA-Z]+)(?:\[(\d+)\])?$`)

func parseTransform(transform string) (base string, arg int, err error) {
	if transform == "" {
		return "", -1, errors.New("empty transform")
	}

	m := transformPattern.FindStringSubmatch(transform)
	if m == nil {
		return "", -1, errors.New("invalid transform")
	}

	base = strings.ToLower(m[1])
	if m[2] != "" {
		arg, err = strconv.Atoi(m[2])
		if err != nil {
			return "", -1, fmt.Errorf("invalid numeric argument in transform %v: %v", transform, err)
		}
	}

	return base, arg, nil
}

func hashUint64(u uint64) uint32 {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], u)
	return murmur3.Sum32(buf[:])
}

func hashString(s string) uint32 {
	return murmur3.Sum32([]byte(s))
}

func identityTransform(val any, colType string) (string, error) {
	switch colType {
	case "boolean":
		return strconv.FormatBool(val.(bool)), nil
	case "timestamptz":
		// Format as ISO-8601 with timezone offset to match Iceberg's format
		// This will be URL-encoded later: 2025-12-09T09:30:00+00:00 -> 2025-12-09T09%3A30%3A00%2B00%3A00
		t := val.(time.Time).UTC()
		return t.Format("2006-01-02T15:04:05-07:00"), nil
	default:
		return fmt.Sprintf("%v", val), nil
	}
}

func timeTransform(val any, unit string, colType string) (string, error) {
	if colType != "timestamptz" {
		return "", fmt.Errorf("unsupported time transform %q", unit)
	}

	v, _ := val.(time.Time)
	v = v.UTC()

	switch unit {
	case "year":
		return strconv.Itoa(v.Year()), nil
	case "month":
		return v.Format("2006-01"), nil
	case "day":
		return v.Format("2006-01-02"), nil
	case "hour":
		return v.Format("2006-01-02-15"), nil
	default:
		return "", fmt.Errorf("unsupported time transform %q", unit)
	}
}

func bucketTransform(val any, num int, colType string) (string, error) {
	if num <= 0 {
		return "", fmt.Errorf("invalid number of buckets: %d (must be > 0)", num)
	}

	var h uint32
	switch colType {
	case "int":
		switch v := val.(type) {
		case int32:
			h = hashUint64(uint64(int64(v)))
		// do we need to handle int64
		case int64:
			h = hashUint64(uint64(v))
		case int:
			h = hashUint64(uint64(v))
		default:
			return "", fmt.Errorf("expected integer for colType %q, got %T", colType, val)
		}
	case "long":
		switch v := val.(type) {
		case int64:
			h = hashUint64(uint64(v))
		case int:
			h = hashUint64(uint64(v))
		default:
			return "", fmt.Errorf("expected int64 for colType %q, got %T", colType, val)
		}
	case "timestamptz":
		tm, ok := val.(time.Time)
		if !ok {
			return "", fmt.Errorf("expected time.Time for colType %q, got %T", colType, val)
		}
		h = hashUint64(uint64(tm.UnixMicro()))
	case "string":
		str, ok := val.(string)
		if !ok {
			return "", fmt.Errorf("expected string for colType %q, got %T", colType, val)
		}
		h = hashString(str)
	default:
		return "", fmt.Errorf("unsupported colType %q for bucket transform", colType)
	}

	masked := int(h & 0x7FFFFFFF)
	bucket := masked % num
	return strconv.Itoa(bucket), nil
}

func truncateTransform(val any, n int, colType string) (string, error) {
	if n <= 0 {
		return "", fmt.Errorf("invalid truncate width: %d (must be > 0)", n)
	}

	switch colType {
	case "int":
		v, ok := val.(int32)
		if !ok {
			return "", fmt.Errorf("expected int32 for colType %q, got %T", colType, val)
		}
		n32 := int32(n)
		// Use Iceberg's formula for proper negative number handling
		trunc := v - (((v%n32)+n32)%n32)

		return fmt.Sprintf("%d", trunc), nil
	case "long":
		v, ok := val.(int64)
		if !ok {
			return "", fmt.Errorf("expected int64 for colType %q, got %T", colType, val)
		}
		n64 := int64(n)
		// Use Iceberg's formula for proper negative number handling
		trunc := v - (((v%n64)+n64)%n64)

		return fmt.Sprintf("%d", trunc), nil
	case "string":
		v, ok := val.(string)
		if !ok {
			return "", fmt.Errorf("expected string for colType %q, got %T", colType, val)
		}

		// Truncate by unicode code points, not bytes
		runes := []rune(v)
		if len(runes) <= n {
			return v, nil
		}
		return string(runes[:n]), nil
	default:
		return "", fmt.Errorf("unsupported colType %q for truncate transform", colType)
	}
}

func ConstructColPath(valueStr, field, transform string) string {
	base, _, _ := parseTransform(transform)

	// URL-encode both field name and value to match Iceberg's partition path format
	encodedField := url.QueryEscape(field)
	encodedValue := url.QueryEscape(valueStr)

	if base == "identity" {
		return fmt.Sprintf("%s=%s", encodedField, encodedValue)
	}

	switch base {
	case "bucket":
		return fmt.Sprintf("%s_bucket=%s", encodedField, encodedValue)
	case "truncate":
		return fmt.Sprintf("%s_trunc=%s", encodedField, encodedValue)
	default:
		return fmt.Sprintf("%s_%s=%s", encodedField, base, encodedValue)
	}
}

func TransformValue(val any, transform string, colType string) (any, error) {
	transform = strings.TrimSpace(strings.ToLower(transform))
	if val == nil {
		return NULL, nil
	}

	base, arg, err := parseTransform(transform)
	if err != nil {
		return nil, err
	}

	switch base {
	case "identity":
		return identityTransform(val, colType)
	case "void":
		return NULL, nil
	case "year", "month", "day", "hour":
		return timeTransform(val, base, colType)
	case "bucket":
		return bucketTransform(val, arg, colType)
	case "truncate":
		return truncateTransform(val, arg, colType)
	default:
		return nil, fmt.Errorf("unknown partition transform %q", transform)
	}
}
