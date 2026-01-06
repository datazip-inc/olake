package arrowwriter

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/datazip-inc/olake/destination/iceberg/proto"

	"github.com/twmb/murmur3"
)

// The current transform logic is limited to the data types handled by OLake.
// As OLake starts supporting more data types, we will update the transformations logic here.
//
// Supported Transforms:
//   - Identity, Void: 			All data types
//   - Bucket: 				int, long, string, timestamptz
//   - Truncate: 				int, long, string
//   - Year, Month, Day, Hour: 	timestamptz

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
			return "", -1, fmt.Errorf("invalid numeric argument in transform %s: %s", transform, err)
		}
	}

	return base, arg, nil
}

func hashInt[T ~int32 | ~int64 | ~int](v T) uint32 {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(v))

	return murmur3.Sum32(buf[:])
}

func hashString(s string) uint32 {
	return murmur3.Sum32([]byte(s))
}

func identityTransform(val any, colType string) (pathStr string, typedVal any, err error) {
	switch colType {
	case "boolean":
		b := val.(bool)
		return strconv.FormatBool(b), b, nil
	case "int":
		v := val.(int32)
		return fmt.Sprintf("%d", v), v, nil
	case "long":
		v := val.(int64)
		return fmt.Sprintf("%d", v), v, nil
	case "float":
		v := val.(float32)
		return fmt.Sprintf("%g", v), v, nil
	case "double":
		v := val.(float64)
		return fmt.Sprintf("%g", v), v, nil
	case "string":
		s := val.(string)
		return s, s, nil
	case "timestamptz":
		t := val.(time.Time).UTC()
		pathStr = t.Format("2006-01-02T15:04:05-07:00")
		typedVal = t.UnixMicro()
		return pathStr, typedVal, nil
	default:
		return fmt.Sprintf("%v", val), val, nil
	}
}

func timeTransform(val any, unit string, colType string) (pathStr string, typedVal any, err error) {
	if colType != "timestamptz" {
		return "", nil, fmt.Errorf("unsupported time transform %q", unit)
	}

	v, _ := val.(time.Time)
	v = v.UTC()
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

	switch unit {
	case "year":
		year := v.Year()
		yearsFrom1970 := int32(year - 1970) // #nosec G115 -- year range is bounded and safe for int32
		return strconv.Itoa(year), yearsFrom1970, nil
	case "month":
		monthsFrom1970 := int32((v.Year()-1970)*12 + int(v.Month()-1)) // #nosec G115 -- month calculation is bounded and safe for int32
		return v.Format("2006-01"), monthsFrom1970, nil
	case "day":
		daysFrom1970 := int32(v.Sub(epoch).Hours() / 24)
		return v.Format("2006-01-02"), daysFrom1970, nil
	case "hour":
		hoursFrom1970 := int32(v.Sub(epoch).Hours())
		return v.Format("2006-01-02-15"), hoursFrom1970, nil
	default:
		return "", nil, fmt.Errorf("unsupported time transform %q", unit)
	}
}

func bucketTransform(val any, num int, colType string) (pathStr string, typedVal any, err error) {
	if num <= 0 {
		return "", nil, fmt.Errorf("invalid number of buckets: %d (must be > 0)", num)
	}

	var h uint32
	switch colType {
	case "int":
		v, _ := val.(int32)
		h = hashInt(v)
	case "long":
		v, _ := val.(int64)
		h = hashInt(v)
	case "timestamptz":
		tm, ok := val.(time.Time)
		if !ok {
			return "", nil, fmt.Errorf("expected time.Time for colType %q, got %T", colType, val)
		}
		h = hashInt(tm.UnixMicro())
	case "string":
		str, ok := val.(string)
		if !ok {
			return "", nil, fmt.Errorf("expected string for colType %q, got %T", colType, val)
		}
		h = hashString(str)
	default:
		return "", nil, fmt.Errorf("unsupported colType %q for bucket transform", colType)
	}

	masked := int(h & 0x7FFFFFFF)
	bucket := int32(masked % num) // #nosec G115 -- modulo result is bounded by num parameter
	return strconv.Itoa(int(bucket)), bucket, nil
}

func truncateTransform(val any, n int, colType string) (pathStr string, typedVal any, err error) {
	if n <= 0 {
		return "", nil, fmt.Errorf("invalid truncate width: %d (must be > 0)", n)
	}

	switch colType {
	case "int":
		v, _ := val.(int32)
		if n > math.MaxInt32 {
			return "", nil, fmt.Errorf("truncate width %d exceeds int32 range", n)
		}
		n32 := int32(n)
		trunc := v - (((v % n32) + n32) % n32)
		return fmt.Sprintf("%d", trunc), trunc, nil
	case "long":
		v, _ := val.(int64)
		n64 := int64(n)
		// Using Iceberg's formula for proper negative number handling
		trunc := v - (((v % n64) + n64) % n64)
		return fmt.Sprintf("%d", trunc), trunc, nil
	case "string":
		v, ok := val.(string)
		if !ok {
			return "", nil, fmt.Errorf("expected string for colType %q, got %T", colType, val)
		}
		runes := []rune(v)
		if len(runes) <= n {
			return v, v, nil
		}
		truncated := string(runes[:n])
		return truncated, truncated, nil
	default:
		return "", nil, fmt.Errorf("unsupported colType %q for truncate transform", colType)
	}
}

func ConstructColPath(valueStr, field, transform string) string {
	base, _, _ := parseTransform(transform)

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

// TransformValue applies a partition transform and returns both:
// 1. pathStr: human-readable string for partition directory names (e.g., "null", "2024-01-05")
// 2. typedVal: properly typed value for Iceberg PartitionData (e.g., nil, int32, int64, string)
func TransformValue(val any, transform string, colType string) (pathStr string, typedVal any, err error) {
	transform = strings.TrimSpace(strings.ToLower(transform))
	if val == nil {
		return NULL, nil, nil
	}

	base, arg, err := parseTransform(transform)
	if err != nil {
		return "", nil, err
	}

	switch base {
	case "identity":
		return identityTransform(val, colType)
	case "void":
		return NULL, nil, nil
	case "year", "month", "day", "hour":
		return timeTransform(val, base, colType)
	case "bucket":
		return bucketTransform(val, arg, colType)
	case "truncate":
		return truncateTransform(val, arg, colType)
	default:
		return "", nil, fmt.Errorf("unknown partition transform %q", transform)
	}
}

func toProtoPartitionValues(values []any) ([]*proto.ArrowPayload_FileMetadata_PartitionValue, error) {
	result := make([]*proto.ArrowPayload_FileMetadata_PartitionValue, len(values))

	for i, val := range values {
		pv := &proto.ArrowPayload_FileMetadata_PartitionValue{}

		if val == nil {
			// nil values remain as empty proto message (all oneof fields unset)
			result[i] = pv
			continue
		}

		switch v := val.(type) {
		case int32:
			pv.Value = &proto.ArrowPayload_FileMetadata_PartitionValue_IntValue{IntValue: v}
		case int64:
			pv.Value = &proto.ArrowPayload_FileMetadata_PartitionValue_LongValue{LongValue: v}
		case float32:
			pv.Value = &proto.ArrowPayload_FileMetadata_PartitionValue_FloatValue{FloatValue: v}
		case float64:
			pv.Value = &proto.ArrowPayload_FileMetadata_PartitionValue_DoubleValue{DoubleValue: v}
		case string:
			pv.Value = &proto.ArrowPayload_FileMetadata_PartitionValue_StringValue{StringValue: v}
		case bool:
			// Booleans stored as string "true"/"false" per Iceberg convention
			pv.Value = &proto.ArrowPayload_FileMetadata_PartitionValue_StringValue{StringValue: fmt.Sprintf("%t", v)}
		default:
			return nil, fmt.Errorf("unsupported partition value type: %T", v)
		}

		result[i] = pv
	}

	return result, nil
}
