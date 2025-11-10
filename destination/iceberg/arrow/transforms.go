package arrow

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

// The current transforms logic is limited to the data types which are handled by OLake
// As we start handling more data types, we will update the transformations logic here

type Transform interface {
	canTransform(colType string) bool
	apply(val any, colType string) (string, error)
}

type IdentityTransform struct{}

func (t IdentityTransform) canTransform(colType string) bool {
	// OLake does not handles complex types right now, only primitive types, thus true
	// Once we have complex types, we need to add a check here

	return true
}

func (t IdentityTransform) apply(val any, colType string) (string, error) {
	switch v := val.(type) {
	case nil:
		return "null", nil
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

var epochTM = time.Unix(0, 0).UTC()

func canTransform(colType string) bool {
	switch colType {
	case "timestamptz":
		return true
	default:
		return false
	}
}

type YearTransform struct{}

func (t YearTransform) canTransform(colType string) bool {
	return canTransform(colType)
}

func (t YearTransform) apply(val any, colType string) (string, error) {
	switch colType {
	case "timestamptz":
		if val == nil {
			return "null", nil
		}

		var tm int

		switch t := val.(type) {
		case time.Time:
			tm = t.UTC().Year()
		default:
			return "", fmt.Errorf("cannot apply year transform for col: %v", colType)
		}

		out := int32(tm - epochTM.Year())

		return fmt.Sprintf("%v", out), nil
	default:
		return "", fmt.Errorf("cannot apply year transformation for col: %v", colType)
	}
}

type MonthTransform struct{}

func (t MonthTransform) canTransform(colType string) bool {
	return canTransform(colType)
}

func (t MonthTransform) apply(val any, colType string) (string, error) {
	switch colType {
	case "timestamptz":
		if val == nil {
			return "null", nil
		}

		var tm time.Time

		switch t := val.(type) {
		case time.Time:
			tm = t
		default:
			return "", fmt.Errorf("cannot apply month transformation for col: %v", colType)
		}

		out := int32((tm.Year()-epochTM.Year())*12 + (int(tm.Month()) - int(epochTM.Month())))

		return fmt.Sprintf("%v", out), nil
	default:

		return "", fmt.Errorf("cannot apply month transformation for col: %v", colType)
	}
}

type DayTransform struct{}

func (t DayTransform) canTransform(colType string) bool {
	return canTransform(colType)
}

func (t DayTransform) apply(val any, colType string) (string, error) {
	switch colType {
	case "timestamp", "timestamptz":
		if val == nil {
			return "null", nil
		}

		var tm int32

		switch v := val.(type) {
		case time.Time:
			tm = int32(v.Truncate(24*time.Hour).Unix() / int64((time.Hour * 24).Seconds()))
		default:
			return "", fmt.Errorf("cannot apply day transformation for col: %v", colType)
		}

		return fmt.Sprintf("%v", tm), nil
	default:
		return "", fmt.Errorf("cannot apply day transformation for col: %v", colType)
	}
}

type HourTransform struct{}

func (t HourTransform) canTransform(colType string) bool {
	return canTransform(colType)
}

func (t HourTransform) apply(val any, colType string) (string, error) {
	switch colType {
	case "timestamptz":
		if val == nil {
			return "null", nil
		}

		var tm int32

		switch v := val.(type) {
		case time.Time:
			tm = int32(v.Truncate(24*time.Hour).Unix() / int64((time.Hour * 24).Seconds()))
		default:
			return "", fmt.Errorf("cannot apply hour transformation for col: %v", colType)
		}

		return fmt.Sprintf("%v", tm), nil
	default:
		return "", fmt.Errorf("cannot apply hour transformation for col: %v", colType)
	}
}

type VoidTransform struct{}

func (t VoidTransform) canTransform(colType string) bool              { return true }
func (t VoidTransform) apply(val any, colType string) (string, error) { return "null", nil }

func hashFunction[T ~int32 | ~int64](v any) uint32 {
	var (
		val = uint64(v.(T))
		buf [8]byte
		b   = buf[:]
	)

	binary.LittleEndian.PutUint64(b, val)

	return murmur3.Sum32(b)
}

type BucketTransform struct {
	NumBuckets int
}

func (t BucketTransform) canTransform(colType string) bool {
	switch colType {
	case "int", "long", "timestamptz", "string":
		return true
	default:
		return false
	}
}

func (t BucketTransform) apply(val any, colType string) (string, error) {
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

	return fmt.Sprintf("%v", hash), nil
}

type TruncateTransform struct {
	Width int
}

func (t TruncateTransform) canTransform(colType string) bool {
	switch colType {
	case "int", "long", "string":
		return true
	default:
		return false
	}
}

func (t TruncateTransform) apply(val any, colType string) (string, error) {
	if val == nil {
		return "null", nil
	}

	switch colType {
	case "int":
		v := val.(int32)
		tval := v - (v % int32(t.Width))
		return fmt.Sprintf("%v", tval), nil
	case "long":
		v := val.(int64)
		tVal := v - (v % int64(t.Width))
		return fmt.Sprintf("%v", tVal), nil
	case "string":
		v := val.(string)
		tVal := v[:min(len(v), t.Width)]
		return fmt.Sprintf("%v", tVal), nil
	default:
		return "", fmt.Errorf("cannot apply truncate transformation for col: %v", colType)
	}
}

func constructColPath(tVal, field, transform string) string {
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
