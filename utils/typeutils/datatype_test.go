package typeutils

import (
	"encoding/json"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type sampleStruct struct {
	Name string
}

func TestTypeFromValue(t *testing.T) {
	trueValue := true
	int64Value := int64(100)
	int8Value := int8(8)
	uintValue := uint(9)
	float64Value := float64(1.5)
	stringValue := "test string"
	dateStringValue := "2024-12-18"
	emptyStringValue := ""
	ptrInt64 := &int64Value
	doublePtrInt64 := &ptrInt64

	testCases := []struct {
		name     string
		input    any
		expected types.DataType
	}{
		{name: "nil", input: nil, expected: types.Null},
		{name: "nil_int_pointer", input: (*int)(nil), expected: types.Null},
		{name: "nil_string_pointer", input: (*string)(nil), expected: types.Null},
		{name: "invalid_reflect_value", input: reflect.Value{}, expected: types.Null},
		{name: "invalid_reflect_value_via_elem", input: reflect.ValueOf((*int)(nil)).Elem(), expected: types.Null},

		{name: "bool_true", input: true, expected: types.Bool},
		{name: "bool_false", input: false, expected: types.Bool},
		{name: "bool_pointer", input: &trueValue, expected: types.Bool},

		{name: "int", input: int(42), expected: types.Int32},
		{name: "int8_max", input: int8(math.MaxInt8), expected: types.Int32},
		{name: "int8_min", input: int8(math.MinInt8), expected: types.Int32},
		{name: "int16_max", input: int16(math.MaxInt16), expected: types.Int32},
		{name: "int32_min", input: int32(math.MinInt32), expected: types.Int32},
		{name: "uint", input: uint(100), expected: types.Int32},
		{name: "uint8_max", input: uint8(math.MaxUint8), expected: types.Int32},
		{name: "uint16_max", input: uint16(math.MaxUint16), expected: types.Int32},
		{name: "uint32_max", input: uint32(math.MaxUint32), expected: types.Int32},

		{name: "int64_max", input: int64(math.MaxInt64), expected: types.Int64},
		{name: "int64_min", input: int64(math.MinInt64), expected: types.Int64},
		{name: "int64_zero", input: int64(0), expected: types.Int64},
		{name: "uint64_max", input: uint64(math.MaxUint64), expected: types.Int64},

		{name: "float32_positive", input: float32(3.14), expected: types.Float32},
		{name: "float32_zero", input: float32(0), expected: types.Float32},
		{name: "float32_negative", input: float32(-1.5), expected: types.Float32},
		{name: "float64_positive", input: float64(2.718281828), expected: types.Float64},
		{name: "float64_zero", input: float64(0), expected: types.Float64},

		{name: "string_regular", input: "hello world", expected: types.String},
		{name: "string_empty", input: "", expected: types.String},
		{name: "string_not_date", input: "not a date", expected: types.String},
		{name: "string_date", input: "2024-12-18", expected: types.Timestamp},
		{name: "string_timestamp_second", input: "2024-12-18T10:30:00Z", expected: types.Timestamp},
		{name: "string_timestamp_milli", input: "2024-12-18T10:30:00.123Z", expected: types.TimestampMilli},
		{name: "string_timestamp_micro", input: "2024-12-18T10:30:00.123456Z", expected: types.TimestampMicro},
		{name: "string_timestamp_nano", input: "2024-12-18T10:30:00.123456789Z", expected: types.TimestampNano},
		{name: "string_invalid_date_shape", input: "2024-13-40", expected: types.String},

		{name: "byte_slice", input: []byte("hello"), expected: types.String},

		{name: "int_slice", input: []int{1, 2, 3}, expected: types.Array},
		{name: "empty_string_slice", input: []string{}, expected: types.Array},
		{name: "int_array", input: [5]int{1, 2, 3, 4, 5}, expected: types.Array},
		{name: "interface_slice", input: []any{1, "a", true}, expected: types.Array},

		{name: "map_string_int", input: map[string]int{"a": 1}, expected: types.Object},
		{name: "map_string_any_empty", input: map[string]any{}, expected: types.Object},
		{name: "map_int_string", input: map[int]string{1: "one"}, expected: types.Object},

		{name: "time_second_precision", input: time.Date(2024, 12, 18, 10, 30, 0, 0, time.UTC), expected: types.Timestamp},
		{name: "time_milli_precision", input: time.Date(2024, 12, 18, 10, 30, 0, 123000000, time.UTC), expected: types.TimestampMilli},
		{name: "time_micro_precision", input: time.Date(2024, 12, 18, 10, 30, 0, 123456000, time.UTC), expected: types.TimestampMicro},
		{name: "time_nano_precision", input: time.Date(2024, 12, 18, 10, 30, 0, 123456789, time.UTC), expected: types.TimestampNano},
		{name: "time_zero_value", input: time.Time{}, expected: types.Timestamp},

		{name: "int64_pointer", input: &int64Value, expected: types.Int64},
		{name: "float64_pointer", input: &float64Value, expected: types.Float64},
		{name: "double_pointer_int64", input: doublePtrInt64, expected: types.Int64},
		{name: "string_pointer", input: &stringValue, expected: types.String},
		{name: "nil_double_pointer", input: (**int64)(nil), expected: types.Null},

		{name: "custom_struct", input: sampleStruct{Name: "test"}, expected: types.Unknown},
		{name: "channel", input: make(chan int), expected: types.Unknown},
		{name: "function", input: func() {}, expected: types.Unknown},

		{name: "json_number_integer", input: json.Number("42"), expected: types.Int64},
		{name: "json_number_float", input: json.Number("42.5"), expected: types.Float64},
		{name: "json_number_invalid", input: json.Number("invalid"), expected: types.Float64},
		{name: "reflect_value_primitive_int", input: reflect.ValueOf(int(42)), expected: types.Int32},
		{name: "reflect_value_primitive_bool", input: reflect.ValueOf(true), expected: types.Bool},
		{name: "reflect_value_time", input: reflect.ValueOf(time.Date(2024, 12, 18, 10, 30, 0, 123000000, time.UTC)), expected: types.TimestampMilli},
		{name: "int8_pointer_not_direct_switch", input: &int8Value, expected: types.Int32},
		{name: "uint_pointer_not_direct_switch", input: &uintValue, expected: types.Int32},
		{name: "string_pointer_date", input: &dateStringValue, expected: types.Timestamp},
		{name: "string_pointer_empty", input: &emptyStringValue, expected: types.String},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, TypeFromValue(tc.input))
		})
	}
}

func TestMaximumOnDataTypeTimestamp(t *testing.T) {
	testCases := []struct {
		name          string
		leftArgument  string
		rightArgument string
		expected      string
		expectError   bool
	}{
		{name: "left_is_max", leftArgument: "2024-12-18", rightArgument: "2024-12-17", expected: "2024-12-18"},
		{name: "right_is_max", leftArgument: "2024-01-01", rightArgument: "2024-12-31", expected: "2024-12-31"},
		{name: "equal_values_returns_left", leftArgument: "2024-12-18", rightArgument: "2024-12-18", expected: "2024-12-18"},
		{name: "invalid_left_value", leftArgument: "invalid-date", rightArgument: "2024-12-18", expectError: true},
		{name: "invalid_right_value", leftArgument: "2024-12-18", rightArgument: "invalid-date", expectError: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := MaximumOnDataType(types.Timestamp, tc.leftArgument, tc.rightArgument)
			if tc.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "failed to reformat")
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.expected, got)
		})
	}

	t.Run("equal_timestamps_return_left_value", func(t *testing.T) {
		left := time.Date(2024, 12, 18, 0, 0, 0, 0, time.UTC)
		right := time.Date(2024, 12, 18, 0, 0, 0, 0, time.UTC)

		got, err := MaximumOnDataType(types.Timestamp, &left, &right)
		require.NoError(t, err)
		assert.Equal(t, &left, got)
	})
}

func TestMaximumOnDataTypeInt64(t *testing.T) {
	testCases := []struct {
		name          string
		leftArgument  int64
		rightArgument int64
		expected      int64
	}{
		{name: "left_is_max", leftArgument: 100, rightArgument: 50, expected: 100},
		{name: "right_is_max", leftArgument: -100, rightArgument: 100, expected: 100},
		{name: "equal_values_returns_right", leftArgument: 0, rightArgument: 0, expected: 0},
		{name: "boundaries", leftArgument: math.MaxInt64, rightArgument: math.MinInt64, expected: math.MaxInt64},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := MaximumOnDataType(types.Int64, tc.leftArgument, tc.rightArgument)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, got)
		})
	}

	t.Run("invalid_left_value", func(t *testing.T) {
		got, err := MaximumOnDataType[any](types.Int64, "not-a-number", 100)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to reformat")
		assert.Equal(t, "not-a-number", got)
	})

	t.Run("invalid_right_value", func(t *testing.T) {
		got, err := MaximumOnDataType[any](types.Int64, 100, "not-a-number")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to reformat")
		assert.Equal(t, 100, got)
	})

	t.Run("equal_values_return_right_value", func(t *testing.T) {
		got, err := MaximumOnDataType[any](types.Int64, "0", int64(0))
		require.NoError(t, err)
		assert.Equal(t, int64(0), got)
	})
}

func TestMaximumOnDataTypeUnsupported(t *testing.T) {
	testCases := []struct {
		name          string
		dataType      types.DataType
		leftArgument  any
		rightArgument any
		expected      any
	}{
		{name: "string", dataType: types.String, leftArgument: "a", rightArgument: "b", expected: "a"},
		{name: "float64", dataType: types.Float64, leftArgument: 1.5, rightArgument: 2.5, expected: 1.5},
		{name: "bool", dataType: types.Bool, leftArgument: true, rightArgument: false, expected: true},
		{name: "array", dataType: types.Array, leftArgument: []int{1}, rightArgument: []int{2}, expected: []int{1}},
		{name: "timestamp_milli", dataType: types.TimestampMilli, leftArgument: "2024-12-18T10:30:00.123Z", rightArgument: "2024-12-18T10:30:00.124Z", expected: "2024-12-18T10:30:00.123Z"},
		{name: "timestamp_micro", dataType: types.TimestampMicro, leftArgument: "2024-12-18T10:30:00.123456Z", rightArgument: "2024-12-18T10:30:00.123457Z", expected: "2024-12-18T10:30:00.123456Z"},
		{name: "timestamp_nano", dataType: types.TimestampNano, leftArgument: "2024-12-18T10:30:00.123456789Z", rightArgument: "2024-12-18T10:30:00.123456790Z", expected: "2024-12-18T10:30:00.123456789Z"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := MaximumOnDataType[any](tc.dataType, tc.leftArgument, tc.rightArgument)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "comparison not available for data types")
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestDetectTimestampPrecision(t *testing.T) {
	testCases := []struct {
		name     string
		input    time.Time
		expected types.DataType
	}{
		{name: "second_precision", input: time.Date(2024, 12, 18, 10, 30, 0, 0, time.UTC), expected: types.Timestamp},
		{name: "epoch_second_precision", input: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), expected: types.Timestamp},

		{name: "one_millisecond", input: time.Date(2024, 12, 18, 10, 30, 0, 1000000, time.UTC), expected: types.TimestampMilli},
		{name: "hundred_twenty_three_millisecond", input: time.Date(2024, 12, 18, 10, 30, 0, 123000000, time.UTC), expected: types.TimestampMilli},
		{name: "nine_hundred_ninety_nine_millisecond", input: time.Date(2024, 12, 18, 10, 30, 0, 999000000, time.UTC), expected: types.TimestampMilli},

		{name: "one_microsecond", input: time.Date(2024, 12, 18, 10, 30, 0, 1000, time.UTC), expected: types.TimestampMicro},
		{name: "hundred_twenty_three_thousand_four_hundred_fifty_six_microseconds", input: time.Date(2024, 12, 18, 10, 30, 0, 123456000, time.UTC), expected: types.TimestampMicro},
		{name: "nine_hundred_ninety_nine_thousand_nine_hundred_ninety_nine_microseconds", input: time.Date(2024, 12, 18, 10, 30, 0, 999999000, time.UTC), expected: types.TimestampMicro},

		{name: "one_nanosecond", input: time.Date(2024, 12, 18, 10, 30, 0, 1, time.UTC), expected: types.TimestampNano},
		{name: "hundred_twenty_three_million_four_hundred_fifty_six_thousand_seven_hundred_eighty_nine_nanoseconds", input: time.Date(2024, 12, 18, 10, 30, 0, 123456789, time.UTC), expected: types.TimestampNano},
		{name: "nine_hundred_ninety_nine_million_nine_hundred_ninety_nine_thousand_nine_hundred_ninety_nine_nanoseconds", input: time.Date(2024, 12, 18, 10, 30, 0, 999999999, time.UTC), expected: types.TimestampNano},

		{name: "zero_time", input: time.Time{}, expected: types.Timestamp},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, detectTimestampPrecision(tc.input))
		})
	}

	t.Run("time_now_precision", func(t *testing.T) {
		result := detectTimestampPrecision(time.Now())
		assert.Contains(t, []types.DataType{types.Timestamp, types.TimestampMilli, types.TimestampMicro, types.TimestampNano}, result)
	})
}

func TestExtractAndMapColumnType(t *testing.T) {
	typeMapping := map[string]types.DataType{
		"bigint":    types.Int64,
		"float":     types.Float32,
		"double":    types.Float64,
		"timestamp": types.Timestamp,
		"unknown":   types.Unknown,
	}

	testCases := []struct {
		name       string
		columnType string
		expected   types.DataType
	}{
		{name: "empty_string", columnType: "", expected: types.DataType("")},
		{name: "only_spaces", columnType: "   ", expected: types.DataType("")},
		{name: "bigint_with_size", columnType: "BIGINT(20)", expected: types.Int64},
		{name: "bigint_trailing_space", columnType: "BIGINT ", expected: types.Int64},
		{name: "float_trimmed", columnType: "  float  ", expected: types.Float32},
		{name: "double", columnType: "double", expected: types.Float64},
		{name: "timestamp_with_size", columnType: "timestamp(6)", expected: types.Timestamp},
		{name: "unknown", columnType: "unknown", expected: types.Unknown},
		{name: "unmapped_type", columnType: "varchar(255)", expected: types.DataType("")},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, ExtractAndMapColumnType(tc.columnType, typeMapping))
		})
	}
}
