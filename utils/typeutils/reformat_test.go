package typeutils

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// TestGetFirstNotNullType tests the getFirstNotNullType function
func TestGetFirstNotNullType(t *testing.T) {
	tests := []struct {
		name     string
		input    []types.DataType
		expected types.DataType
	}{
		{
			name:     "all null",
			input:    []types.DataType{types.Null, types.Null},
			expected: types.Null,
		},
		{
			name:     "first non-null at start",
			input:    []types.DataType{types.String, types.Null},
			expected: types.String,
		},
		{
			name:     "first non-null in middle",
			input:    []types.DataType{types.Null, types.Int64, types.String},
			expected: types.Int64,
		},
		{
			name:     "first non-null at end",
			input:    []types.DataType{types.Null, types.Null, types.Bool},
			expected: types.Bool,
		},
		{
			name:     "all non-null values",
			input:    []types.DataType{types.Array, types.Int64, types.Float64, types.String, types.Bool},
			expected: types.Array,
		},
		{
			name:     "empty slice",
			input:    []types.DataType{},
			expected: types.Null,
		},
		{
			name:     "nil slice",
			input:    nil,
			expected: types.Null,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := getFirstNotNullType(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}

// TestReformatRecord tests the ReformatRecord function
func TestReformatRecord(t *testing.T) {
	tests := []struct {
		name        string
		fields      Fields
		record      types.Record
		expected    types.Record
		expectedErr error
	}{
		{
			name:        "empty record",
			fields:      Fields{},
			record:      types.Record{},
			expected:    types.Record{},
			expectedErr: nil,
		},
		{
			name: "multiple and extra fields valid",
			fields: Fields{
				"a": NewField(types.Int64),
				"b": NewField(types.String),
				"c": NewField(types.Array),
			},
			record: types.Record{
				"a": int64(1),
				"b": "hello",
			},
			expected: types.Record{
				"a": int64(1),
				"b": "hello",
			},
			expectedErr: nil,
		},
		{
			name: "missing field",
			fields: Fields{
				"a": NewField(types.Int64),
			},
			record: types.Record{
				"b": int64(10),
			},
			expected: types.Record{
				"b": int64(10),
			},
			expectedErr: fmt.Errorf("missing field [b]"),
		},
		{
			name: "invalid type conversion",
			fields: Fields{
				"a": NewField(types.Int64),
			},
			record: types.Record{
				"a": "invalid",
			},
			expected: types.Record{
				"a": "invalid",
			},
			expectedErr: fmt.Errorf("failed to reformat value[invalid] to datatype[integer] for key[a]: failed to change string invalid to int64: strconv.ParseInt: parsing \"invalid\": invalid syntax"),
		},
		{
			name: "nil value",
			fields: Fields{
				"a": NewField(types.Int64),
			},
			record: types.Record{
				"a": nil,
			},
			expected: types.Record{
				"a": nil,
			},
			expectedErr: nil,
		},
		{
			name: "null type",
			fields: Fields{
				"a": NewField(types.Null),
			},
			record: types.Record{
				"a": "some_value",
			},
			expected: types.Record{
				"a": nil,
			},
			expectedErr: nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ReformatRecord(tc.fields, tc.record)

			if tc.expectedErr != nil {
				assert.Equal(t, tc.expectedErr, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, tc.record)
			}
		})
	}
}

// TestReformatValueOnDataTypes tests the ReformatValueOnDataTypes function
func TestReformatValueOnDataTypes(t *testing.T) {
	tests := []struct {
		name        string
		dataTypes   []types.DataType
		value       any
		expected    any
		expectedErr error
	}{
		{
			name:        "nil value",
			dataTypes:   []types.DataType{types.Null},
			value:       nil,
			expected:    nil,
			expectedErr: nil,
		},
		{
			name:        "datatype null",
			dataTypes:   []types.DataType{types.Null},
			value:       "abc",
			expected:    nil,
			expectedErr: ErrNullValue,
		},
		{
			name:        "datatype bool",
			dataTypes:   []types.DataType{types.Bool, types.Timestamp, types.Int64, types.Float64, types.String, types.Array},
			value:       "true",
			expected:    true,
			expectedErr: nil,
		},
		{
			name:        "datatype wrong",
			dataTypes:   []types.DataType{types.Bool, types.Timestamp, types.Int64, types.Float64, types.String, types.Array},
			value:       10,
			expected:    false,
			expectedErr: fmt.Errorf("found to be boolean, but value is not boolean : 10"),
		},
		{
			name:        "empty datatype list",
			dataTypes:   []types.DataType{},
			value:       "123",
			expected:    nil,
			expectedErr: ErrNullValue,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ReformatValueOnDataTypes(tc.dataTypes, tc.value)

			if tc.expectedErr != nil {
				assert.Equal(t, tc.expectedErr, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}

// TestReformatValue tests the ReformatValue function
func TestReformatValue(t *testing.T) {
	tests := []struct {
		name        string
		dataType    types.DataType
		value       any
		expected    any
		expectedErr error
	}{
		// ===== Null =====
		{
			name:        "nil value",
			dataType:    types.Null,
			value:       nil,
			expected:    nil,
			expectedErr: nil,
		},
		{
			name:        "datatype null",
			dataType:    types.Null,
			value:       "abc",
			expected:    nil,
			expectedErr: ErrNullValue,
		},

		// ===== Bool =====
		{
			name:        "bool true",
			dataType:    types.Bool,
			value:       "true",
			expected:    true,
			expectedErr: nil,
		},
		{
			name:        "bool false",
			dataType:    types.Bool,
			value:       "false",
			expected:    false,
			expectedErr: nil,
		},
		{
			name:        "bool invalid",
			dataType:    types.Bool,
			value:       "invalid",
			expected:    false,
			expectedErr: fmt.Errorf("found to be boolean, but value is not boolean : invalid"),
		},

		// ===== Int64 =====
		{
			name:        "int64 from string",
			dataType:    types.Int64,
			value:       "123",
			expected:    int64(123),
			expectedErr: nil,
		},
		{
			name:        "invalid value int64",
			dataType:    types.Int64,
			value:       "invalid",
			expected:    int64(0),
			expectedErr: fmt.Errorf("failed to change string invalid to int64: strconv.ParseInt: parsing \"invalid\": invalid syntax"),
		},

		// ===== Int32 =====
		{
			name:        "int32 from string",
			dataType:    types.Int32,
			value:       "123",
			expected:    int32(123),
			expectedErr: nil,
		},
		{
			name:        "invalid value int32",
			dataType:    types.Int32,
			value:       "invalid",
			expected:    int32(0),
			expectedErr: fmt.Errorf("failed to change string invalid to int32: strconv.ParseInt: parsing \"invalid\": invalid syntax"),
		},

		// ===== Timestamp =====
		{
			name:        "timestamp from string",
			dataType:    types.Timestamp,
			value:       "2021-01-01T00:00:00Z",
			expected:    time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
			expectedErr: nil,
		},
		{
			name:        "timestampmilli from string",
			dataType:    types.TimestampMilli,
			value:       "2021-01-01T00:00:00.123Z",
			expected:    time.Date(2021, 1, 1, 0, 0, 0, 123000000, time.UTC),
			expectedErr: nil,
		},
		{
			name:        "timestampmicro from string",
			dataType:    types.TimestampMicro,
			value:       "2021-01-01T00:00:00.123456Z",
			expected:    time.Date(2021, 1, 1, 0, 0, 0, 123456000, time.UTC),
			expectedErr: nil,
		},
		{
			name:        "timestampnano from string",
			dataType:    types.TimestampNano,
			value:       "2021-01-01T00:00:00.123456789Z",
			expected:    time.Date(2021, 1, 1, 0, 0, 0, 123456789, time.UTC),
			expectedErr: nil,
		},
		{
			name:        "invalid value timestamp",
			dataType:    types.Timestamp,
			value:       "invalid",
			expected:    time.Time{},
			expectedErr: fmt.Errorf("string does not start with date pattern (YYYY-MM-DD)"),
		},

		// ===== String =====
		{
			name:        "string from int",
			dataType:    types.String,
			value:       10,
			expected:    "10",
			expectedErr: nil,
		},
		{
			name:        "string from int8",
			dataType:    types.String,
			value:       int8(10),
			expected:    "10",
			expectedErr: nil,
		},
		{
			name:        "string from int16",
			dataType:    types.String,
			value:       int16(10),
			expected:    "10",
			expectedErr: nil,
		},
		{
			name:        "string from int32",
			dataType:    types.String,
			value:       int32(10),
			expected:    "10",
			expectedErr: nil,
		},
		{
			name:        "string from int64",
			dataType:    types.String,
			value:       int64(10),
			expected:    "10",
			expectedErr: nil,
		},
		{
			name:        "string from uint",
			dataType:    types.String,
			value:       uint(10),
			expected:    "10",
			expectedErr: nil,
		},
		{
			name:        "string from uint8",
			dataType:    types.String,
			value:       uint8(10),
			expected:    "10",
			expectedErr: nil,
		},
		{
			name:        "string from uint16",
			dataType:    types.String,
			value:       uint16(10),
			expected:    "10",
			expectedErr: nil,
		},
		{
			name:        "string from uint32",
			dataType:    types.String,
			value:       uint32(10),
			expected:    "10",
			expectedErr: nil,
		},
		{
			name:        "string from uint64",
			dataType:    types.String,
			value:       uint64(10),
			expected:    "10",
			expectedErr: nil,
		},
		{
			name:        "string from float32",
			dataType:    types.String,
			value:       float32(5.9),
			expected:    "5.9",
			expectedErr: nil,
		},
		{
			name:        "string from float64",
			dataType:    types.String,
			value:       float64(5.9),
			expected:    "5.9",
			expectedErr: nil,
		},
		{
			name:        "string from bytes",
			dataType:    types.String,
			value:       []byte("hello"),
			expected:    "hello",
			expectedErr: nil,
		},

		// ===== Float32 =====
		{
			name:        "float32 from string",
			dataType:    types.Float32,
			value:       "123.45",
			expected:    float32(123.45),
			expectedErr: nil,
		},
		{
			name:        "float32 invalid",
			dataType:    types.Float32,
			value:       "invalid",
			expected:    float32(0),
			expectedErr: fmt.Errorf("failed to change string invalid to float32: strconv.ParseFloat: parsing \"invalid\": invalid syntax"),
		},

		// ===== Float64 =====
		{
			name:        "float64 from string",
			dataType:    types.Float64,
			value:       "123.45",
			expected:    float64(123.45),
			expectedErr: nil,
		},
		{
			name:        "float64 invalid",
			dataType:    types.Float64,
			value:       "invalid",
			expected:    float64(0),
			expectedErr: fmt.Errorf("failed to change string invalid to float64: strconv.ParseFloat: parsing \"invalid\": invalid syntax"),
		},

		// ===== Array =====
		{
			name:        "array existing",
			dataType:    types.Array,
			value:       []any{1, 2},
			expected:    []any{1, 2},
			expectedErr: nil,
		},
		{
			name:        "array wrap",
			dataType:    types.Array,
			value:       5,
			expected:    []any{5},
			expectedErr: nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ReformatValue(tc.dataType, tc.value)

			if tc.expectedErr != nil {
				assert.Equal(t, tc.expectedErr, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}

// TestParseFilterValue tests the ParseFilterValue function
func TestParseFilterValue(t *testing.T) {
	tests := []struct {
		name        string
		dataType    types.DataType
		value       any
		expected    any
		expectedErr error
	}{
		// ===== Timestamp strict parsing =====
		{
			name:        "timestamp valid",
			dataType:    types.Timestamp,
			value:       "2023-01-01T00:00:00Z",
			expected:    time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
			expectedErr: nil,
		},
		{
			name:        "timestampmilli from string",
			dataType:    types.TimestampMilli,
			value:       "2021-01-01T00:00:00.123Z",
			expected:    time.Date(2021, 1, 1, 0, 0, 0, 123000000, time.UTC),
			expectedErr: nil,
		},
		{
			name:        "timestampmicro from string",
			dataType:    types.TimestampMicro,
			value:       "2021-01-01T00:00:00.123456Z",
			expected:    time.Date(2021, 1, 1, 0, 0, 0, 123456000, time.UTC),
			expectedErr: nil,
		},
		{
			name:        "timestampnano from string",
			dataType:    types.TimestampNano,
			value:       "2021-01-01T00:00:00.123456789Z",
			expected:    time.Date(2021, 1, 1, 0, 0, 0, 123456789, time.UTC),
			expectedErr: nil,
		},
		{
			name:        "timestamp invalid",
			dataType:    types.Timestamp,
			value:       "not-a-date",
			expected:    time.Time{},
			expectedErr: fmt.Errorf("string does not start with date pattern (YYYY-MM-DD)"),
		},

		// ===== Default → ReformatValue =====
		{
			name:        "int64",
			dataType:    types.Int64,
			value:       "123",
			expected:    int64(123),
			expectedErr: nil,
		},
		{
			name:        "null non-nil value",
			dataType:    types.Null,
			value:       "abc",
			expected:    nil,
			expectedErr: ErrNullValue,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ParseFilterValue(tc.dataType, tc.value)

			if tc.expectedErr != nil {
				assert.Equal(t, tc.expectedErr, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}

// TestReformatBool tests the ReformatBool function
func TestReformatBool(t *testing.T) {
	tests := []struct {
		name         string
		v            any
		stateVersion int
		expected     bool
		expectedErr  error
	}{
		// ===== bool =====
		{
			name:        "bool true",
			v:           true,
			expected:    true,
			expectedErr: nil,
		},
		{
			name:        "bool false",
			v:           false,
			expected:    false,
			expectedErr: nil,
		},

		// ===== string =====
		{
			name:        "string 1",
			v:           "1",
			expected:    true,
			expectedErr: nil,
		},
		{
			name:        "string t",
			v:           "t",
			expected:    true,
			expectedErr: nil,
		},
		{
			name:        "string T",
			v:           "T",
			expected:    true,
			expectedErr: nil,
		},
		{
			name:        "string true",
			v:           "true",
			expected:    true,
			expectedErr: nil,
		},
		{
			name:        "string TRUE",
			v:           "TRUE",
			expected:    true,
			expectedErr: nil,
		},
		{
			name:        "string True",
			v:           "True",
			expected:    true,
			expectedErr: nil,
		},
		{
			name:        "string YES",
			v:           "YES",
			expected:    true,
			expectedErr: nil,
		},
		{
			name:        "string Yes",
			v:           "Yes",
			expected:    true,
			expectedErr: nil,
		},
		{
			name:        "string yes",
			v:           "yes",
			expected:    true,
			expectedErr: nil,
		},
		{
			name:        "string 0",
			v:           "0",
			expected:    false,
			expectedErr: nil,
		},
		{
			name:        "string f",
			v:           "f",
			expected:    false,
			expectedErr: nil,
		},
		{
			name:        "string F",
			v:           "F",
			expected:    false,
			expectedErr: nil,
		},
		{
			name:        "string false",
			v:           "false",
			expected:    false,
			expectedErr: nil,
		},
		{
			name:        "string FALSE",
			v:           "FALSE",
			expected:    false,
			expectedErr: nil,
		},
		{
			name:        "string False",
			v:           "False",
			expected:    false,
			expectedErr: nil,
		},
		{
			name:        "string NO",
			v:           "NO",
			expected:    false,
			expectedErr: nil,
		},
		{
			name:        "string No",
			v:           "No",
			expected:    false,
			expectedErr: nil,
		},
		{
			name:        "string no",
			v:           "no",
			expected:    false,
			expectedErr: nil,
		},

		// --- invalid string ---
		{
			name:        "string invalid",
			v:           "maybe",
			expected:    false,
			expectedErr: fmt.Errorf("found to be boolean, but value is not boolean : %v", "maybe"),
		},
		{
			name:        "string empty",
			v:           "",
			expected:    false,
			expectedErr: fmt.Errorf("found to be boolean, but value is not boolean : %v", ""),
		},

		// ===== int =====
		{
			name:        "int 1",
			v:           int(1),
			expected:    true,
			expectedErr: nil,
		},
		{
			name:        "int 0",
			v:           int(0),
			expected:    false,
			expectedErr: nil,
		},
		{
			name:        "int invalid",
			v:           int(2),
			expected:    false,
			expectedErr: fmt.Errorf("found to be boolean, but value is not boolean : 2"),
		},
		{
			name:         "int backward compatibility",
			v:            int(1),
			stateVersion: 6,
			expected:     true,
			expectedErr:  nil,
		},
		{
			name:        "int16 1",
			v:           int16(1),
			expected:    true,
			expectedErr: nil,
		},
		{
			name:        "int16 0",
			v:           int16(0),
			expected:    false,
			expectedErr: nil,
		},
		{
			name:        "int16 invalid",
			v:           int16(2),
			expected:    false,
			expectedErr: fmt.Errorf("found to be boolean, but value is not boolean : %v", "2"),
		},
		{
			name:         "int16 backward compatibility",
			v:            int16(1),
			stateVersion: 6,
			expected:     false,
			expectedErr:  fmt.Errorf("found to be boolean, but value is not boolean : 1"),
		},
		{
			name:        "int32 1",
			v:           int32(1),
			expected:    true,
			expectedErr: nil,
		},
		{
			name:        "int32 0",
			v:           int32(0),
			expected:    false,
			expectedErr: nil,
		},
		{
			name:        "int32 invalid",
			v:           int32(2),
			expected:    false,
			expectedErr: fmt.Errorf("found to be boolean, but value is not boolean : 2"),
		},
		{
			name:         "int32 backward compatibility",
			v:            int32(1),
			stateVersion: 6,
			expected:     false,
			expectedErr:  fmt.Errorf("found to be boolean, but value is not boolean : 1"),
		},
		{
			name:        "int64 1",
			v:           int64(1),
			expected:    true,
			expectedErr: nil,
		},
		{
			name:        "int64 0",
			v:           int64(0),
			expected:    false,
			expectedErr: nil,
		},
		{
			name:        "int64 invalid",
			v:           int64(2),
			expected:    false,
			expectedErr: fmt.Errorf("found to be boolean, but value is not boolean : 2"),
		},
		{
			name:         "int64 backward compatibility",
			v:            int64(1),
			stateVersion: 6,
			expected:     false,
			expectedErr:  fmt.Errorf("found to be boolean, but value is not boolean : 1"),
		},
		{
			name:        "int8 1",
			v:           int8(1),
			expected:    true,
			expectedErr: nil,
		},
		{
			name:        "int8 0",
			v:           int8(0),
			expected:    false,
			expectedErr: nil,
		},
		{
			name:        "int8 invalid",
			v:           int8(2),
			expected:    false,
			expectedErr: fmt.Errorf("found to be boolean, but value is not boolean : 2"),
		},
		{
			name:         "int8 backward compatibility",
			v:            int8(1),
			stateVersion: 6,
			expected:     false,
			expectedErr:  fmt.Errorf("found to be boolean, but value is not boolean : 1"),
		},

		// ===== default / unsupported =====
		{
			name:        "float input",
			v:           1.0,
			expected:    false,
			expectedErr: fmt.Errorf("found to be boolean, but value is not boolean : 1"),
		},
		{
			name:        "slice input",
			v:           []int{1},
			expected:    false,
			expectedErr: fmt.Errorf("found to be boolean, but value is not boolean : [1]"),
		},
		{
			name:        "nil input",
			v:           nil,
			expected:    false,
			expectedErr: fmt.Errorf("found to be boolean, but value is not boolean : <nil>"),
		},
	}

	old := constants.LoadedStateVersion
	t.Cleanup(func() { constants.LoadedStateVersion = old })

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			constants.LoadedStateVersion = utils.Ternary(tc.stateVersion != 0, tc.stateVersion, constants.LatestStateVersion).(int)

			result, err := ReformatBool(tc.v)
			if tc.expectedErr != nil {
				assert.Equal(t, tc.expectedErr, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}

// TestReformatDate tests the ReformatDate function
func TestReformatDate(t *testing.T) {
	now := time.Now().UTC()

	validNullTime := sql.NullTime{
		Time:  now,
		Valid: true,
	}

	invalidNullTime := sql.NullTime{
		Valid: false,
	}

	tests := []struct {
		name            string
		v               any
		isTimestampInDB bool
		expected        time.Time
		expectedErr     error
	}{
		// ===== []uint8 =====
		{
			name:            "byte slice valid",
			v:               []uint8("2023-10-05"),
			isTimestampInDB: true,
			expected:        time.Date(2023, 10, 5, 0, 0, 0, 0, time.UTC),
			expectedErr:     nil,
		},
		{
			name:            "byte slice invalid",
			v:               []uint8("2023-10-a5"),
			isTimestampInDB: true,
			expected:        time.Time{},
			expectedErr:     fmt.Errorf("string does not start with date pattern (YYYY-MM-DD)"),
		},

		// ===== []int8 =====
		{
			name:            "int8 slice valid",
			v:               []int8{'2', '0', '2', '3', '-', '1', '0', '-', '0', '5'},
			isTimestampInDB: true,
			expected:        time.Date(2023, 10, 5, 0, 0, 0, 0, time.UTC),
			expectedErr:     nil,
		},
		{
			name:            "int8 slice invalid",
			v:               []int8{'2', '0', '2', '3', '-', '1', '0', '-', '0', '5', 'a'},
			isTimestampInDB: true,
			expected:        time.Unix(0, 0).UTC(),
			expectedErr:     nil,
		},

		// ===== int64 =====
		{
			name:            "int64 unix",
			v:               int64(1000),
			isTimestampInDB: true,
			expected:        time.Unix(1000, 0),
			expectedErr:     nil,
		},

		// ===== *int64 =====
		{
			name:            "int64 ptr valid",
			v:               func() *int64 { x := int64(1000); return &x }(),
			isTimestampInDB: true,
			expected:        time.Unix(1000, 0),
			expectedErr:     nil,
		},
		{
			name:            "int64 ptr nil",
			v:               (*int64)(nil),
			isTimestampInDB: true,
			expected:        time.Time{},
			expectedErr:     fmt.Errorf("null time passed"),
		},

		// ===== time.Time =====
		{
			name:            "time direct",
			v:               now,
			isTimestampInDB: true,
			expected:        now,
			expectedErr:     nil,
		},

		// ===== *time.Time =====
		{
			name:            "time ptr valid",
			v:               &now,
			isTimestampInDB: true,
			expected:        now,
			expectedErr:     nil,
		},
		{
			name:            "time ptr nil",
			v:               (*time.Time)(nil),
			isTimestampInDB: true,
			expected:        time.Time{},
			expectedErr:     fmt.Errorf("null time passed"),
		},

		// ===== sql.NullTime =====
		{
			name:            "sql nulltime valid",
			v:               validNullTime,
			isTimestampInDB: true,
			expected:        now,
			expectedErr:     nil,
		},
		{
			name:            "sql nulltime invalid",
			v:               invalidNullTime,
			isTimestampInDB: true,
			expected:        time.Time{},
			expectedErr:     fmt.Errorf("invalid null time"),
		},

		// ===== *sql.NullTime =====
		{
			name:            "sql nulltime ptr valid",
			v:               &validNullTime,
			isTimestampInDB: true,
			expected:        now,
			expectedErr:     nil,
		},
		{
			name:            "sql nulltime ptr nil",
			v:               (*sql.NullTime)(nil),
			isTimestampInDB: true,
			expected:        time.Time{},
			expectedErr:     fmt.Errorf("null time passed"),
		},
		{
			name:            "sql nulltime ptr invalid",
			v:               &invalidNullTime,
			isTimestampInDB: true,
			expected:        time.Time{},
			expectedErr:     fmt.Errorf("invalid null time"),
		},

		// ===== nil =====
		{
			name:            "nil input",
			v:               nil,
			isTimestampInDB: true,
			expected:        time.Time{},
			expectedErr:     nil,
		},

		// ===== string =====
		{
			name:            "string valid",
			v:               "2023-10-05",
			isTimestampInDB: true,
			expected:        time.Date(2023, 10, 5, 0, 0, 0, 0, time.UTC),
			expectedErr:     nil,
		},
		{
			name:            "string invalid",
			v:               "2023-10-a5",
			isTimestampInDB: true,
			expected:        time.Time{},
			expectedErr:     fmt.Errorf("string does not start with date pattern (YYYY-MM-DD)"),
		},

		// ===== *string =====
		{
			name:            "string ptr valid",
			v:               func() *string { s := "2023-10-05 12:30:45"; return &s }(),
			isTimestampInDB: true,
			expected:        time.Date(2023, 10, 5, 12, 30, 45, 0, time.UTC),
			expectedErr:     nil,
		},
		{
			name:            "string ptr nil",
			v:               (*string)(nil),
			isTimestampInDB: true,
			expected:        time.Time{},
			expectedErr:     fmt.Errorf("empty string passed"),
		},

		// ===== primitive.DateTime =====
		{
			name:            "primitive datetime",
			v:               primitive.NewDateTimeFromTime(now),
			isTimestampInDB: true,
			expected:        primitive.NewDateTimeFromTime(now).Time(),
			expectedErr:     nil,
		},

		// ===== *any recursion =====
		{
			name:            "pointer any recursion",
			v:               func() *any { var x any = "2023-10-05"; return &x }(),
			isTimestampInDB: true,
			expected:        time.Date(2023, 10, 5, 0, 0, 0, 0, time.UTC),
			expectedErr:     nil,
		},

		// ===== unsupported =====
		{
			name:            "unsupported type",
			v:               []int{1, 2},
			isTimestampInDB: true,
			expected:        time.Time{},
			expectedErr:     fmt.Errorf("unhandled type[[]int] passed: unable to parse into time"),
		},
		{
			name:            "year zero",
			v:               time.Date(0, 0, 0, 0, 0, 0, 0, time.UTC),
			isTimestampInDB: true,
			expected:        time.Unix(0, 0).UTC(),
			expectedErr:     nil,
		},
		{
			name:            "year above max",
			v:               time.Date(22000, 5, 10, 0, 0, 0, 0, time.UTC),
			isTimestampInDB: true,
			expected:        time.Date(9999, 5, 10, 0, 0, 0, 0, time.UTC),
			expectedErr:     nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ReformatDate(tc.v, tc.isTimestampInDB)

			if tc.expectedErr != nil {
				assert.Equal(t, tc.expectedErr, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}

// TestParseStringTimestamp tests the parseStringTimestamp function
func TestParseStringTimestamp(t *testing.T) {
	tests := []struct {
		name            string
		value           string
		isTimestampInDB bool
		stateVersion    int
		expected        time.Time
		expectedErr     error
	}{
		// ===== valid formats =====
		{
			name:            "date only",
			value:           "2023-10-05",
			isTimestampInDB: true,
			expected:        time.Date(2023, 10, 5, 0, 0, 0, 0, time.UTC),
			expectedErr:     nil,
		},
		{
			name:            "datetime space format",
			value:           "2023-10-05 12:30:45",
			isTimestampInDB: true,
			expected:        time.Date(2023, 10, 5, 12, 30, 45, 0, time.UTC),
			expectedErr:     nil,
		},
		{
			name:            "iso format",
			value:           "2023-10-05T12:30:45",
			isTimestampInDB: true,
			expected:        time.Date(2023, 10, 5, 12, 30, 45, 0, time.UTC),
			expectedErr:     nil,
		},
		{
			name:            "iso with Z",
			value:           "2020-08-17T05:50:22.895Z",
			isTimestampInDB: true,
			expected:        time.Date(2020, 8, 17, 5, 50, 22, 895000000, time.UTC),
			expectedErr:     nil,
		},

		// ===== invalid formats =====
		{
			name:            "invalid format too short",
			value:           "2023-10",
			isTimestampInDB: true,
			expected:        time.Time{},
			expectedErr:     fmt.Errorf("string does not start with date pattern (YYYY-MM-DD)"),
		},
		{
			name:            "invalid format too many parts",
			value:           "21-0-2-3-1",
			isTimestampInDB: true,
			expected:        time.Time{},
			expectedErr:     fmt.Errorf("string does not start with date pattern (YYYY-MM-DD)"),
		},
		{
			name:            "invalid format year too long",
			value:           "1234-1-05",
			isTimestampInDB: true,
			expected:        time.Time{},
			expectedErr:     fmt.Errorf("string does not start with date pattern (YYYY-MM-DD)"),
		},
		{
			name:            "invalid format year too short",
			value:           "abc-10-05",
			isTimestampInDB: true,
			expected:        time.Time{},
			expectedErr:     fmt.Errorf("string does not start with date pattern (YYYY-MM-DD)"),
		},

		// ===== parse failure + non-db + version != 0 =====
		{
			name:            "invalid non-db versioned",
			value:           "2023-10-05 invalid",
			isTimestampInDB: false,
			stateVersion:    1,
			expected:        time.Time{},
			expectedErr:     fmt.Errorf(`failed to parse datetime from available formats: parsing time "2023-10-05 invalid" as "2006-01-02T15:04:05.000000000Z": cannot parse " invalid" as "T"`),
		},

		// ===== parse failure + db + version != 0 =====
		{
			name:            "invalid db versioned",
			value:           "2023-10-05 invalid",
			isTimestampInDB: true,
			stateVersion:    1,
			expected:        time.Unix(0, 0).UTC(),
			expectedErr:     nil,
		},

		// ===== parse failure + db + version = 0 =====
		{
			name:            "invalid db backward compatibility",
			value:           "2023-10-05 invalid",
			isTimestampInDB: true,
			expected:        time.Unix(0, 0).UTC(),
			expectedErr:     nil,
		},
	}

	old := constants.LoadedStateVersion
	t.Cleanup(func() { constants.LoadedStateVersion = old })

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			constants.LoadedStateVersion = tc.stateVersion
			result, err := parseStringTimestamp(tc.value, tc.isTimestampInDB)

			if tc.expectedErr != nil {
				assert.Equal(t, tc.expectedErr, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}

// TestReformatInt64 tests the ReformatInt64 function
func TestReformatInt64(t *testing.T) {
	tests := []struct {
		name        string
		v           any
		expected    int64
		expectedErr error
		version     int
	}{
		// ===== json.Number =====
		{
			name:        "json number max int64",
			v:           json.Number(strconv.FormatInt(math.MaxInt64, 10)),
			expected:    int64(math.MaxInt64),
			expectedErr: nil,
		},
		{
			name:        "json number min int64",
			v:           json.Number(strconv.FormatInt(math.MinInt64, 10)),
			expected:    int64(math.MinInt64),
			expectedErr: nil,
		},
		{
			name:        "json number overflow",
			v:           json.Number(strconv.FormatUint(uint64(math.MaxInt64)+1, 10)),
			expected:    int64(0),
			expectedErr: &strconv.NumError{Func: "ParseInt", Num: strconv.FormatUint(uint64(math.MaxInt64)+1, 10), Err: strconv.ErrRange},
		},
		{
			name:        "json number float",
			v:           json.Number("5.9"),
			expected:    int64(0),
			expectedErr: &strconv.NumError{Func: "ParseInt", Num: "5.9", Err: strconv.ErrSyntax},
		},
		{
			name:        "json number invalid",
			v:           json.Number("abc"),
			expected:    int64(0),
			expectedErr: &strconv.NumError{Func: "ParseInt", Num: "abc", Err: strconv.ErrSyntax},
		},

		// ===== float =====
		{
			name:        "float32",
			v:           float32(5.9),
			expected:    int64(5),
			expectedErr: nil,
		},
		{
			name:        "float32 negative",
			v:           float32(-5.9),
			expected:    int64(-5),
			expectedErr: nil,
		},
		{
			name:        "float64",
			v:           float64(10.9),
			expected:    int64(10),
			expectedErr: nil,
		},
		{
			name:        "float64 negative",
			v:           float64(-10.9),
			expected:    int64(-10),
			expectedErr: nil,
		},

		// ===== signed ints =====
		{
			name:        "int",
			v:           int(math.MaxInt),
			expected:    int64(math.MaxInt),
			expectedErr: nil,
		},
		{
			name:        "int8 max",
			v:           int8(math.MaxInt8),
			expected:    int64(math.MaxInt8),
			expectedErr: nil,
		},
		{
			name:        "int8 min",
			v:           int8(math.MinInt8),
			expected:    int64(math.MinInt8),
			expectedErr: nil,
		},
		{
			name:        "int16 max",
			v:           int16(math.MaxInt16),
			expected:    int64(math.MaxInt16),
			expectedErr: nil,
		},
		{
			name:        "int16 min",
			v:           int16(math.MinInt16),
			expected:    int64(math.MinInt16),
			expectedErr: nil,
		},
		{
			name:        "int32 max",
			v:           int32(math.MaxInt32),
			expected:    int64(math.MaxInt32),
			expectedErr: nil,
		},
		{
			name:        "int32 min",
			v:           int32(math.MinInt32),
			expected:    int64(math.MinInt32),
			expectedErr: nil,
		},
		{
			name:        "int64 max",
			v:           int64(math.MaxInt64),
			expected:    int64(math.MaxInt64),
			expectedErr: nil,
		},
		{
			name:        "int64 min",
			v:           int64(math.MinInt64),
			expected:    int64(math.MinInt64),
			expectedErr: nil,
		},

		// ===== unsigned ints =====
		{
			name:        "uint",
			v:           uint(math.MaxInt),
			expected:    int64(math.MaxInt),
			expectedErr: nil,
		},
		{
			name:        "uint max uint",
			v:           uint(math.MaxUint),
			expected:    int64(-1),
			expectedErr: nil,
		},
		{
			name:        "uint8 max",
			v:           uint8(math.MaxUint8),
			expected:    int64(math.MaxUint8),
			expectedErr: nil,
		},
		{
			name:        "uint16 max",
			v:           uint16(math.MaxUint16),
			expected:    int64(math.MaxUint16),
			expectedErr: nil,
		},
		{
			name:        "uint32 max",
			v:           uint32(math.MaxUint32),
			expected:    int64(math.MaxUint32),
			expectedErr: nil,
		},
		{
			name:        "uint64 max int64",
			v:           uint64(math.MaxInt64),
			expected:    int64(math.MaxInt64),
			expectedErr: nil,
		},
		{
			name:        "uint64 max overflow",
			v:           uint64(math.MaxUint64),
			expected:    int64(-1),
			expectedErr: nil,
		},

		// ===== bool =====
		{
			name:        "bool true",
			v:           true,
			expected:    int64(1),
			expectedErr: nil,
		},
		{
			name:        "bool false",
			v:           false,
			expected:    int64(0),
			expectedErr: nil,
		},

		// ===== string =====
		{
			name:        "string max int64",
			v:           strconv.FormatInt(math.MaxInt64, 10),
			expected:    int64(math.MaxInt64),
			expectedErr: nil,
		},
		{
			name:        "string min int64",
			v:           strconv.FormatInt(math.MinInt64, 10),
			expected:    int64(math.MinInt64),
			expectedErr: nil,
		},
		{
			name:        "string overflow",
			v:           strconv.FormatUint(math.MaxInt64+1, 10),
			expected:    int64(0),
			expectedErr: fmt.Errorf("failed to change string %v to int64: %v", strconv.FormatUint(math.MaxInt64+1, 10), &strconv.NumError{Func: "ParseInt", Num: strconv.FormatUint(math.MaxInt64+1, 10), Err: strconv.ErrRange}),
		},
		{
			name:        "string invalid",
			v:           "abc",
			expected:    int64(0),
			expectedErr: fmt.Errorf("failed to change string %v to int64: %v", "abc", &strconv.NumError{Func: "ParseInt", Num: "abc", Err: strconv.ErrSyntax}),
		},
		{
			name:        "string leading space",
			v:           " 123",
			expected:    int64(0),
			expectedErr: fmt.Errorf("failed to change string %v to int64: %v", " 123", &strconv.NumError{Func: "ParseInt", Num: " 123", Err: strconv.ErrSyntax}),
		},

		// ===== pointer =====
		{
			name: "pointer nested",
			v: func() any {
				var x any = int64(10)
				var y any = &x
				return &y
			}(),
			expected:    int64(10),
			expectedErr: nil,
		},
		{
			name: "pointer nil",
			v: func() any {
				var x any
				return &x
			}(),
			expected:    int64(0),
			expectedErr: fmt.Errorf("failed to change %v (type:%T) to int64", nil, nil),
		},

		// ===== []uint8 =====
		{
			name:        "byte slice int",
			v:           []uint8("123"),
			expected:    int64(123),
			expectedErr: nil,
		},
		{
			name:        "byte slice uint fallback (version>5)",
			v:           []uint8("18446744073709551615"),
			expected:    int64(-1),
			expectedErr: nil,
		},
		{
			name:        "byte slice negative (version>5)",
			v:           []uint8("-123"),
			expected:    int64(-123),
			expectedErr: nil,
		},
		{
			name:        "byte slice invalid backward compatibility",
			v:           []uint8("abc"),
			expected:    int64(0),
			expectedErr: fmt.Errorf("failed to change %v (type:%T) to int64", []uint8("abc"), []uint8("abc")),
		},
		{
			name:        "byte slice empty",
			v:           []uint8(""),
			expected:    int64(0),
			expectedErr: fmt.Errorf("failed to change %v (type:%T) to int64", []uint8(""), []uint8("")),
		},
		{
			name:        "byte slice int backward compatibility",
			v:           []uint8("123"),
			expected:    int64(0),
			expectedErr: fmt.Errorf("failed to change %v (type:%T) to int64", []uint8("123"), []uint8("123")),
			version:     5,
		},

		// ===== unsupported =====
		{
			name:        "unsupported type",
			v:           []int{1, 2, 3},
			expected:    int64(0),
			expectedErr: fmt.Errorf("failed to change %v (type:%T) to int64", []int{1, 2, 3}, []int{1, 2, 3}),
		},
		{
			name:        "nil input",
			v:           nil,
			expected:    int64(0),
			expectedErr: fmt.Errorf("failed to change %v (type:%T) to int64", nil, nil),
		},
	}

	old := constants.LoadedStateVersion
	t.Cleanup(func() { constants.LoadedStateVersion = old })

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			constants.LoadedStateVersion = utils.Ternary(tc.version != 0, tc.version, constants.LatestStateVersion).(int)
			result, err := ReformatInt64(tc.v)

			if tc.expectedErr != nil {
				assert.Equal(t, tc.expectedErr, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}

// TestReformatInt32 tests the ReformatInt32 function
func TestReformatInt32(t *testing.T) {
	tests := []struct {
		name        string
		v           any
		expected    int32
		expectedErr error
	}{
		// ===== float =====
		{
			name:        "float32",
			v:           float32(5.9),
			expected:    int32(5),
			expectedErr: nil,
		},
		{
			name:        "float32 negative",
			v:           float32(-5.9),
			expected:    int32(-5),
			expectedErr: nil,
		},
		{
			name:        "float64",
			v:           float64(10.9),
			expected:    int32(10),
			expectedErr: nil,
		},
		{
			name:        "float64 negative",
			v:           float64(-10.9),
			expected:    int32(-10),
			expectedErr: nil,
		},

		// ===== signed ints =====
		{
			name:        "int",
			v:           int(math.MaxInt),
			expected:    int32(-1),
			expectedErr: nil,
		},
		{
			name:        "int8 max",
			v:           int8(math.MaxInt8),
			expected:    int32(math.MaxInt8),
			expectedErr: nil,
		},
		{
			name:        "int8 min",
			v:           int8(math.MinInt8),
			expected:    int32(math.MinInt8),
			expectedErr: nil,
		},
		{
			name:        "int16 max",
			v:           int16(math.MaxInt16),
			expected:    int32(math.MaxInt16),
			expectedErr: nil,
		},
		{
			name:        "int16 min",
			v:           int16(math.MinInt16),
			expected:    int32(math.MinInt16),
			expectedErr: nil,
		},
		{
			name:        "int32 max",
			v:           int32(math.MaxInt32),
			expected:    int32(math.MaxInt32),
			expectedErr: nil,
		},
		{
			name:        "int32 min",
			v:           int32(math.MinInt32),
			expected:    int32(math.MinInt32),
			expectedErr: nil,
		},
		{
			name:        "int64 max",
			v:           int64(math.MaxInt64),
			expected:    int32(-1),
			expectedErr: nil,
		},
		{
			name:        "int64 min",
			v:           int64(math.MinInt64),
			expected:    int32(0),
			expectedErr: nil,
		},

		// ===== unsigned ints =====
		{
			name:        "uint",
			v:           uint(math.MaxInt),
			expected:    int32(-1),
			expectedErr: nil,
		},
		{
			name:        "uint8 max",
			v:           uint8(math.MaxUint8),
			expected:    int32(math.MaxUint8),
			expectedErr: nil,
		},
		{
			name:        "uint16 max",
			v:           uint16(math.MaxUint16),
			expected:    int32(math.MaxUint16),
			expectedErr: nil,
		},
		{
			name:        "uint32",
			v:           uint32(100),
			expected:    int32(100),
			expectedErr: nil,
		},
		{
			name:        "uint32 max",
			v:           uint32(math.MaxUint32),
			expected:    int32(-1),
			expectedErr: nil,
		},
		{
			name:        "uint64",
			v:           uint64(999),
			expected:    int32(999),
			expectedErr: nil,
		},
		{
			name:        "uint64 max",
			v:           uint64(math.MaxUint64),
			expected:    int32(-1),
			expectedErr: nil,
		},

		// ===== bool =====
		{
			name:        "bool true",
			v:           true,
			expected:    int32(1),
			expectedErr: nil,
		},
		{
			name:        "bool false",
			v:           false,
			expected:    int32(0),
			expectedErr: nil,
		},

		// ===== string =====
		{
			name:        "string max int32",
			v:           strconv.FormatInt(math.MaxInt32, 10),
			expected:    int32(math.MaxInt32),
			expectedErr: nil,
		},
		{
			name:        "string min int32",
			v:           strconv.FormatInt(math.MinInt32, 10),
			expected:    int32(math.MinInt32),
			expectedErr: nil,
		},
		{
			name:        "string overflow",
			v:           strconv.FormatInt(math.MaxInt32+1, 10),
			expected:    int32(0),
			expectedErr: fmt.Errorf("failed to change string %v to int32: %v", strconv.FormatInt(math.MaxInt32+1, 10), &strconv.NumError{Func: "ParseInt", Num: strconv.FormatInt(math.MaxInt32+1, 10), Err: strconv.ErrRange}),
		},
		{
			name:        "string negative float",
			v:           strconv.FormatFloat(math.SmallestNonzeroFloat64, 'f', -1, 64),
			expected:    int32(0),
			expectedErr: fmt.Errorf("failed to change string %v to int32: %v", strconv.FormatFloat(math.SmallestNonzeroFloat64, 'f', -1, 64), &strconv.NumError{Func: "ParseInt", Num: strconv.FormatFloat(math.SmallestNonzeroFloat64, 'f', -1, 64), Err: strconv.ErrSyntax}),
		},
		{
			name:        "string invalid",
			v:           "abc",
			expected:    int32(0),
			expectedErr: fmt.Errorf("failed to change string %v to int32: %v", "abc", &strconv.NumError{Func: "ParseInt", Num: "abc", Err: strconv.ErrSyntax}),
		},

		// ===== json.Number =====
		{
			name:        "json number max int32",
			v:           json.Number(strconv.FormatInt(math.MaxInt32, 10)),
			expected:    int32(math.MaxInt32),
			expectedErr: nil,
		},
		{
			name:        "json number min int32",
			v:           json.Number(strconv.FormatInt(math.MinInt32, 10)),
			expected:    int32(math.MinInt32),
			expectedErr: nil,
		},
		{
			name:        "json number overflow",
			v:           json.Number(strconv.FormatInt(math.MaxInt32+1, 10)),
			expected:    int32(math.MinInt32),
			expectedErr: nil,
		},
		{
			name:        "json number float",
			v:           json.Number("5.9"),
			expected:    int32(0),
			expectedErr: &strconv.NumError{Func: "ParseInt", Num: "5.9", Err: strconv.ErrSyntax},
		},
		{
			name:        "json number invalid",
			v:           json.Number("abc"),
			expected:    int32(0),
			expectedErr: &strconv.NumError{Func: "ParseInt", Num: "abc", Err: strconv.ErrSyntax},
		},

		// ===== []uint8 =====
		{
			name:        "byte slice int32",
			v:           []uint8("123"),
			expected:    int32(0),
			expectedErr: fmt.Errorf("unsupported []uint8 of length %d: %v", len([]uint8("123")), []uint8("123")),
		},
		{
			name:        "byte slice uint fallback",
			v:           []uint8(strconv.FormatUint(math.MaxUint32, 10)),
			expected:    int32(0),
			expectedErr: fmt.Errorf("unsupported []uint8 of length %d: %v", len([]uint8(strconv.FormatUint(math.MaxUint32, 10))), []uint8(strconv.FormatUint(math.MaxUint32, 10))),
		},
		{
			name:        "byte slice invalid",
			v:           []uint8("abc"),
			expected:    int32(0),
			expectedErr: fmt.Errorf("unsupported []uint8 of length %d: %v", len([]uint8("abc")), []uint8("abc")),
		},
		{
			name:        "byte slice empty",
			v:           []uint8(""),
			expected:    int32(0),
			expectedErr: fmt.Errorf("unsupported []uint8 of length %d: %v", len([]uint8("")), []uint8("")),
		},

		// ===== pointer =====
		{
			name: "pointer nested",
			v: func() any {
				var x any = int32(10)
				var y any = &x
				return &y
			}(),
			expected:    int32(10),
			expectedErr: nil,
		},
		{
			name: "pointer nil",
			v: func() any {
				var x any
				return &x
			}(),
			expected:    int32(0),
			expectedErr: fmt.Errorf("failed to change %v (type:%T) to int32", nil, nil),
		},

		// ===== unsupported =====
		{
			name:        "unsupported type",
			v:           []int{1, 2, 3},
			expected:    int32(0),
			expectedErr: fmt.Errorf("failed to change %v (type:%T) to int32", []int{1, 2, 3}, []int{1, 2, 3}),
		},
		{
			name:        "nil input",
			v:           nil,
			expected:    int32(0),
			expectedErr: fmt.Errorf("failed to change %v (type:%T) to int32", nil, nil),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ReformatInt32(tc.v)

			if tc.expectedErr != nil {
				assert.Equal(t, tc.expectedErr, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}

// TestReformatFloat64 tests the ReformatFloat64 function
func TestReformatFloat64(t *testing.T) {
	tests := []struct {
		name        string
		v           any
		expected    float64
		expectedErr error
	}{
		// ===== json.Number =====
		{
			name:        "json number integer",
			v:           json.Number(strconv.FormatInt(math.MaxInt64, 10)),
			expected:    float64(math.MaxInt64),
			expectedErr: nil,
		},
		{
			name:        "json number float",
			v:           json.Number(strconv.FormatFloat(math.MaxFloat64, 'f', -1, 64)),
			expected:    float64(math.MaxFloat64),
			expectedErr: nil,
		},
		{
			name:        "json number invalid",
			v:           json.Number("abc"),
			expected:    float64(0),
			expectedErr: &strconv.NumError{Func: "ParseFloat", Num: "abc", Err: strconv.ErrSyntax},
		},

		// ===== []uint8 =====
		{
			name:        "byte slice float",
			v:           []uint8("123.45"),
			expected:    float64(123.45),
			expectedErr: nil,
		},
		{
			name:        "byte slice integer",
			v:           []uint8("123"),
			expected:    float64(123),
			expectedErr: nil,
		},
		{
			name:        "byte slice negative",
			v:           []uint8("-123.45"),
			expected:    float64(-123.45),
			expectedErr: nil,
		},
		{
			name:        "byte slice invalid",
			v:           []uint8("abc"),
			expected:    float64(0),
			expectedErr: fmt.Errorf("failed to change []byte %v to float64: %v", []uint8("abc"), &strconv.NumError{Func: "ParseFloat", Num: "abc", Err: strconv.ErrSyntax}),
		},
		{
			name:        "byte slice empty",
			v:           []uint8(""),
			expected:    float64(0),
			expectedErr: fmt.Errorf("failed to change []byte %v to float64: %v", []uint8(""), &strconv.NumError{Func: "ParseFloat", Num: "", Err: strconv.ErrSyntax}),
		},

		// ===== float =====
		{
			name:        "float32",
			v:           float32(5.9),
			expected:    float64(float32(5.9)),
			expectedErr: nil,
		},
		{
			name:        "float32 max",
			v:           float32(math.MaxFloat32),
			expected:    float64(math.MaxFloat32),
			expectedErr: nil,
		},
		{
			name:        "float32 smallest nonzero",
			v:           float32(math.SmallestNonzeroFloat32),
			expected:    float64(math.SmallestNonzeroFloat32),
			expectedErr: nil,
		},
		{
			name:        "float32 negative",
			v:           float32(-5.9),
			expected:    float64(float32(-5.9)),
			expectedErr: nil,
		},
		{
			name:        "float64",
			v:           float64(math.MaxFloat64),
			expected:    float64(math.MaxFloat64),
			expectedErr: nil,
		},
		{
			name:        "float64 negative",
			v:           float64(math.SmallestNonzeroFloat64),
			expected:    float64(math.SmallestNonzeroFloat64),
			expectedErr: nil,
		},

		// ===== signed ints =====
		{
			name:        "int max",
			v:           int(math.MaxInt),
			expected:    float64(math.MaxInt),
			expectedErr: nil,
		},
		{
			name:        "int min",
			v:           int(math.MinInt),
			expected:    float64(math.MinInt),
			expectedErr: nil,
		},
		{
			name:        "int8 max",
			v:           int8(math.MaxInt8),
			expected:    float64(math.MaxInt8),
			expectedErr: nil,
		},
		{
			name:        "int8 min",
			v:           int8(math.MinInt8),
			expected:    float64(math.MinInt8),
			expectedErr: nil,
		},
		{
			name:        "int16 max",
			v:           int16(math.MaxInt16),
			expected:    float64(math.MaxInt16),
			expectedErr: nil,
		},
		{
			name:        "int16 min",
			v:           int16(math.MinInt16),
			expected:    float64(math.MinInt16),
			expectedErr: nil,
		},
		{
			name:        "int32 max",
			v:           int32(math.MaxInt32),
			expected:    float64(math.MaxInt32),
			expectedErr: nil,
		},
		{
			name:        "int32 min",
			v:           int32(math.MinInt32),
			expected:    float64(math.MinInt32),
			expectedErr: nil,
		},
		{
			name:        "int64",
			v:           int64(100),
			expected:    float64(100),
			expectedErr: nil,
		},
		{
			name:        "int64 large",
			v:           int64(math.MaxInt64),
			expected:    float64(math.MaxInt64),
			expectedErr: nil,
		},

		// ===== unsigned ints =====
		{
			name:        "uint",
			v:           uint(math.MaxUint),
			expected:    float64(math.MaxUint),
			expectedErr: nil,
		},
		{
			name:        "uint8 max",
			v:           uint8(math.MaxUint8),
			expected:    float64(math.MaxUint8),
			expectedErr: nil,
		},
		{
			name:        "uint16 max",
			v:           uint16(math.MaxUint16),
			expected:    float64(math.MaxUint16),
			expectedErr: nil,
		},
		{
			name:        "uint32",
			v:           uint32(math.MaxUint32),
			expected:    float64(math.MaxUint32),
			expectedErr: nil,
		},
		{
			name:        "uint64",
			v:           uint64(math.MaxUint64),
			expected:    float64(math.MaxUint64),
			expectedErr: nil,
		},

		// ===== bool =====
		{
			name:        "bool true",
			v:           true,
			expected:    float64(1),
			expectedErr: nil,
		},
		{
			name:        "bool false",
			v:           false,
			expected:    float64(0),
			expectedErr: nil,
		},

		// ===== string =====
		{
			name:        "string integer",
			v:           "123",
			expected:    float64(123),
			expectedErr: nil,
		},
		{
			name:        "string float",
			v:           "123.45",
			expected:    float64(123.45),
			expectedErr: nil,
		},
		{
			name:        "string negative float",
			v:           "-123.45",
			expected:    float64(-123.45),
			expectedErr: nil,
		},
		{
			name:        "string invalid",
			v:           "abc",
			expected:    float64(0),
			expectedErr: fmt.Errorf("failed to change string %v to float64: %v", "abc", &strconv.NumError{Func: "ParseFloat", Num: "abc", Err: strconv.ErrSyntax}),
		},
		{
			name:        "string leading space",
			v:           " 123",
			expected:    float64(0),
			expectedErr: fmt.Errorf("failed to change string %v to float64: %v", " 123", &strconv.NumError{Func: "ParseFloat", Num: " 123", Err: strconv.ErrSyntax}),
		},

		// ===== unsupported =====
		{
			name:        "unsupported type",
			v:           []int{1, 2, 3},
			expected:    float64(0),
			expectedErr: fmt.Errorf("failed to change %v (type:%T) to float64", []int{1, 2, 3}, []int{1, 2, 3}),
		},
		{
			name:        "nil input",
			v:           nil,
			expected:    float64(0),
			expectedErr: fmt.Errorf("failed to change %v (type:%T) to float64", nil, nil),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ReformatFloat64(tc.v)

			if tc.expectedErr != nil {
				assert.Equal(t, tc.expectedErr, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}

// TestReformatFloat32 tests the ReformatFloat32 function.
func TestReformatFloat32(t *testing.T) {
	tests := []struct {
		name        string
		v           any
		expected    float32
		expectedErr error
	}{
		// ===== json.Number =====
		{
			name:        "json number integer",
			v:           json.Number("123"),
			expected:    float32(123),
			expectedErr: nil,
		},
		{
			name:        "json number float",
			v:           json.Number("5.9"),
			expected:    float32(5.9),
			expectedErr: nil,
		},
		{
			name:        "json number invalid",
			v:           json.Number("abc"),
			expected:    float32(0),
			expectedErr: &strconv.NumError{Func: "ParseFloat", Num: "abc", Err: strconv.ErrSyntax},
		},

		// ===== []uint8 =====
		{
			name:        "byte slice float",
			v:           []uint8("123.45"),
			expected:    float32(123.45),
			expectedErr: nil,
		},
		{
			name:        "byte slice integer",
			v:           []uint8("123"),
			expected:    float32(123),
			expectedErr: nil,
		},
		{
			name:        "byte slice negative",
			v:           []uint8("-123.45"),
			expected:    float32(-123.45),
			expectedErr: nil,
		},
		{
			name:        "byte slice invalid",
			v:           []uint8("abc"),
			expected:    float32(0),
			expectedErr: fmt.Errorf("failed to change []byte %v to float32: %v", []uint8("abc"), &strconv.NumError{Func: "ParseFloat", Num: "abc", Err: strconv.ErrSyntax}),
		},
		{
			name:        "byte slice empty",
			v:           []uint8(""),
			expected:    float32(0),
			expectedErr: fmt.Errorf("failed to change []byte %v to float32: %v", []uint8(""), &strconv.NumError{Func: "ParseFloat", Num: "", Err: strconv.ErrSyntax}),
		},

		// ===== float =====
		{
			name:        "float32 max",
			v:           float32(math.MaxFloat32),
			expected:    float32(math.MaxFloat32),
			expectedErr: nil,
		},
		{
			name:        "float32 smallest nonzero",
			v:           float32(math.SmallestNonzeroFloat32),
			expected:    float32(math.SmallestNonzeroFloat32),
			expectedErr: nil,
		},
		{
			name:        "float32 negative",
			v:           float32(-5.9),
			expected:    float32(-5.9),
			expectedErr: nil,
		},
		{
			name:        "float64 smallest nonzero",
			v:           float64(math.SmallestNonzeroFloat64),
			expected:    float32(math.SmallestNonzeroFloat64),
			expectedErr: nil,
		},

		// ===== signed ints =====
		{
			name:        "int max",
			v:           int(math.MaxInt),
			expected:    float32(math.MaxInt),
			expectedErr: nil,
		},
		{
			name:        "int min",
			v:           int(math.MinInt),
			expected:    float32(math.MinInt),
			expectedErr: nil,
		},
		{
			name:        "int8 max",
			v:           int8(math.MaxInt8),
			expected:    float32(math.MaxInt8),
			expectedErr: nil,
		},
		{
			name:        "int8 min",
			v:           int8(math.MinInt8),
			expected:    float32(math.MinInt8),
			expectedErr: nil,
		},
		{
			name:        "int16 max",
			v:           int16(math.MaxInt16),
			expected:    float32(math.MaxInt16),
			expectedErr: nil,
		},
		{
			name:        "int16 min",
			v:           int16(math.MinInt16),
			expected:    float32(math.MinInt16),
			expectedErr: nil,
		},
		{
			name:        "int32 max",
			v:           int32(math.MaxInt32),
			expected:    float32(math.MaxInt32),
			expectedErr: nil,
		},
		{
			name:        "int32 min",
			v:           int32(math.MinInt32),
			expected:    float32(math.MinInt32),
			expectedErr: nil,
		},
		{
			name:        "int64",
			v:           int64(100),
			expected:    float32(100),
			expectedErr: nil,
		},
		{
			name:        "int64 large",
			v:           int64(math.MaxInt64),
			expected:    float32(math.MaxInt64),
			expectedErr: nil,
		},

		// ===== unsigned ints =====
		{
			name:        "uint",
			v:           uint(math.MaxUint),
			expected:    float32(math.MaxUint),
			expectedErr: nil,
		},
		{
			name:        "uint8 max",
			v:           uint8(math.MaxUint8),
			expected:    float32(math.MaxUint8),
			expectedErr: nil,
		},
		{
			name:        "uint16 max",
			v:           uint16(math.MaxUint16),
			expected:    float32(math.MaxUint16),
			expectedErr: nil,
		},
		{
			name:        "uint32 max",
			v:           uint32(math.MaxUint32),
			expected:    float32(math.MaxUint32),
			expectedErr: nil,
		},
		{
			name:        "uint64 max",
			v:           uint64(math.MaxUint64),
			expected:    float32(math.MaxUint64),
			expectedErr: nil,
		},

		// ===== bool =====
		{
			name:        "bool true",
			v:           true,
			expected:    float32(1),
			expectedErr: nil,
		},
		{
			name:        "bool false",
			v:           false,
			expected:    float32(0),
			expectedErr: nil,
		},

		// ===== string =====
		{
			name:        "string integer",
			v:           "123",
			expected:    float32(123),
			expectedErr: nil,
		},
		{
			name:        "string float",
			v:           "123.45",
			expected:    float32(123.45),
			expectedErr: nil,
		},
		{
			name:        "string negative float",
			v:           "-123.45",
			expected:    float32(-123.45),
			expectedErr: nil,
		},
		{
			name:        "string invalid",
			v:           "abc",
			expected:    float32(0),
			expectedErr: fmt.Errorf("failed to change string abc to float32: strconv.ParseFloat: parsing \"abc\": invalid syntax"),
		},
		{
			name:        "string leading space",
			v:           " 123",
			expected:    float32(0),
			expectedErr: fmt.Errorf("failed to change string  123 to float32: strconv.ParseFloat: parsing \" 123\": invalid syntax"),
		},

		// ===== unsupported =====
		{
			name:        "unsupported type",
			v:           []int{1, 2, 3},
			expected:    float32(0),
			expectedErr: fmt.Errorf("failed to change [1 2 3] (type:[]int) to float32"),
		},
		{
			name:        "nil input",
			v:           nil,
			expected:    float32(0),
			expectedErr: fmt.Errorf("failed to change <nil> (type:<nil>) to float32"),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ReformatFloat32(tc.v)

			if tc.expectedErr != nil {
				assert.Equal(t, tc.expectedErr, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}

// TestReformatByteArraysToString tests the ReformatByteArraysToString function.
func TestReformatByteArraysToString(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]any
		expected map[string]any
	}{
		// ===== map[string]any (recursive) =====
		{
			name: "nested map recursion",
			input: map[string]any{
				"outer": map[string]any{
					"inner": []byte("value"),
				},
			},
			expected: map[string]any{
				"outer": map[string]any{
					"inner": "value",
				},
			},
		},

		// ===== []byte =====
		{
			name: "byte slice",
			input: map[string]any{
				"key": []byte("hello"),
			},
			expected: map[string]any{
				"key": "hello",
			},
		},
		{
			name: "empty byte slice",
			input: map[string]any{
				"key": []byte(""),
			},
			expected: map[string]any{
				"key": "",
			},
		},

		// ===== []map[string]any =====
		{
			name: "slice of maps",
			input: map[string]any{
				"arr": []map[string]any{
					{"a": []byte("x")},
					{"b": []byte("y")},
					{"c": []byte("")},
					{"d": int64(123)},
				},
			},
			expected: map[string]any{
				"arr": []map[string]any{
					{"a": "x"},
					{"b": "y"},
					{"c": ""},
					{"d": int64(123)},
				},
			},
		},

		// ===== []any =====
		{
			name: "slice any with all supported types",
			input: map[string]any{
				"arr": []any{
					[]byte("hello"),

					map[string]any{
						"nested_byte": []byte("world"),
					},

					[]map[string]any{
						{"a": []byte("x")},
						{"b": []byte("y")},
					},

					[]any{
						[]byte("inner"),
						map[string]any{"k": []byte("v")},
					},

					int64(123),
					123,
					true,
				},
			},
			expected: map[string]any{
				"arr": []any{
					"hello",

					map[string]any{
						"nested_byte": "world",
					},

					[]map[string]any{
						{"a": []byte("x")},
						{"b": []byte("y")},
					},

					[]any{
						[]byte("inner"),
						map[string]any{"k": []byte("v")},
					},

					int64(123),
					123,
					true,
				},
			},
		},

		// ===== default / unchanged =====
		{
			name: "no byte arrays",
			input: map[string]any{
				"a": 1,
				"b": "text",
				"c": true,
			},
			expected: map[string]any{
				"a": 1,
				"b": "text",
				"c": true,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, ReformatByteArraysToString(tc.input))
		})
	}
}

// TestReformatGeoType tests the ReformatGeoType function.
func TestReformatGeoType(t *testing.T) {
	tests := []struct {
		name        string
		input       any
		expected    any
		expectedErr error
	}{
		// ===== nil =====
		{
			name:        "nil input",
			input:       nil,
			expected:    nil,
			expectedErr: ErrNullValue,
		},

		// ===== string =====
		{
			name:        "string input",
			input:       "POINT(1 2)",
			expected:    "POINT(1 2)",
			expectedErr: nil,
		},
		{
			name:        "string input with invalid time",
			input:       "x",
			expected:    "x",
			expectedErr: nil,
		},

		// ===== []byte =====
		{
			name:        "invalid wkb returns hex",
			input:       []byte{0x01, 0x02, 0x03},
			expected:    "010203",
			expectedErr: nil,
		},
		{
			name:        "invalid wkb with srid prefix",
			input:       []byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x02},
			expected:    "000000000102",
			expectedErr: nil,
		},
		{
			name:        "valid wkb point",
			input:       []byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40},
			expected:    "POINT(1 2)",
			expectedErr: nil,
		},

		// ===== pointer to value =====
		{
			name: "pointer to string",
			input: func() *any {
				var v any = "POINT(3 4)"
				return &v
			}(),
			expected:    "POINT(3 4)",
			expectedErr: nil,
		},
		{
			name: "nil pointer",
			input: func() *any {
				var v *any
				return v
			}(),
			expected:    nil,
			expectedErr: ErrNullValue,
		},
		{
			name: "pointer to byte slice",
			input: func() *any {
				var v any = []byte{0x01, 0x02}
				return &v
			}(),
			expected:    "0102",
			expectedErr: nil,
		},
		{
			name:        "int fallback",
			input:       123,
			expected:    "123",
			expectedErr: nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ReformatGeoType(tc.input)

			if tc.expectedErr != nil {
				assert.Equal(t, tc.expectedErr, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}

// TestReformatTimeValue tests the ReformatTimeValue function.
func TestReformatTimeValue(t *testing.T) {
	tests := []struct {
		name        string
		input       any
		expected    string
		expectedErr error
	}{
		// ===== time.Time =====
		{
			name:        "time value",
			input:       time.Date(2024, 1, 1, 14, 30, 45, 0, time.UTC),
			expected:    "14:30:45",
			expectedErr: nil,
		},

		// ===== []byte =====
		{
			name:        "byte slice",
			input:       []byte("12:34:56"),
			expected:    "12:34:56",
			expectedErr: nil,
		},

		// ===== string =====
		{
			name:        "string value",
			input:       "08:15:00",
			expected:    "08:15:00",
			expectedErr: nil,
		},
		{
			name:        "string value with invalid time",
			input:       "invalid time",
			expected:    "invalid time",
			expectedErr: nil,
		},

		// ===== default cases for fallback =====
		{
			name:        "int fallback",
			input:       123,
			expected:    "123",
			expectedErr: nil,
		},
		{
			name:        "nil input",
			input:       nil,
			expected:    "<nil>",
			expectedErr: nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ReformatTimeValue(tc.input)

			if tc.expectedErr != nil {
				assert.Equal(t, tc.expectedErr, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}
