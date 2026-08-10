package parser

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"math/big"
	"testing"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	pq "github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMapParquetTypeToOlake_LogicalTypes(t *testing.T) {
	// Covers every logical type the mapper handles: integers (signed/unsigned,
	// all bit widths), decimal (all backings), date, timestamp (all precisions),
	// time (all precisions), and the string-family (utf8/enum/json/bson/uuid),
	// plus list and map.
	fieldType := func(node pq.Node) pq.Type {
		schema := pq.NewSchema("test", pq.Group{"f": node})
		return schema.Fields()[0].Type()
	}

	tests := []struct {
		name     string
		node     pq.Node
		expected types.DataType
	}{
		// Signed integers: 8/16/32 fit Int32, 64 needs Int64.
		{name: "Signed int8 maps to Int32", node: pq.Int(8), expected: types.Int32},
		{name: "Signed int16 maps to Int32", node: pq.Int(16), expected: types.Int32},
		{name: "Signed int32 maps to Int32", node: pq.Int(32), expected: types.Int32},
		{name: "Signed int64 maps to Int64", node: pq.Int(64), expected: types.Int64},
		// Unsigned integers: 8/16 fit Int32, 32 must widen to Int64 (2^32-1
		// overflows int32), 64 maps to Int64.
		{name: "Unsigned int8 fits Int32", node: pq.Uint(8), expected: types.Int32},
		{name: "Unsigned int16 fits Int32", node: pq.Uint(16), expected: types.Int32},
		{name: "Unsigned int32 widens to Int64 to avoid overflow", node: pq.Uint(32), expected: types.Int64},
		{name: "Unsigned int64 maps to Int64", node: pq.Uint(64), expected: types.Int64},
		// Decimal (any physical backing) maps to Float64.
		{name: "Decimal int32-backed", node: pq.Decimal(2, 9, pq.Int32Type), expected: types.Float64},
		{name: "Decimal int64-backed", node: pq.Decimal(4, 18, pq.Int64Type), expected: types.Float64},
		{name: "Decimal fixed-backed", node: pq.Decimal(10, 38, pq.FixedLenByteArrayType(16)), expected: types.Float64},
		// Temporal types.
		{name: "Date", node: pq.Date(), expected: types.Timestamp},
		{name: "Timestamp millis", node: pq.Timestamp(pq.Millisecond), expected: types.TimestampMilli},
		{name: "Timestamp micros", node: pq.Timestamp(pq.Microsecond), expected: types.TimestampMicro},
		{name: "Timestamp nanos", node: pq.Timestamp(pq.Nanosecond), expected: types.TimestampNano},
		{name: "Time millis maps to Int64", node: pq.Time(pq.Millisecond), expected: types.Int64},
		{name: "Time micros maps to Int64", node: pq.Time(pq.Microsecond), expected: types.Int64},
		{name: "Time nanos maps to Int64", node: pq.Time(pq.Nanosecond), expected: types.Int64},
		// String-family logical types all map to String.
		{name: "String (UTF8)", node: pq.String(), expected: types.String},
		{name: "Enum maps to string", node: pq.Enum(), expected: types.String},
		{name: "JSON maps to string", node: pq.JSON(), expected: types.String},
		{name: "BSON maps to string", node: pq.BSON(), expected: types.String},
		{name: "UUID maps to string", node: pq.UUID(), expected: types.String},
		// Nested types.
		{name: "List maps to Array", node: pq.List(pq.String()), expected: types.Array},
		{name: "Map maps to Object", node: pq.Map(pq.String(), pq.Int(32)), expected: types.Object},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, mapParquetTypeToOlake(fieldType(tt.node)))
		})
	}
}

func TestMapParquetTypeToOlake_PhysicalTypes(t *testing.T) {
	tests := []struct {
		name        string
		pqType      pq.Type
		expected    types.DataType
		description string
	}{
		{
			name:        "Boolean physical type",
			pqType:      pq.BooleanType,
			expected:    types.Bool,
			description: "Boolean should map to Bool",
		},
		{
			name:        "Int32 physical type",
			pqType:      pq.Int32Type,
			expected:    types.Int32,
			description: "Int32 should map to Int32",
		},
		{
			name:        "Int64 physical type",
			pqType:      pq.Int64Type,
			expected:    types.Int64,
			description: "Int64 should map to Int64",
		},
		{
			name:        "Float physical type",
			pqType:      pq.FloatType,
			expected:    types.Float32,
			description: "Float should map to Float32",
		},
		{
			name:        "Double physical type",
			pqType:      pq.DoubleType,
			expected:    types.Float64,
			description: "Double should map to Float64",
		},
		{
			name:        "ByteArray physical type",
			pqType:      pq.ByteArrayType,
			expected:    types.String,
			description: "ByteArray should map to String",
		},
		{
			name:        "FixedLenByteArray physical type",
			pqType:      pq.FixedLenByteArrayType(4),
			expected:    types.String,
			description: "FixedLenByteArray without a logical type should map to String",
		},
		{
			name:        "Int96 physical type",
			pqType:      pq.Int96Type,
			expected:    types.Timestamp,
			description: "Int96 should map to Timestamp (legacy)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mapParquetTypeToOlake(tt.pqType)
			assert.Equal(t, tt.expected, result, tt.description)
		})
	}
}

func TestParquetValueToInterfaceWithType_Date(t *testing.T) {
	// Date: days since Unix epoch (1970-01-01)
	// Test with day 0 (1970-01-01) and day 19723 (2024-01-15)
	// We'll create a mock type with Date logical type by using schema creation
	schema := pq.NewSchema("test", pq.Group{
		"date_field": pq.Date(),
	})
	dateType := schema.Fields()[0].Type()

	tests := []struct {
		name        string
		days        int32
		expected    time.Time
		description string
	}{
		{
			name:        "Unix epoch date",
			days:        0,
			expected:    time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
			description: "Day 0 should be 1970-01-01",
		},
		{
			name:        "2024-01-15",
			days:        19737, // Days from 1970-01-01 to 2024-01-15 (calculated)
			expected:    time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
			description: "Should convert days to a time.Time like DB drivers emit",
		},
		{
			name:        "far future date 9999-12-31",
			days:        2932896,
			expected:    time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC),
			description: "Dates outside the int64-nanosecond range (1678-2262) must not overflow",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val := pq.Int32Value(tt.days)
			result := parquetValueToInterfaceWithType(val, dateType)
			resultTime, ok := result.(time.Time)
			require.True(t, ok, "Date should return time.Time, got %T", result)
			assert.True(t, tt.expected.Equal(resultTime), tt.description)
		})
	}
}

func TestParquetValueToInterfaceWithType_Timestamp(t *testing.T) {
	tests := []struct {
		name         string
		createSchema func() pq.Type
		rawValue     int64
		expected     time.Time
		description  string
	}{
		{
			name: "Timestamp nanoseconds",
			createSchema: func() pq.Type {
				schema := pq.NewSchema("test", pq.Group{
					"ts": pq.Timestamp(pq.Nanosecond),
				})
				return schema.Fields()[0].Type()
			},
			rawValue:    1705314645123456789, // nanoseconds: 1705314645 seconds + 123456789 nanos
			expected:    time.Date(2024, 1, 15, 10, 30, 45, 123456789, time.UTC),
			description: "Should convert nanoseconds to a timestamp",
		},
		{
			name: "Timestamp microseconds",
			createSchema: func() pq.Type {
				schema := pq.NewSchema("test", pq.Group{
					"ts": pq.Timestamp(pq.Microsecond),
				})
				return schema.Fields()[0].Type()
			},
			rawValue:    1705314645123456, // microseconds: 1705314645 seconds + 123456 micros
			expected:    time.Date(2024, 1, 15, 10, 30, 45, 123456000, time.UTC),
			description: "Should convert microseconds to a timestamp",
		},
		{
			name: "Timestamp milliseconds",
			createSchema: func() pq.Type {
				schema := pq.NewSchema("test", pq.Group{
					"ts": pq.Timestamp(pq.Millisecond),
				})
				return schema.Fields()[0].Type()
			},
			rawValue:    1705314645123, // milliseconds: 1705314645 seconds + 123 millis
			expected:    time.Date(2024, 1, 15, 10, 30, 45, 123000000, time.UTC),
			description: "Should convert milliseconds to a timestamp",
		},
		{
			name: "Timestamp seconds",
			createSchema: func() pq.Type {
				// For seconds, use millisecond precision with value in milliseconds
				schema := pq.NewSchema("test", pq.Group{
					"ts": pq.Timestamp(pq.Millisecond),
				})
				return schema.Fields()[0].Type()
			},
			rawValue:    1705314645000, // milliseconds for 2024-01-15 10:30:45 (exact second)
			expected:    time.Date(2024, 1, 15, 10, 30, 45, 0, time.UTC),
			description: "Should convert milliseconds to a timestamp (seconds precision)",
		},
		{
			name: "Timestamp micros far future does not overflow",
			createSchema: func() pq.Type {
				schema := pq.NewSchema("test", pq.Group{
					"ts": pq.Timestamp(pq.Microsecond),
				})
				return schema.Fields()[0].Type()
			},
			// Scaling micros into int64 nanoseconds overflows past ~year 2262 and wraps:
			// this value used to come back as 1816-03-30.
			rawValue:    253402300799000000,
			expected:    time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC),
			description: "Should carry a timestamp beyond the int64 nanosecond range",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val := pq.Int64Value(tt.rawValue)
			pqType := tt.createSchema()
			result := parquetValueToInterfaceWithType(val, pqType)

			resultTime, ok := result.(time.Time)
			require.True(t, ok, "Result should be a time.Time, got %T", result)
			// Compared exactly rather than within a tolerance: sub-second precision is the
			// point of these cases, and a tolerance wide enough to absorb rounding would
			// also absorb a timestamp truncated to whole seconds.
			assert.True(t, tt.expected.Equal(resultTime), "%s: expected %s, got %s",
				tt.description, tt.expected, resultTime)
		})
	}
}

func TestParquetValueToInterfaceWithType_Time(t *testing.T) {
	tests := []struct {
		name         string
		createSchema func() pq.Type
		rawValue     interface{} // int32 or int64
		expected     int64       // seconds
		description  string
	}{
		{
			name: "Time Int32 milliseconds",
			createSchema: func() pq.Type {
				schema := pq.NewSchema("test", pq.Group{
					"time": pq.Time(pq.Millisecond),
				})
				return schema.Fields()[0].Type()
			},
			rawValue:    int32(123456), // 123456 ms
			expected:    123,
			description: "Int32 millis returned as raw int64",
		},
		{
			name: "Time Int64 microseconds",
			createSchema: func() pq.Type {
				schema := pq.NewSchema("test", pq.Group{
					"time": pq.Time(pq.Microsecond),
				})
				return schema.Fields()[0].Type()
			},
			rawValue:    int64(123456789), // 123456789 µs
			expected:    123,
			description: "Int64 micros returned as raw int64",
		},
		{
			name: "Time Int64 nanoseconds",
			createSchema: func() pq.Type {
				schema := pq.NewSchema("test", pq.Group{
					"time": pq.Time(pq.Nanosecond),
				})
				return schema.Fields()[0].Type()
			},
			rawValue:    int64(123456789000), // 123456789000 ns
			expected:    123,
			description: "Int64 nanos returned as raw int64",
		},
		{
			name: "Time whole seconds",
			createSchema: func() pq.Type {
				schema := pq.NewSchema("test", pq.Group{
					"time": pq.Time(pq.Millisecond),
				})
				return schema.Fields()[0].Type()
			},
			rawValue:    int64(12345000), // 12345000 ms = 03:25:45.000
			expected:    12345,
			description: "Value in seconds",
		},
		{
			name: "Time not adjusted to UTC",
			createSchema: func() pq.Type {
				schema := pq.NewSchema("test", pq.Group{
					"time": pq.TimeAdjusted(pq.Microsecond, false),
				})
				return schema.Fields()[0].Type()
			},
			rawValue:    int64(49530123456), // 13:45:30.123456 in micros
			expected:    49530,
			description: "isAdjustedToUTC=false TIME columns should behave identically",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var val pq.Value
			if intVal, ok := tt.rawValue.(int32); ok {
				val = pq.Int32Value(intVal)
			} else {
				val = pq.Int64Value(tt.rawValue.(int64))
			}

			pqType := tt.createSchema()
			result := parquetValueToInterfaceWithType(val, pqType)

			resultInt, ok := result.(int64)
			require.True(t, ok, "Time should return an int64, got %T", result)
			assert.Equal(t, tt.expected, resultInt, tt.description)
		})
	}
}

func TestParquetValueToInterfaceWithType_Int96(t *testing.T) {
	// INT96 legacy timestamp: low 64 bits = nanoseconds-of-day, high 32 bits =
	// Julian day. The value must come back as time.Time so it agrees with the
	// schema (Timestamp) instead of collapsing the column to a raw-integer string.
	tests := []struct {
		name       string
		julianDay  uint32
		nanosOfDay int64
		expected   time.Time
	}{
		{
			name:       "2024-06-15 13:45:30.123456789",
			julianDay:  2460477, // 19889 days since epoch + 2440588
			nanosOfDay: 49530123456789,
			expected:   time.Date(2024, 6, 15, 13, 45, 30, 123456789, time.UTC),
		},
		{
			name:       "midnight epoch",
			julianDay:  2440588, // Unix epoch
			nanosOfDay: 0,
			expected:   time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:       "far future 9999-12-31 23:59:59",
			julianDay:  5373484, // 2932896 days since epoch + 2440588
			nanosOfDay: (23*3600 + 59*60 + 59) * 1_000_000_000,
			expected:   time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i96 := deprecated.Int64ToInt96(tt.nanosOfDay)
			i96[2] = tt.julianDay
			result := parquetValueToInterfaceWithType(pq.Int96Value(i96), pq.Int96Type)

			resultTime, ok := result.(time.Time)
			require.True(t, ok, "Int96 should return time.Time, got %T", result)
			assert.True(t, tt.expected.Equal(resultTime),
				"expected %s, got %s", tt.expected.Format(time.RFC3339Nano), resultTime.Format(time.RFC3339Nano))
		})
	}
}

func TestParquetValueToInterfaceWithType_UUID(t *testing.T) {
	// UUID must format as canonical 8-4-4-4-12 hex. The 16 raw bytes below are all
	// < 0x80 (valid UTF-8), so without UUID handling they'd surface as a
	// control-char string via the generic byte-array path.
	schema := pq.NewSchema("test", pq.Group{"u": pq.UUID()})
	uuidType := schema.Fields()[0].Type()

	raw := []byte{0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f}
	result := parquetValueToInterfaceWithType(pq.FixedLenByteArrayValue(raw), uuidType)

	resultStr, ok := result.(string)
	require.True(t, ok, "UUID should return a string, got %T", result)
	assert.Equal(t, "10111213-1415-1617-1819-1a1b1c1d1e1f", resultStr)
}

func TestParquetValueToInterfaceWithType_JSON(t *testing.T) {
	schema := pq.NewSchema("test", pq.Group{
		"j": pq.JSON(),
	})
	jsonType := schema.Fields()[0].Type()

	raw := []byte(`{"name":"alice","age":30}`)
	result := parquetValueToInterfaceWithType(
		pq.ByteArrayValue(raw),
		jsonType,
	)

	resultStr, ok := result.(string)
	require.True(t, ok, "JSON should return a string, got %T", result)
	assert.Equal(t, `{"name":"alice","age":30}`, resultStr)
}

func TestParquetValueToInterfaceWithType_UnsignedIntegers(t *testing.T) {
	// Unsigned 8/16 fit int32; unsigned 32 must widen to int64 (values above
	// 2^31-1 would otherwise wrap negative); unsigned 64 returns int64.
	fieldType := func(bitWidth int) pq.Type {
		schema := pq.NewSchema("test", pq.Group{"u": pq.Uint(bitWidth)})
		return schema.Fields()[0].Type()
	}

	tests := []struct {
		name     string
		bitWidth int
		val      pq.Value
		expected interface{}
	}{
		{name: "uint8 stays int32", bitWidth: 8, val: pq.Int32Value(200), expected: int32(200)},
		{name: "uint16 stays int32", bitWidth: 16, val: pq.Int32Value(60000), expected: int32(60000)},
		{name: "uint32 small widens to int64", bitWidth: 32, val: pq.Int32Value(100), expected: int64(100)},
		{name: "uint32 max signed", bitWidth: 32, val: pq.Int32Value(2147483647), expected: int64(2147483647)},
		{name: "uint32 2^31 must not wrap negative", bitWidth: 32, val: pq.Int32Value(-2147483648), expected: int64(2147483648)},
		{name: "uint32 max", bitWidth: 32, val: pq.Int32Value(-1), expected: int64(4294967295)},
		{name: "uint64 returns int64", bitWidth: 64, val: pq.Int64Value(9223372036854775807), expected: int64(9223372036854775807)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parquetValueToInterfaceWithType(tt.val, fieldType(tt.bitWidth))
			assert.Equal(t, tt.expected, result, tt.name)
		})
	}
}

func TestParquetValueToInterfaceWithType_StringFamily(t *testing.T) {
	// Enum/JSON/FIXED_LEN_BYTE_ARRAY all surface as strings: valid UTF-8 verbatim,
	// binary as base64 (same rule as plain BYTE_ARRAY).
	fieldType := func(node pq.Node) pq.Type {
		schema := pq.NewSchema("test", pq.Group{"f": node})
		return schema.Fields()[0].Type()
	}

	tests := []struct {
		name     string
		node     pq.Node
		value    pq.Value
		expected string
	}{
		{name: "Enum value", node: pq.Enum(), value: pq.ByteArrayValue([]byte("RED")), expected: "RED"},
		{name: "JSON value", node: pq.JSON(), value: pq.ByteArrayValue([]byte(`{"a":1}`)), expected: `{"a":1}`},
		{name: "FixedLen UTF-8", node: pq.Leaf(pq.FixedLenByteArrayType(4)), value: pq.FixedLenByteArrayValue([]byte("abcd")), expected: "abcd"},
		{name: "FixedLen binary as base64", node: pq.Leaf(pq.FixedLenByteArrayType(4)), value: pq.FixedLenByteArrayValue([]byte{0x01, 0xAA, 0xBB, 0xCC}), expected: "Aaq7zA=="},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parquetValueToInterfaceWithType(tt.value, fieldType(tt.node))
			resultStr, ok := result.(string)
			require.True(t, ok, "%s should return a string, got %T", tt.name, result)
			assert.Equal(t, tt.expected, resultStr, tt.name)
		})
	}
}

func TestDecodeParquetDecimal(t *testing.T) {
	tests := []struct {
		name        string
		createValue func() pq.Value
		scale       int32
		expected    string // decimal string representation
		expectError bool
		description string
	}{
		{
			name: "Decimal from Int32",
			createValue: func() pq.Value {
				return pq.Int32Value(12345) // scale 2 means 123.45
			},
			scale:       2,
			expected:    "123.45",
			expectError: false,
			description: "Should decode Int32 decimal with scale",
		},
		{
			name: "Decimal from Int64",
			createValue: func() pq.Value {
				return pq.Int64Value(1234567890) // scale 3 means 1234567.890
			},
			scale:       3,
			expected:    "1234567.890",
			expectError: false,
			description: "Should decode Int64 decimal with scale",
		},
		{
			name: "Decimal from ByteArray (positive)",
			createValue: func() pq.Value {
				// Represent 12345 with scale 2 = 123.45
				bigInt := big.NewInt(12345)
				bytes := bigInt.Bytes()
				return pq.ByteArrayValue(bytes)
			},
			scale:       2,
			expected:    "123.45",
			expectError: false,
			description: "Should decode ByteArray decimal (positive)",
		},
		{
			name: "Decimal from ByteArray (negative, two's complement)",
			createValue: func() pq.Value {
				// Represent -12345 with scale 2 = -123.45
				// Using two's complement for negative numbers
				bigInt := big.NewInt(-12345)
				bytes := make([]byte, 2)
				bigInt.FillBytes(bytes)
				// Set sign bit for two's complement
				if bytes[0]&0x80 == 0 {
					bytes[0] |= 0x80
				}
				return pq.ByteArrayValue(bytes)
			},
			scale:       2,
			expectError: false,
			description: "Should decode ByteArray decimal (negative with two's complement)",
		},
		{
			name: "Decimal from empty ByteArray",
			createValue: func() pq.Value {
				return pq.ByteArrayValue([]byte{})
			},
			scale:       2,
			expected:    "0",
			expectError: false,
			description: "Empty ByteArray should return zero",
		},
		{
			name: "Decimal from FixedLenByteArray",
			createValue: func() pq.Value {
				// 16-byte big-endian two's complement of 12345 (scale 2 = 123.45)
				raw := make([]byte, 16)
				big.NewInt(12345).FillBytes(raw)
				return pq.FixedLenByteArrayValue(raw)
			},
			scale:       2,
			expected:    "123.45",
			expectError: false,
			description: "Should decode FixedLenByteArray decimal",
		},
		{
			name: "Decimal from FixedLenByteArray (negative, two's complement)",
			createValue: func() pq.Value {
				// 16-byte big-endian two's complement of -12345 (scale 2 = -123.45)
				neg := new(big.Int).Add(new(big.Int).Lsh(big.NewInt(1), 128), big.NewInt(-12345))
				raw := make([]byte, 16)
				neg.FillBytes(raw)
				return pq.FixedLenByteArrayValue(raw)
			},
			scale:       2,
			expected:    "-123.45",
			expectError: false,
			description: "Should decode negative FixedLenByteArray decimal via two's complement",
		},
		{
			name: "Decimal from unsupported type",
			createValue: func() pq.Value {
				return pq.BooleanValue(true)
			},
			scale:       2,
			expectError: true,
			description: "Should error on unsupported decimal type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val := tt.createValue()
			result, err := decodeParquetDecimal(val, tt.scale)

			if tt.expectError {
				assert.Error(t, err, tt.description)
			} else {
				assert.NoError(t, err, tt.description)
				if tt.expected != "" {
					expectedDec, err := decimal.NewFromString(tt.expected)
					require.NoError(t, err)
					assert.True(t, result.Equal(expectedDec),
						"Expected %s, got %s", tt.expected, result.String())
				} else {
					// For negative two's complement test, just verify it's not zero
					assert.False(t, result.IsZero(), "Result should not be zero")
				}
			}
		})
	}
}

func TestParquetValueToInterfaceWithType_Decimal(t *testing.T) {
	// Note: pq.Decimal signature appears to be (precision, scale, type)
	// But the scale field in the logical type might be read differently
	// Let's test with a simpler approach - verify decimal conversion works
	schema := pq.NewSchema("test", pq.Group{
		"decimal": pq.Decimal(10, 2, pq.Int64Type),
	})
	decimalType := schema.Fields()[0].Type()

	// Get the actual scale from the schema
	logicalType := decimalType.LogicalType()
	require.NotNil(t, logicalType, "Should have logical type")
	require.NotNil(t, logicalType.Decimal, "Should be decimal type")
	actualScale := logicalType.Decimal.Scale

	// Use a value that works with the actual scale
	// If scale is 10, then 1234500000000 with scale 10 = 123.45
	// If scale is 2, then 12345 with scale 2 = 123.45
	var testValue int64
	var expectedResult float64

	if actualScale == 10 {
		testValue = 1234500000000 // Will give 123.45 with scale 10
		expectedResult = 123.45
	} else if actualScale == 2 {
		testValue = 12345 // Will give 123.45 with scale 2
		expectedResult = 123.45
	} else {
		// Calculate expected based on scale
		testValue = 12345
		expectedResult = float64(testValue) / float64(pow10ForTest(actualScale))
	}

	val := pq.Int64Value(testValue)

	// Test the decodeParquetDecimal function directly
	dec, err := decodeParquetDecimal(val, actualScale)
	require.NoError(t, err)
	directResult, _ := dec.Float64()

	// Now test through the full conversion
	result := parquetValueToInterfaceWithType(val, decimalType)

	resultFloat, ok := result.(float64)
	require.True(t, ok, "Result should be float64")

	// Verify both match
	assert.InDelta(t, directResult, resultFloat, 0.0001,
		"Full conversion should match direct decode. Got %f, expected %f", resultFloat, directResult)

	// If we calculated expectedResult, verify it
	if expectedResult > 0 {
		assert.InDelta(t, expectedResult, resultFloat, 0.01,
			"Should convert decimal correctly. Got %f, expected %f (scale: %d)",
			resultFloat, expectedResult, actualScale)
	}
}

// Helper function for testing
func pow10ForTest(n int32) int64 {
	result := int64(1)
	for i := int32(0); i < n; i++ {
		result *= 10
	}
	return result
}

func TestParquetValueToInterfaceWithType_ByteArray(t *testing.T) {
	tests := []struct {
		name        string
		data        []byte
		expected    interface{}
		checkType   func(interface{}) bool
		description string
	}{
		{
			name:     "Valid UTF-8 string",
			data:     []byte("Hello, World!"),
			expected: "Hello, World!",
			checkType: func(v interface{}) bool {
				_, ok := v.(string)
				return ok
			},
			description: "Valid UTF-8 should return as string",
		},
		{
			name:     "Invalid UTF-8 (binary data)",
			data:     []byte{0xFF, 0xFE, 0xFD},
			expected: "//79", // Base64 encoding of [0xFF, 0xFE, 0xFD]
			checkType: func(v interface{}) bool {
				str, ok := v.(string)
				if !ok {
					return false
				}
				// Should be base64 encoded - verify it's valid base64 and can be decoded
				decoded, err := base64.StdEncoding.DecodeString(str)
				if err != nil {
					return false
				}
				// Verify decoded data matches original
				return len(decoded) == 3 && decoded[0] == 0xFF && decoded[1] == 0xFE && decoded[2] == 0xFD
			},
			description: "Invalid UTF-8 should be base64 encoded",
		},
		{
			name:     "Empty byte array",
			data:     []byte{},
			expected: "",
			checkType: func(v interface{}) bool {
				_, ok := v.(string)
				return ok
			},
			description: "Empty byte array should return empty string",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val := pq.ByteArrayValue(tt.data)
			result := parquetValueToInterfaceWithType(val, pq.ByteArrayType)

			assert.True(t, tt.checkType(result), tt.description)
			if tt.expected != "" {
				assert.Equal(t, tt.expected, result, tt.description)
			}
		})
	}
}

func TestParquetValueToInterfaceWithType_PhysicalTypes(t *testing.T) {
	tests := []struct {
		name        string
		createValue func() pq.Value
		pqType      pq.Type
		expected    interface{}
		checkType   func(interface{}) bool
		description string
	}{
		{
			name: "Boolean true",
			createValue: func() pq.Value {
				return pq.BooleanValue(true)
			},
			pqType:   pq.BooleanType,
			expected: true,
			checkType: func(v interface{}) bool {
				_, ok := v.(bool)
				return ok
			},
			description: "Boolean true should return bool",
		},
		{
			name: "Int32 value",
			createValue: func() pq.Value {
				return pq.Int32Value(12345)
			},
			pqType:   pq.Int32Type,
			expected: int32(12345),
			checkType: func(v interface{}) bool {
				_, ok := v.(int32)
				return ok
			},
			description: "Int32 should keep its native width so the destination does not promote int -> long",
		},
		{
			name: "Int64 value",
			createValue: func() pq.Value {
				return pq.Int64Value(9223372036854775807)
			},
			pqType:   pq.Int64Type,
			expected: int64(9223372036854775807),
			checkType: func(v interface{}) bool {
				_, ok := v.(int64)
				return ok
			},
			description: "Int64 should return int64",
		},
		{
			name: "Float value",
			createValue: func() pq.Value {
				return pq.FloatValue(3.14)
			},
			pqType: pq.FloatType,
			checkType: func(v interface{}) bool {
				_, ok := v.(float32)
				return ok
			},
			description: "Float should keep its native width so the destination does not promote float -> double",
		},
		{
			name: "Double value",
			createValue: func() pq.Value {
				return pq.DoubleValue(3.141592653589793)
			},
			pqType:   pq.DoubleType,
			expected: 3.141592653589793,
			checkType: func(v interface{}) bool {
				_, ok := v.(float64)
				return ok
			},
			description: "Double should return float64",
		},
		{
			name: "Null value",
			createValue: func() pq.Value {
				return pq.NullValue()
			},
			pqType:   pq.Int32Type,
			expected: nil,
			checkType: func(v interface{}) bool {
				return v == nil
			},
			description: "Null value should return nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val := tt.createValue()
			result := parquetValueToInterfaceWithType(val, tt.pqType)

			assert.True(t, tt.checkType(result), tt.description)
			if tt.expected != nil {
				if expectedFloat, ok := tt.expected.(float64); ok {
					resultFloat, _ := result.(float64)
					assert.InDelta(t, expectedFloat, resultFloat, 0.0001, tt.description)
				} else {
					assert.Equal(t, tt.expected, result, tt.description)
				}
			}
		})
	}
}

func TestParquetReaderWrapper(t *testing.T) {
	data := []byte("Hello, World! This is test data for ParquetReaderWrapper")
	readerAt := bytes.NewReader(data)
	wrapper := NewParquetReaderWrapper(readerAt, int64(len(data)))

	t.Run("ReadAt", func(t *testing.T) {
		buf := make([]byte, 5)
		n, err := wrapper.ReadAt(buf, 0)
		require.NoError(t, err)
		assert.Equal(t, 5, n)
		assert.Equal(t, "Hello", string(buf))
	})

	t.Run("Seek Start", func(t *testing.T) {
		pos, err := wrapper.Seek(10, io.SeekStart)
		require.NoError(t, err)
		assert.Equal(t, int64(10), pos)
		assert.Equal(t, int64(10), wrapper.offset)
	})

	t.Run("Seek Current", func(t *testing.T) {
		wrapper.offset = 5
		pos, err := wrapper.Seek(5, io.SeekCurrent)
		require.NoError(t, err)
		assert.Equal(t, int64(10), pos)
		assert.Equal(t, int64(10), wrapper.offset)
	})

	t.Run("Seek End", func(t *testing.T) {
		pos, err := wrapper.Seek(-5, io.SeekEnd)
		require.NoError(t, err)
		expected := int64(len(data)) - 5
		assert.Equal(t, expected, pos)
		assert.Equal(t, expected, wrapper.offset)
	})

	t.Run("Seek bounds checking", func(t *testing.T) {
		// Test negative offset
		pos, err := wrapper.Seek(-100, io.SeekStart)
		require.NoError(t, err)
		assert.Equal(t, int64(0), pos)
		assert.Equal(t, int64(0), wrapper.offset)

		// Test offset beyond size
		pos, err = wrapper.Seek(1000, io.SeekStart)
		require.NoError(t, err)
		assert.Equal(t, int64(len(data)), pos)
		assert.Equal(t, int64(len(data)), wrapper.offset)
	})

	t.Run("Read", func(t *testing.T) {
		wrapper.offset = 0
		buf := make([]byte, 5)
		n, err := wrapper.Read(buf)
		require.NoError(t, err)
		assert.Equal(t, 5, n)
		assert.Equal(t, "Hello", string(buf))
		assert.Equal(t, int64(5), wrapper.offset)
	})
}

func TestPrepareParquetReader(t *testing.T) {
	t.Run("Valid ReaderAt and Seeker", func(t *testing.T) {
		data := []byte("test data")
		reader := bytes.NewReader(data)

		readerAt, fileSize, err := prepareParquetReader(reader)
		require.NoError(t, err)
		assert.NotNil(t, readerAt)
		assert.Equal(t, int64(len(data)), fileSize)
	})

	t.Run("Invalid reader (not ReaderAt)", func(t *testing.T) {
		reader := bytes.NewBufferString("test")
		_, _, err := prepareParquetReader(reader)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "requires io.ReaderAt")
	})

	t.Run("ReaderAt without Seeker", func(t *testing.T) {
		// Create a ReaderAt that doesn't implement Seeker
		// bytes.Reader implements both, so we need a custom type
		ro := &readerAtOnly{data: []byte("test")}

		// This should fail because we need Seeker to determine file size
		_, _, err := prepareParquetReader(ro)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "requires io.Seeker")
	})
}

// readerAtOnly implements io.Reader and io.ReaderAt but NOT io.Seeker
type readerAtOnly struct {
	data   []byte
	offset int64
}

func (r *readerAtOnly) Read(p []byte) (n int, err error) {
	if r.offset >= int64(len(r.data)) {
		return 0, io.EOF
	}
	n = copy(p, r.data[r.offset:])
	r.offset += int64(n)
	if n < len(p) {
		err = io.EOF
	}
	return n, err
}

func (r *readerAtOnly) ReadAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, io.EOF
	}
	if off >= int64(len(r.data)) {
		return 0, io.EOF
	}
	n = copy(p, r.data[off:])
	if n < len(p) {
		err = io.EOF
	}
	return n, err
}

// TestParquetParser_Integration writes a real Parquet file exercising every
// datatype that parquet-go can emit via struct tags — int32/int64 and
// uint32/uint64, float/double, bool, string/enum/json/byte-array/fixed-length,
// decimal, date, all timestamp precisions, and all time precisions — then
// verifies InferSchema and StreamRecords emit the same types and values a DB
// driver would for equivalent columns. INT96, UUID, and the 8/16-bit integer
// widths cannot be written through parquet-go struct tags (the uuid tag doesn't
// emit the logical type); they are covered by the dedicated
// TestParquetValueToInterfaceWithType_* / TestMapParquetTypeToOlake_* tests.
func TestParquetParser_Integration(t *testing.T) {
	type allTypes struct {
		ID     int32   `parquet:"id"`
		Int32  int32   `parquet:"l_int32"`
		Int64  int64   `parquet:"l_int64"`
		UInt32 uint32  `parquet:"l_uint32"`
		UInt64 uint64  `parquet:"l_uint64"`
		Float  float32 `parquet:"p_float"`
		Double float64 `parquet:"p_double"`
		Bool   bool    `parquet:"p_bool"`
		Str    string  `parquet:"l_string"`
		Bytes  []byte  `parquet:"p_byte_array"`
		Fixed  [4]byte `parquet:"p_fixed_len"`

		Enum string `parquet:"l_enum,enum"`
		JSON string `parquet:"l_json,json"`

		DecI32 int32 `parquet:"l_dec_int32,decimal(2:9)"`
		DecI64 int64 `parquet:"l_dec_int64,decimal(4:18)"`

		Date   int32     `parquet:"l_date,date"`
		TSms   time.Time `parquet:"l_ts_millis,timestamp(millisecond)"`
		TSus   time.Time `parquet:"l_ts_micros,timestamp(microsecond)"`
		TSns   time.Time `parquet:"l_ts_nanos,timestamp(nanosecond)"`
		TimeMs int32     `parquet:"l_time_millis,time(millisecond)"`
		TimeUs int64     `parquet:"l_time_micros,time(microsecond)"`
		TimeNs int64     `parquet:"l_time_nanos,time(nanosecond)"`
	}

	tsMillis := time.Date(2024, 6, 15, 13, 45, 30, 123000000, time.UTC)
	tsMicros := time.Date(2024, 6, 15, 13, 45, 30, 123456000, time.UTC)
	tsNanos := time.Date(2024, 6, 15, 13, 45, 30, 123456789, time.UTC)
	rows := []allTypes{
		{
			ID:     7,
			Int32:  -32,
			Int64:  9007199254740993, // 2^53+1: not representable in float64
			UInt32: 2147483648,       // 2^31: overflows a signed int32, must widen to int64
			UInt64: 10000000000,
			Float:  3.14,
			Double: 2.718281828459045,
			Bool:   true,
			Str:    "olake",
			Bytes:  []byte{0x00, 0x01, 0xFF}, // invalid UTF-8 → base64
			Fixed:  [4]byte{0x01, 0xAA, 0xBB, 0xCC},
			Enum:   "GREEN",
			JSON:   `{"k":"v"}`,
			DecI32: 12345,     // decimal(scale=2) = 123.45
			DecI64: 123456789, // decimal(scale=4) = 12345.6789
			Date:   19889,     // 2024-06-15
			TSms:   tsMillis,
			TSus:   tsMicros,
			TSns:   tsNanos,
			TimeMs: 49530123,       // 13:45:30.123
			TimeUs: 49530123456,    // 13:45:30.123456
			TimeNs: 49530123456789, // 13:45:30.123456789
		},
	}

	var buf bytes.Buffer
	writer := pq.NewGenericWriter[allTypes](&buf)
	_, err := writer.Write(rows)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	fileBytes := buf.Bytes()
	newReader := func() *ParquetReaderWrapper {
		return NewParquetReaderWrapper(bytes.NewReader(fileBytes), int64(len(fileBytes)))
	}

	ctx := context.Background()
	stream := types.NewStream("test", "test", nil)
	parser := NewParquetParser(ParquetConfig{}, stream)

	_, err = parser.InferSchema(ctx, newReader())
	require.NoError(t, err)

	expectedTypes := map[string]types.DataType{
		"id":            types.Int32,
		"l_int32":       types.Int32,
		"l_int64":       types.Int64,
		"l_uint32":      types.Int64, // widened to avoid overflow
		"l_uint64":      types.Int64,
		"p_float":       types.Float32,
		"p_double":      types.Float64,
		"p_bool":        types.Bool,
		"l_string":      types.String,
		"p_byte_array":  types.String,
		"p_fixed_len":   types.String,
		"l_enum":        types.String,
		"l_json":        types.String,
		"l_dec_int32":   types.Float64,
		"l_dec_int64":   types.Float64,
		"l_date":        types.Timestamp,
		"l_ts_millis":   types.TimestampMilli,
		"l_ts_micros":   types.TimestampMicro,
		"l_ts_nanos":    types.TimestampNano,
		"l_time_millis": types.Int64,
		"l_time_micros": types.Int64,
		"l_time_nanos":  types.Int64,
	}
	for column, expected := range expectedTypes {
		actual, err := stream.Schema.GetType(column)
		require.NoError(t, err, "column %s missing from inferred schema", column)
		assert.Equal(t, expected, actual, "column %s", column)
	}

	records := []map[string]any{}
	err = parser.StreamRecords(ctx, newReader(), func(_ context.Context, record map[string]any) error {
		records = append(records, record)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, records, 1)

	record := records[0]
	// Integers keep their native Go width (no promotion at the destination).
	assert.Equal(t, int32(7), record["id"])
	assert.Equal(t, int32(-32), record["l_int32"])
	assert.Equal(t, int64(9007199254740993), record["l_int64"], "int64 beyond 2^53 must survive exactly")
	assert.Equal(t, int64(2147483648), record["l_uint32"], "unsigned 32-bit must not wrap negative")
	assert.Equal(t, int64(10000000000), record["l_uint64"])
	// Floats keep native width; bool/string verbatim.
	assert.Equal(t, float32(3.14), record["p_float"])
	assert.Equal(t, 2.718281828459045, record["p_double"])
	assert.Equal(t, true, record["p_bool"])
	assert.Equal(t, "olake", record["l_string"])
	// Byte arrays: base64 when not valid UTF-8.
	assert.Equal(t, "AAH/", record["p_byte_array"])
	assert.Equal(t, "Aaq7zA==", record["p_fixed_len"])
	// String-family logical types.
	assert.Equal(t, "GREEN", record["l_enum"])
	assert.Equal(t, `{"k":"v"}`, record["l_json"])
	// Decimals decode with their scale.
	assert.Equal(t, 123.45, record["l_dec_int32"])
	assert.Equal(t, 12345.6789, record["l_dec_int64"])
	// Time-of-day columns
	assert.Equal(t, int64(49530), record["l_time_millis"])
	assert.Equal(t, int64(49530), record["l_time_micros"])
	assert.Equal(t, int64(49530), record["l_time_nanos"])

	day, ok := record["l_date"].(time.Time)
	require.True(t, ok, "date should be time.Time, got %T", record["l_date"])
	assert.True(t, time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC).Equal(day))

	for column, want := range map[string]time.Time{"l_ts_millis": tsMillis, "l_ts_micros": tsMicros, "l_ts_nanos": tsNanos} {
		got, ok := record[column].(time.Time)
		require.True(t, ok, "%s should be time.Time, got %T", column, record[column])
		assert.True(t, want.Equal(got), "%s precision must survive: expected %s, got %s",
			column, want.Format(time.RFC3339Nano), got.Format(time.RFC3339Nano))
	}
}

type nestedTestRow struct {
	ID     int64             `parquet:"id"`
	Str    string            `parquet:"str"`
	List   []string          `parquet:"col_list,list"`
	Map    map[string]string `parquet:"col_map"`
	Nested struct {
		A string `parquet:"a"`
		B int64  `parquet:"b"`
	} `parquet:"col_struct"`
}

// TestParquetParser_NestedTypes covers the columns whose values do not sit one-per-row in a
// single leaf column. Reading those by column index used to mis-attribute them: a map or a
// nested struct panicked outright, and a list kept only its first element.
func TestParquetParser_NestedTypes(t *testing.T) {
	rows := make([]nestedTestRow, 0, 3)
	for i := int64(0); i < 3; i++ {
		var row nestedTestRow
		row.ID = i
		row.Str = "row"
		row.List = []string{"a", "b"}
		row.Map = map[string]string{"k": "v"}
		row.Nested.A = "x"
		row.Nested.B = 2
		rows = append(rows, row)
	}

	var buf bytes.Buffer
	writer := pq.NewGenericWriter[nestedTestRow](&buf)
	_, err := writer.Write(rows)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	stream := types.NewStream("nested", "nested", nil)
	parser := NewParquetParser(ParquetConfig{}, stream)

	_, err = parser.InferSchema(context.Background(), bytes.NewReader(buf.Bytes()))
	require.NoError(t, err, "group fields have no physical kind and must not panic")

	for column, expected := range map[string]types.DataType{
		"id": types.Int64, "str": types.String,
		"col_list": types.Array, "col_map": types.Object, "col_struct": types.Object,
	} {
		actual, err := stream.Schema.GetType(column)
		require.NoError(t, err)
		require.Equal(t, expected, actual, "column %s", column)
	}

	var records []map[string]any
	require.NoError(t, parser.StreamRecords(context.Background(), bytes.NewReader(buf.Bytes()),
		func(_ context.Context, record map[string]any) error {
			records = append(records, record)
			return nil
		}))

	require.Len(t, records, 3)
	for i, record := range records {
		require.Equal(t, int64(i), record["id"], "each row keeps its own values")
		require.Equal(t, []any{"a", "b"}, record["col_list"], "every list element is kept")
		require.Equal(t, map[string]any{"k": "v"}, record["col_map"])
		require.Equal(t, map[string]any{"a": "x", "b": int64(2)}, record["col_struct"])
	}
}

// nestedLogicalRow binds to the explicit schema in TestParquetParser_NestedLogicalTypes:
// logical types living inside groups, where reconstruction (unlike the plain-leaf path)
// hands back raw physical values unless the parser converts them itself.
type nestedLogicalRow struct {
	ID int64 `parquet:"id"`

	ListTS []int64 `parquet:"list_ts"`

	StructCol struct {
		D  int32    `parquet:"d"`
		TS int64    `parquet:"ts"`
		U  [16]byte `parquet:"u"`
		S  string   `parquet:"s"`
	} `parquet:"struct_col"`

	MapTS map[string]int64 `parquet:"map_ts"`

	RepTS []int64 `parquet:"rep_ts"`
}

// TestParquetParser_NestedLogicalTypes pins the conversion of logical types nested inside
// groups. Reconstruction assigns raw physical values into any-typed destinations (an int64
// tick count for a timestamp, an int32 day count for a date, unscaled big-endian bytes for
// a decimal), so without convertReconstructed a list<timestamp> surfaced integers and a
// nested decimal a garbage string, while the identical types at top level converted fine.
func TestParquetParser_NestedLogicalTypes(t *testing.T) {
	schema := pq.NewSchema("nested_logical", pq.Group{
		"id":      pq.Int(64),
		"list_ts": pq.List(pq.Timestamp(pq.Microsecond)),
		"struct_col": pq.Group{
			"d":  pq.Date(),
			"ts": pq.Timestamp(pq.Microsecond),
			"u":  pq.UUID(),
			"s":  pq.String(),
		},
		"map_ts": pq.Map(pq.String(), pq.Timestamp(pq.Microsecond)),
		"rep_ts": pq.Repeated(pq.Timestamp(pq.Microsecond)),
	})

	ts := time.Date(2024, 1, 15, 10, 30, 45, 123456000, time.UTC)
	uuidBytes := [16]byte{0x12, 0x3e, 0x45, 0x67, 0xe8, 0x9b, 0x12, 0xd3, 0xa4, 0x56, 0x42, 0x66, 0x14, 0x17, 0x40, 0x00}
	// 12345 at scale 2 = 123.45, as 16 byte big-endian two's complement.
	var decBytes [16]byte
	big.NewInt(12345).FillBytes(decBytes[:])

	var row nestedLogicalRow
	row.ID = 1
	row.ListTS = []int64{ts.UnixMicro(), ts.Add(time.Second).UnixMicro()}
	row.StructCol.D = 19737 // days from epoch to 2024-01-15
	row.StructCol.TS = ts.UnixMicro()
	row.StructCol.U = uuidBytes
	row.StructCol.S = "text"
	row.MapTS = map[string]int64{"k": ts.UnixMicro()}
	row.RepTS = []int64{ts.UnixMicro(), ts.Add(time.Minute).UnixMicro()}

	var buf bytes.Buffer
	writer := pq.NewGenericWriter[nestedLogicalRow](&buf, schema)
	_, err := writer.Write([]nestedLogicalRow{row})
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	stream := types.NewStream("nested_logical", "nested_logical", nil)
	parser := NewParquetParser(ParquetConfig{}, stream)

	_, err = parser.InferSchema(context.Background(), bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	for column, expected := range map[string]types.DataType{
		"list_ts": types.Array, "struct_col": types.Object, "map_ts": types.Object, "rep_ts": types.Array,
	} {
		actual, err := stream.Schema.GetType(column)
		require.NoError(t, err)
		require.Equal(t, expected, actual, "column %s", column)
	}

	var records []map[string]any
	require.NoError(t, parser.StreamRecords(context.Background(), bytes.NewReader(buf.Bytes()),
		func(_ context.Context, record map[string]any) error {
			records = append(records, record)
			return nil
		}))
	require.Len(t, records, 1)

	record := records[0]
	require.Equal(t, []any{ts, ts.Add(time.Second)}, record["list_ts"],
		"list elements must carry timestamps, not raw micros")
	require.Equal(t, map[string]any{
		"d":  time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		"ts": ts,
		"u":  "123e4567-e89b-12d3-a456-426614174000",
		"s":  "text",
	}, record["struct_col"], "struct fields must keep their logical meaning")
	require.Equal(t, map[string]any{"k": ts}, record["map_ts"],
		"map values must carry timestamps, not raw micros")
	require.Equal(t, []any{ts, ts.Add(time.Minute)}, record["rep_ts"],
		"repeated leaf elements must carry timestamps, not raw micros")
}

// The three state-gate tests below cannot run concurrently (no t.Parallel, here or in any test
// they share the package with): the gate reads constants.LoadedStateVersion, a process global,
// so setting it leaks across tests. The state version wants to be a parameter rather than a
// global before these can be made modular and parallel-safe.
//
// TestParquetValueToInterfaceWithType_Int96StateGate pins the backward-compatibility gate:
// state created before version 7 keeps the legacy raw-integer string so an existing
// destination column does not change type on upgrade, while newer state gets the time.Time.
func TestParquetValueToInterfaceWithType_Int96StateGate(t *testing.T) {
	defer func(v int) { constants.LoadedStateVersion = v }(constants.LoadedStateVersion)

	i96 := deprecated.Int64ToInt96(49530123456789)
	i96[2] = 2460477 // 2024-06-15

	constants.LoadedStateVersion = parquetTypeStateVersion - 1
	legacy := parquetValueToInterfaceWithType(pq.Int96Value(i96), pq.Int96Type)
	_, isString := legacy.(string)
	require.True(t, isString, "state < 7 must emit the raw Int96 string, got %T", legacy)
	assert.Equal(t, pq.Int96Value(i96).String(), legacy, "must match the pre-gate string output")

	constants.LoadedStateVersion = parquetTypeStateVersion
	gated := parquetValueToInterfaceWithType(pq.Int96Value(i96), pq.Int96Type)
	_, isTime := gated.(time.Time)
	require.True(t, isTime, "state >= 7 must emit time.Time, got %T", gated)
}

// TestUnsigned32StateGate pins the other half of the version-7 gate: state created before it
// keeps the Int32 schema and the signed value that wrapped negative above 2^31-1.
func TestUnsigned32StateGate(t *testing.T) {
	defer func(v int) { constants.LoadedStateVersion = v }(constants.LoadedStateVersion)

	schema := pq.NewSchema("test", pq.Group{"u32": pq.Uint(32)})
	u32 := schema.Fields()[0].Type()
	// 2^31 as stored bits: reads back negative when taken as a signed int32.
	stored := pq.Int32Value(int32(-2147483648))

	constants.LoadedStateVersion = parquetTypeStateVersion - 1
	assert.Equal(t, types.Int32, mapParquetTypeToOlake(u32), "state < 7 must keep the Int32 schema")
	// Pre-gate builds took the physical path, which widened to int64 and left the sign wrapped.
	assert.Equal(t, int64(-2147483648), parquetValueToInterfaceWithType(stored, u32),
		"state < 7 must keep the signed, widened value pre-gate builds emitted")

	constants.LoadedStateVersion = parquetTypeStateVersion
	assert.Equal(t, types.Int64, mapParquetTypeToOlake(u32), "state >= 7 must widen to Int64")
	assert.Equal(t, int64(2147483648), parquetValueToInterfaceWithType(stored, u32),
		"state >= 7 must reinterpret the bits as unsigned")
}

// TestNativeWidthStateGate pins the third part of the version-7 gate: pre-gate builds widened
// int32 to int64 and float32 to float64, which made the destination create the column
// bigint/double. Older state keeps the wide value so that column is not narrowed on upgrade.
func TestNativeWidthStateGate(t *testing.T) {
	defer func(v int) { constants.LoadedStateVersion = v }(constants.LoadedStateVersion)

	i32 := pq.Int32Value(-32768)
	f32 := pq.FloatValue(3.14)

	constants.LoadedStateVersion = parquetTypeStateVersion - 1
	assert.Equal(t, int64(-32768), parquetValueToInterfaceWithType(i32, pq.Int32Type),
		"state < 7 must keep the widened int64")
	assert.Equal(t, float64(float32(3.14)), parquetValueToInterfaceWithType(f32, pq.FloatType),
		"state < 7 must keep the widened float64")

	constants.LoadedStateVersion = parquetTypeStateVersion
	assert.Equal(t, int32(-32768), parquetValueToInterfaceWithType(i32, pq.Int32Type),
		"state >= 7 must keep the native int32")
	assert.Equal(t, float32(3.14), parquetValueToInterfaceWithType(f32, pq.FloatType),
		"state >= 7 must keep the native float32")
}

// TestParquetParser_FlatNullableColumns exercises the columnar read path (used for flat
// schemas) with nulls interleaved through nullable columns. That path reconstructs rows by
// column index and relies on parquet-go emitting null values inline; a misalignment would shift
// a column's values off their rows, which the interleaved nulls below would surface.
func TestParquetParser_FlatNullableColumns(t *testing.T) {
	type row struct {
		ID  int64   `parquet:"id"`
		Opt *int64  `parquet:"opt,optional"`
		Str *string `parquet:"str,optional"`
	}
	i64 := func(v int64) *int64 { return &v }
	str := func(v string) *string { return &v }
	rows := []row{
		{ID: 0, Opt: i64(100), Str: str("a")},
		{ID: 1, Opt: nil, Str: nil},
		{ID: 2, Opt: i64(300), Str: nil},
		{ID: 3, Opt: nil, Str: str("d")},
		{ID: 4, Opt: i64(500), Str: str("e")},
	}

	var buf bytes.Buffer
	writer := pq.NewGenericWriter[row](&buf)
	_, err := writer.Write(rows)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	parser := NewParquetParser(ParquetConfig{}, types.NewStream("flatnull", "flatnull", nil))
	var got []map[string]any
	require.NoError(t, parser.StreamRecords(context.Background(), bytes.NewReader(buf.Bytes()),
		func(_ context.Context, rec map[string]any) error {
			got = append(got, rec)
			return nil
		}))

	require.Len(t, got, 5)
	want := []struct {
		id  int64
		opt interface{}
		str interface{}
	}{
		{0, int64(100), "a"},
		{1, nil, nil},
		{2, int64(300), nil},
		{3, nil, "d"},
		{4, int64(500), "e"},
	}
	for i, w := range want {
		assert.Equal(t, w.id, got[i]["id"], "row %d id", i)
		assert.Equal(t, w.opt, got[i]["opt"], "row %d opt (null alignment)", i)
		assert.Equal(t, w.str, got[i]["str"], "row %d str (null alignment)", i)
	}
}

// BenchmarkStreamRecords measures decode throughput of the batched reader at several read
// batch sizes, plus a "legacy" baseline that replicates the pre-batching parser (whole row
// group materialized column-major, rows reconstructed by index). The batch sizes climb past a
// single row group's row count, so the largest reads the whole group in one ReadRows call —
// the closest in-algorithm equivalent to the old unbounded read. rowReadBatchSize is currently
// 256; run this to check whether it costs throughput versus reading larger or the whole group:
//
//	go test ./pkg/parser/ -run=^$ -bench=BenchmarkStreamRecords -benchmem
func BenchmarkStreamRecords(b *testing.B) {
	type benchRow struct {
		ID    int64     `parquet:"id"`
		Name  string    `parquet:"name"`
		Price float64   `parquet:"price"`
		Flag  bool      `parquet:"flag"`
		TS    time.Time `parquet:"ts,timestamp(microsecond)"`
	}

	const numRows = 50000
	rows := make([]benchRow, numRows)
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := range rows {
		rows[i] = benchRow{
			ID:    int64(i),
			Name:  "record",
			Price: float64(i) * 1.5,
			Flag:  i%2 == 0,
			TS:    base.Add(time.Duration(i) * time.Second),
		}
	}

	var buf bytes.Buffer
	writer := pq.NewGenericWriter[benchRow](&buf)
	if _, err := writer.Write(rows); err != nil {
		b.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		b.Fatal(err)
	}
	fileBytes := buf.Bytes()

	parser := NewParquetParser(ParquetConfig{}, types.NewStream("bench", "bench", nil))
	discard := func(_ context.Context, _ map[string]any) error { return nil }

	// Report how the file was split so a batch >= the per-group row count means "whole group".
	if pqFile, err := pq.OpenFile(bytes.NewReader(fileBytes), int64(len(fileBytes))); err == nil {
		groups := pqFile.RowGroups()
		if len(groups) > 0 {
			b.Logf("%d rows across %d row group(s), ~%d rows/group", numRows, len(groups), groups[0].NumRows())
		}
	}

	// columnar is the production path for this flat schema (StreamRecords picks it when the
	// schema has no group/repeated fields); the batch=N cases drive the row-at-a-time path that
	// nested schemas fall back to, to show why flat schemas read column by column instead.
	b.Run("columnar", func(b *testing.B) {
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			pqFile, err := pq.OpenFile(bytes.NewReader(fileBytes), int64(len(fileBytes)))
			if err != nil {
				b.Fatal(err)
			}
			decoder := newRowDecoder(pqFile.Schema())
			count := 0
			for _, rg := range pqFile.RowGroups() {
				if err := parser.streamRowGroupColumns(context.Background(), rg, decoder, discard, &count); err != nil {
					b.Fatal(err)
				}
			}
		}
		b.ReportMetric(float64(numRows)*float64(b.N)/b.Elapsed().Seconds(), "rows/s")
	})

	for _, batchSize := range []int{256, 1024} {
		b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
			b.ReportAllocs()
			for n := 0; n < b.N; n++ {
				pqFile, err := pq.OpenFile(bytes.NewReader(fileBytes), int64(len(fileBytes)))
				if err != nil {
					b.Fatal(err)
				}
				decoder := newRowDecoder(pqFile.Schema())
				count := 0
				for _, rg := range pqFile.RowGroups() {
					if err := parser.streamRowGroupRows(context.Background(), rg, decoder, batchSize, discard, &count); err != nil {
						b.Fatal(err)
					}
				}
			}
			b.ReportMetric(float64(numRows)*float64(b.N)/b.Elapsed().Seconds(), "rows/s")
		})
	}
}
