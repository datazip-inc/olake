package driver

import (
	"encoding/binary"
	"errors"
	"math"
	"reflect"
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils/typeutils"
)

// mysqlPointWKB builds what MySQL returns for a geometry column: a 4-byte SRID
// prefix followed by little-endian WKB.
func mysqlPointWKB(x, y float64) []byte {
	b := make([]byte, 4)
	b = append(b, 1) // little-endian byte order marker
	b = binary.LittleEndian.AppendUint32(b, 1)
	b = binary.LittleEndian.AppendUint64(b, math.Float64bits(x))
	return binary.LittleEndian.AppendUint64(b, math.Float64bits(y))
}

func intPtr(v int) *int { return &v }

func TestDataTypeConverter(t *testing.T) {
	tests := []struct {
		name string
		// columnType as MySQL reports it; "unsigned <type>" for UNSIGNED columns.
		columnType string
		value      any
		// stateVersion overrides constants.LoadedStateVersion; nil means the latest.
		stateVersion *int
		expected     any
		expectedErr  error
		wantErr      bool
	}{
		// ===== unsigned tinyint: binlog int8 bits read back as 0-255, mapped to Int32 =====
		{
			name:       "unsigned tinyint zero",
			columnType: "unsigned tinyint",
			value:      int8(0),
			expected:   int32(0),
		},
		{
			name:       "unsigned tinyint signed max",
			columnType: "unsigned tinyint",
			value:      int8(math.MaxInt8),
			expected:   int32(math.MaxInt8),
		},
		// the sign bit alone is the midpoint of the unsigned range
		{
			name:       "unsigned tinyint sign bit",
			columnType: "unsigned tinyint",
			value:      int8(math.MinInt8),
			expected:   int32(-math.MinInt8),
		},
		{
			name:       "unsigned tinyint max",
			columnType: "unsigned tinyint",
			value:      int8(-1),
			expected:   int32(math.MaxUint8),
		},

		// ===== unsigned smallint: int16 bits read back as 0-65535, mapped to Int32 =====
		{
			name:       "unsigned smallint signed max",
			columnType: "unsigned smallint",
			value:      int16(math.MaxInt16),
			expected:   int32(math.MaxInt16),
		},
		{
			name:       "unsigned smallint sign bit",
			columnType: "unsigned smallint",
			value:      int16(math.MinInt16),
			expected:   int32(-math.MinInt16),
		},
		{
			name:       "unsigned smallint max",
			columnType: "unsigned smallint",
			value:      int16(-1),
			expected:   int32(math.MaxUint16),
		},

		// ===== unsigned mediumint: 3-byte range, still mapped to Int32 =====
		{
			name:       "unsigned mediumint zero",
			columnType: "unsigned mediumint",
			value:      int32(0),
			expected:   int32(0),
		},
		{
			name:       "unsigned mediumint last value not sign extended",
			columnType: "unsigned mediumint",
			value:      int32(8388607),
			expected:   int32(8388607),
		},
		// mediumint is 3 bytes wide but arrives in a 4-byte int32, so from 8388608 up go-mysql
		// pads the top byte with the sign bit; only masking to the storage width recovers the value
		{
			name:       "unsigned mediumint sign bit",
			columnType: "unsigned mediumint",
			value:      int32(-8388608),
			expected:   int32(8388608),
		},
		{
			name:       "unsigned mediumint sign bit plus one",
			columnType: "unsigned mediumint",
			value:      int32(-8388607),
			expected:   int32(8388609),
		},
		{
			name:       "unsigned mediumint max minus one",
			columnType: "unsigned mediumint",
			value:      int32(-2),
			expected:   int32(16777214),
		},
		{
			// go-mysql hands back the sign-extended 3-byte value, not 16777215
			name:       "unsigned mediumint max",
			columnType: "unsigned mediumint",
			value:      int32(-1),
			expected:   int32(16777215),
		},
		{
			// a value that never went through the binlog is already correct and must survive the mask
			name:       "unsigned mediumint already unsigned",
			columnType: "unsigned mediumint",
			value:      int32(16777215),
			expected:   int32(16777215),
		},

		// ===== unsigned int/integer: int32 bits read back as 0-4294967295, mapped to Int64 =====
		{
			name:       "unsigned int signed max",
			columnType: "unsigned int",
			value:      int32(math.MaxInt32),
			expected:   int64(math.MaxInt32),
		},
		{
			name:       "unsigned int sign bit",
			columnType: "unsigned int",
			value:      int32(math.MinInt32),
			expected:   int64(-math.MinInt32),
		},
		{
			name:       "unsigned int max",
			columnType: "unsigned int",
			value:      int32(-1),
			expected:   int64(math.MaxUint32),
		},
		{
			name:       "unsigned integer max",
			columnType: "unsigned integer",
			value:      int32(-1),
			expected:   int64(math.MaxUint32),
		},
		// column types arrive in mixed case from some code paths
		{
			name:       "unsigned int uppercase column type",
			columnType: "UNSIGNED INT",
			value:      int32(-1),
			expected:   int64(math.MaxUint32),
		},

		// ===== unsigned bigint: no int64 headroom left, so the bits survive as-is =====
		{
			name:       "unsigned bigint small value",
			columnType: "unsigned bigint",
			value:      int64(42),
			expected:   int64(42),
		},
		// TODO: olake has no uint64 data type, so BIGINT UNSIGNED past MaxInt64 stays wrapped
		// negative -- the cases below pin that loss, they are not the values MySQL stored
		{
			// should be uint64(math.MaxUint64): these bits are BIGINT UNSIGNED's 2^64-1
			name:       "unsigned bigint above max int64",
			columnType: "unsigned bigint",
			value:      int64(-1),
			expected:   int64(-1),
		},
		{
			// should be uint64(1<<63): the sign bit alone is 2^63, the midpoint of the range
			name:       "unsigned bigint min int64",
			columnType: "unsigned bigint",
			value:      int64(math.MinInt64),
			expected:   int64(math.MinInt64),
		},

		// ===== unsigned columns outside the binlog path: values arrive already unsigned or widened =====
		{
			name:       "unsigned tinyint already uint8",
			columnType: "unsigned tinyint",
			value:      uint8(math.MaxUint8),
			expected:   int32(math.MaxUint8),
		},
		// the server applies signedness itself on the backfill path, so values come back widened
		// and correct and never reach the mask
		{
			name:       "unsigned mediumint already widened to int64",
			columnType: "unsigned mediumint",
			value:      int64(16777215),
			expected:   int32(16777215),
		},
		{
			name:       "unsigned int already widened to int64",
			columnType: "unsigned int",
			value:      int64(math.MaxUint32),
			expected:   int64(math.MaxUint32),
		},
		{
			// should be uint64(math.MaxUint64): the value arrives correct and only the cast loses it
			name:       "unsigned bigint already uint64",
			columnType: "unsigned bigint",
			value:      uint64(math.MaxUint64),
			expected:   int64(-1),
		},

		// ===== state version gate: v4 introduced the unsigned handling =====
		{
			name:         "unsigned tinyint at version 4",
			columnType:   "unsigned tinyint",
			value:        int8(-1),
			stateVersion: intPtr(4),
			expected:     int32(math.MaxUint8),
		},
		// v3 and below drop the unsigned prefix and keep the signed value
		{
			name:         "unsigned tinyint at version 3",
			columnType:   "unsigned tinyint",
			value:        int8(-1),
			stateVersion: intPtr(3),
			expected:     int32(-1),
		},
		{
			name:         "unsigned mediumint at version 3",
			columnType:   "unsigned mediumint",
			value:        int32(-1),
			stateVersion: intPtr(3),
			expected:     int32(-1),
		},
		// legacy maps unsigned int to Int32 (the overflow v4 fixed), not Int64
		{
			name:         "unsigned int at version 3",
			columnType:   "unsigned int",
			value:        int32(-1),
			stateVersion: intPtr(3),
			expected:     int32(-1),
		},
		{
			name:         "unsigned bigint at version 0",
			columnType:   "UNSIGNED BIGINT",
			value:        int64(-1),
			stateVersion: intPtr(0),
			expected:     int64(-1),
		},

		// ===== geospatial columns: binary WKB is converted to WKT =====
		{
			name:       "point wkb",
			columnType: "point",
			value:      mysqlPointWKB(1, 2),
			expected:   "POINT(1 2)",
		},
		{
			name:       "geometry wkb",
			columnType: "geometry",
			value:      mysqlPointWKB(-3.5, 4.25),
			expected:   "POINT(-3.5 4.25)",
		},
		// textual geometry is already WKT and passes through untouched
		{
			name:       "point already wkt",
			columnType: "point",
			value:      "POINT(1 2)",
			expected:   "POINT(1 2)",
		},
		// non-geospatial binary keeps its bytes
		{
			name:       "varbinary is not geospatial",
			columnType: "varbinary(16)",
			value:      []byte("abc"),
			expected:   []byte("abc"),
		},
		{
			name:       "blob keeps non-utf8 bytes",
			columnType: "blob",
			value:      []byte{0xff, 0x00, 0x80},
			expected:   []byte{0xff, 0x00, 0x80},
		},

		// ===== signed and non-integer columns =====
		{
			name:       "signed tinyint keeps sign",
			columnType: "tinyint",
			value:      int8(-1),
			expected:   int32(-1),
		},
		// the width mask must not reach signed columns, which share mediumint's int32 shape
		{
			name:       "signed mediumint min keeps sign",
			columnType: "mediumint",
			value:      int32(-8388608),
			expected:   int32(-8388608),
		},
		{
			name:       "signed bigint keeps sign",
			columnType: "bigint",
			value:      int64(-9007199254740993),
			expected:   int64(-9007199254740993),
		},
		// the length suffix is stripped before the type lookup
		{
			name:       "varchar with length",
			columnType: "varchar(50)",
			value:      "hello",
			expected:   "hello",
		},
		{
			name:       "decimal from byte slice",
			columnType: "decimal(10,2)",
			value:      []uint8("123.45"),
			expected:   float64(123.45),
		},
		// unmapped types fall through unchanged
		{
			name:       "unknown column type",
			columnType: "unknown_type",
			value:      "as-is",
			expected:   "as-is",
		},

		// ===== errors =====
		{
			name:        "nil value",
			columnType:  "unsigned bigint",
			value:       nil,
			expected:    nil,
			expectedErr: typeutils.ErrNullValue,
		},
		{
			name:       "non numeric value for int column",
			columnType: "int",
			value:      "not-a-number",
			wantErr:    true,
		},
	}

	old := constants.LoadedStateVersion
	t.Cleanup(func() { constants.LoadedStateVersion = old })

	m := &MySQL{}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			constants.LoadedStateVersion = constants.LatestStateVersion
			if tc.stateVersion != nil {
				constants.LoadedStateVersion = *tc.stateVersion
			}

			got, err := m.dataTypeConverter(tc.value, tc.columnType)
			switch {
			case tc.expectedErr != nil:
				if !errors.Is(err, tc.expectedErr) {
					t.Fatalf("expected error %v, got %v", tc.expectedErr, err)
				}
			case tc.wantErr:
				if err == nil {
					t.Fatalf("expected an error, got value %#v", got)
				}
				return
			default:
				if err != nil {
					t.Fatalf("unexpected error: %s", err)
				}
			}

			if !reflect.DeepEqual(got, tc.expected) {
				t.Fatalf("expected %#v (%T), got %#v (%T)", tc.expected, tc.expected, got, got)
			}
		})
	}
}
