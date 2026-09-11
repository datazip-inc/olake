package arrowwriter

import (
	"math"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestToArrowType(t *testing.T) {
	testCases := []struct {
		icebergType string
		want        arrow.DataType
	}{
		{
			icebergType: "boolean",
			want:        arrow.FixedWidthTypes.Boolean,
		},
		{
			icebergType: "int",
			want:        arrow.PrimitiveTypes.Int32,
		},
		{
			icebergType: "long",
			want:        arrow.PrimitiveTypes.Int64,
		},
		{
			icebergType: "float",
			want:        arrow.PrimitiveTypes.Float32,
		},
		{
			icebergType: "double",
			want:        arrow.PrimitiveTypes.Float64,
		},
		{
			icebergType: "timestamptz",
			want:        arrow.FixedWidthTypes.Timestamp_us,
		},
		{
			icebergType: "binary",
			want:        arrow.BinaryTypes.Binary,
		},
		{
			icebergType: "fixed[16]",
			want:        &arrow.FixedSizeBinaryType{ByteWidth: 16},
		},
		{
			icebergType: "string",
			want:        arrow.BinaryTypes.String,
		},
		{
			icebergType: "fixed[oops]",
			want:        arrow.BinaryTypes.String,
		},
		{
			icebergType: "fixed[0]",
			want:        arrow.BinaryTypes.String,
		},
		{
			icebergType: "decimal(9, 2)",
			want:        arrow.BinaryTypes.String,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.icebergType, func(t *testing.T) {
			assert.Equal(t, tc.want, toArrowType(tc.icebergType))
		})
	}
}

func TestAppendValueToBuilder(t *testing.T) {
	sample := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	raw := []byte{0xff, 0x00, 0x80, 0x41}

	testCases := []struct {
		name    string
		builder func(memory.Allocator) array.Builder
		value   any
		want    any
		wantErr bool
	}{
		{
			name:    "boolean",
			builder: func(mem memory.Allocator) array.Builder { return array.NewBooleanBuilder(mem) },
			value:   true,
			want:    true,
		},
		{
			name:    "int32",
			builder: func(mem memory.Allocator) array.Builder { return array.NewInt32Builder(mem) },
			value:   int32(7),
			want:    int32(7),
		},
		{
			name:    "int64",
			builder: func(mem memory.Allocator) array.Builder { return array.NewInt64Builder(mem) },
			value:   int64(42),
			want:    int64(42),
		},
		{
			name:    "float32",
			builder: func(mem memory.Allocator) array.Builder { return array.NewFloat32Builder(mem) },
			value:   float32(1.5),
			want:    float32(1.5),
		},
		{
			name:    "float64",
			builder: func(mem memory.Allocator) array.Builder { return array.NewFloat64Builder(mem) },
			value:   2.5,
			want:    2.5,
		},
		{
			name: "timestamp",
			builder: func(mem memory.Allocator) array.Builder {
				return array.NewTimestampBuilder(mem, arrow.FixedWidthTypes.Timestamp_us.(*arrow.TimestampType))
			},
			value: sample,
			want:  arrow.Timestamp(sample.UnixMicro()),
		},
		{
			name: "bytes into a binary column",
			builder: func(mem memory.Allocator) array.Builder {
				return array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
			},
			value: raw,
			want:  raw,
		},
		{
			name: "text into a binary column",
			builder: func(mem memory.Allocator) array.Builder {
				return array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
			},
			value: "text",
			want:  []byte("text"),
		},
		{
			name: "bytes of the exact width",
			builder: func(mem memory.Allocator) array.Builder {
				return array.NewFixedSizeBinaryBuilder(mem, &arrow.FixedSizeBinaryType{ByteWidth: 4})
			},
			value: raw,
			want:  raw,
		},
		{
			name: "a short value is zero padded to the width",
			builder: func(mem memory.Allocator) array.Builder {
				return array.NewFixedSizeBinaryBuilder(mem, &arrow.FixedSizeBinaryType{ByteWidth: 4})
			},
			value: []byte{1, 2},
			want:  []byte{1, 2, 0, 0},
		},
		{
			name: "a value wider than the column is rejected",
			builder: func(mem memory.Allocator) array.Builder {
				return array.NewFixedSizeBinaryBuilder(mem, &arrow.FixedSizeBinaryType{ByteWidth: 4})
			},
			value:   []byte{1, 2, 3, 4, 5},
			wantErr: true,
		},
		{
			name:    "bytes into a string column stay text",
			builder: func(mem memory.Allocator) array.Builder { return array.NewStringBuilder(mem) },
			value:   []byte("abc"),
			want:    "abc",
		},
		{
			name:    "a map into a string column becomes json",
			builder: func(mem memory.Allocator) array.Builder { return array.NewStringBuilder(mem) },
			value:   map[string]interface{}{"a": 1},
			want:    `{"a":1}`,
		},
		{
			name:    "a value that is not a boolean is rejected",
			builder: func(mem memory.Allocator) array.Builder { return array.NewBooleanBuilder(mem) },
			value:   map[string]interface{}{"a": 1},
			wantErr: true,
		},
		{
			name:    "a value that is not an int32 is rejected",
			builder: func(mem memory.Allocator) array.Builder { return array.NewInt32Builder(mem) },
			value:   "abc",
			wantErr: true,
		},
		{
			name:    "a value that is not an int64 is rejected",
			builder: func(mem memory.Allocator) array.Builder { return array.NewInt64Builder(mem) },
			value:   "abc",
			wantErr: true,
		},
		{
			name:    "a value that is not a float32 is rejected",
			builder: func(mem memory.Allocator) array.Builder { return array.NewFloat32Builder(mem) },
			value:   "abc",
			wantErr: true,
		},
		{
			name:    "a value that is not a float64 is rejected",
			builder: func(mem memory.Allocator) array.Builder { return array.NewFloat64Builder(mem) },
			value:   "abc",
			wantErr: true,
		},
		{
			name: "a value that is not bytes is rejected",
			builder: func(mem memory.Allocator) array.Builder {
				return array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
			},
			value:   int64(42),
			wantErr: true,
		},
		{
			name:    "a map that cannot be marshaled is rejected",
			builder: func(mem memory.Allocator) array.Builder { return array.NewStringBuilder(mem) },
			value:   map[string]interface{}{"a": make(chan int)},
			wantErr: true,
		},
		{
			name:    "an unsupported builder is rejected",
			builder: func(mem memory.Allocator) array.Builder { return array.NewDate32Builder(mem) },
			value:   sample,
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			builder := tc.builder(memory.NewGoAllocator())
			defer builder.Release()

			err := appendValueToBuilder(builder, tc.value)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			arr := builder.NewArray()
			defer arr.Release()
			require.Equal(t, 1, arr.Len())
			assert.Equal(t, tc.want, firstValue(t, arr))
		})
	}
}

func TestArrowFieldsToParquet(t *testing.T) {
	testCases := []struct {
		name       string
		field      arrow.Field
		physical   parquet.Type
		typeLength int
		logical    schema.LogicalType
		repetition parquet.Repetition
		fieldID    int32
		wantErr    bool
	}{
		{
			name:       "binary carries no string annotation",
			field:      arrow.Field{Name: "b", Type: arrow.BinaryTypes.Binary, Nullable: true},
			physical:   parquet.Types.ByteArray,
			typeLength: -1,
			logical:    schema.NoLogicalType{},
			repetition: parquet.Repetitions.Optional,
			fieldID:    -1,
		},
		{
			name:       "fixed binary carries its width",
			field:      arrow.Field{Name: "f", Type: &arrow.FixedSizeBinaryType{ByteWidth: 16}, Nullable: true},
			physical:   parquet.Types.FixedLenByteArray,
			typeLength: 16,
			logical:    schema.NoLogicalType{},
			repetition: parquet.Repetitions.Optional,
			fieldID:    -1,
		},
		{
			name:       "string is annotated as text",
			field:      arrow.Field{Name: "s", Type: arrow.BinaryTypes.String, Nullable: true},
			physical:   parquet.Types.ByteArray,
			typeLength: -1,
			logical:    schema.StringLogicalType{},
			repetition: parquet.Repetitions.Optional,
			fieldID:    -1,
		},
		{
			name:       "long",
			field:      arrow.Field{Name: "l", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			physical:   parquet.Types.Int64,
			typeLength: -1,
			logical:    schema.NoLogicalType{},
			repetition: parquet.Repetitions.Optional,
			fieldID:    -1,
		},
		{
			name:       "timestamp is annotated as utc micros",
			field:      arrow.Field{Name: "t", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true},
			physical:   parquet.Types.Int64,
			typeLength: -1,
			logical:    schema.NewTimestampLogicalType(true, schema.TimeUnitMicros),
			repetition: parquet.Repetitions.Optional,
			fieldID:    -1,
		},
		{
			name:       "a column that is not nullable is required",
			field:      arrow.Field{Name: "b", Type: arrow.BinaryTypes.Binary},
			physical:   parquet.Types.ByteArray,
			typeLength: -1,
			logical:    schema.NoLogicalType{},
			repetition: parquet.Repetitions.Required,
			fieldID:    -1,
		},
		{
			name:       "boolean",
			field:      arrow.Field{Name: "bo", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
			physical:   parquet.Types.Boolean,
			typeLength: -1,
			logical:    schema.NoLogicalType{},
			repetition: parquet.Repetitions.Optional,
			fieldID:    -1,
		},
		{
			name:       "int",
			field:      arrow.Field{Name: "i", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			physical:   parquet.Types.Int32,
			typeLength: -1,
			logical:    schema.NoLogicalType{},
			repetition: parquet.Repetitions.Optional,
			fieldID:    -1,
		},
		{
			name:       "float",
			field:      arrow.Field{Name: "f32", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
			physical:   parquet.Types.Float,
			typeLength: -1,
			logical:    schema.NoLogicalType{},
			repetition: parquet.Repetitions.Optional,
			fieldID:    -1,
		},
		{
			name:       "double",
			field:      arrow.Field{Name: "f64", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
			physical:   parquet.Types.Double,
			typeLength: -1,
			logical:    schema.NoLogicalType{},
			repetition: parquet.Repetitions.Optional,
			fieldID:    -1,
		},
		{
			name:       "an arrow type with no mapping falls back to text",
			field:      arrow.Field{Name: "d", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
			physical:   parquet.Types.ByteArray,
			typeLength: -1,
			logical:    schema.StringLogicalType{},
			repetition: parquet.Repetitions.Optional,
			fieldID:    -1,
		},
		{
			name: "a field id in the metadata reaches the parquet node",
			field: arrow.Field{
				Name:     "b",
				Type:     arrow.BinaryTypes.Binary,
				Nullable: true,
				Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"7"}),
			},
			physical:   parquet.Types.ByteArray,
			typeLength: -1,
			logical:    schema.NoLogicalType{},
			repetition: parquet.Repetitions.Optional,
			fieldID:    7,
		},
		{
			name:    "a width parquet cannot store is rejected",
			field:   arrow.Field{Name: "f", Type: &arrow.FixedSizeBinaryType{ByteWidth: math.MaxInt32 + 1}, Nullable: true},
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			node, err := arrowFieldsToParquet(tc.field)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)

			primitive := node.(*schema.PrimitiveNode)
			assert.Equal(t, tc.physical, primitive.PhysicalType())
			assert.Equal(t, tc.typeLength, primitive.TypeLength())
			assert.True(t, tc.logical.Equals(primitive.LogicalType()), "logical type of %s", tc.name)
			assert.Equal(t, tc.repetition, primitive.RepetitionType())
			assert.Equal(t, tc.fieldID, primitive.FieldID())
		})
	}
}

func firstValue(t *testing.T, arr arrow.Array) any {
	t.Helper()
	switch typed := arr.(type) {
	case *array.Boolean:
		return typed.Value(0)
	case *array.Int32:
		return typed.Value(0)
	case *array.Int64:
		return typed.Value(0)
	case *array.Float32:
		return typed.Value(0)
	case *array.Float64:
		return typed.Value(0)
	case *array.Timestamp:
		return typed.Value(0)
	case *array.Binary:
		return typed.Value(0)
	case *array.FixedSizeBinary:
		return typed.Value(0)
	case *array.String:
		return typed.Value(0)
	}
	t.Fatalf("no reader for array type %T", arr)
	return nil
}
