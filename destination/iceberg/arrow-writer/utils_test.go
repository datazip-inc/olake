package arrowwriter

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestToArrowTypeBinary(t *testing.T) {
	assert.Equal(t, arrow.BinaryTypes.Binary, toArrowType("binary"))
	assert.Equal(t, &arrow.FixedSizeBinaryType{ByteWidth: 16}, toArrowType("fixed[16]"))
	assert.Equal(t, arrow.BinaryTypes.String, toArrowType("string"))
	assert.Equal(t, arrow.BinaryTypes.String, toArrowType("fixed[oops]"), "a malformed fixed type falls back to string like any unknown type")
}

// TestAppendValueToBuilderBinary asserts bytes land in binary and fixed-size-binary builders
// unchanged, and that a wrong width is rejected instead of reaching arrow's own panic.
func TestAppendValueToBuilderBinary(t *testing.T) {
	mem := memory.NewGoAllocator()
	raw := []byte{0xff, 0x00, 0x80, 0x41}

	binaryBuilder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer binaryBuilder.Release()
	require.NoError(t, appendValueToBuilder(binaryBuilder, raw))
	require.NoError(t, appendValueToBuilder(binaryBuilder, "text"))
	binaryArr := binaryBuilder.NewBinaryArray()
	defer binaryArr.Release()
	assert.Equal(t, raw, binaryArr.Value(0))
	assert.Equal(t, []byte("text"), binaryArr.Value(1))

	fixedBuilder := array.NewFixedSizeBinaryBuilder(mem, &arrow.FixedSizeBinaryType{ByteWidth: 4})
	defer fixedBuilder.Release()
	require.NoError(t, appendValueToBuilder(fixedBuilder, raw))
	require.NoError(t, appendValueToBuilder(fixedBuilder, []byte{1, 2}))
	require.Error(t, appendValueToBuilder(fixedBuilder, []byte{1, 2, 3, 4, 5}))
	fixedArr := fixedBuilder.NewFixedSizeBinaryArray()
	defer fixedArr.Release()
	assert.Equal(t, 2, fixedArr.Len())
	assert.Equal(t, raw, fixedArr.Value(0))
	assert.Equal(t, []byte{1, 2, 0, 0}, fixedArr.Value(1), "a short value is zero padded to the width")

	// a string builder still turns bytes into text rather than a %v byte dump
	stringBuilder := array.NewStringBuilder(mem)
	defer stringBuilder.Release()
	require.NoError(t, appendValueToBuilder(stringBuilder, []byte("abc")))
	stringArr := stringBuilder.NewStringArray()
	defer stringArr.Release()
	assert.Equal(t, "abc", stringArr.Value(0))
}

func TestArrowFieldsToParquetBinary(t *testing.T) {
	node, err := arrowFieldsToParquet(arrow.Field{Name: "b", Type: arrow.BinaryTypes.Binary, Nullable: true})
	require.NoError(t, err)
	primitive := node.(*schema.PrimitiveNode)
	assert.Equal(t, parquet.Types.ByteArray, primitive.PhysicalType())
	assert.Equal(t, schema.NoLogicalType{}, primitive.LogicalType(), "binary carries no string annotation")
	assert.Equal(t, parquet.Repetitions.Optional, primitive.RepetitionType())

	node, err = arrowFieldsToParquet(arrow.Field{Name: "f", Type: &arrow.FixedSizeBinaryType{ByteWidth: 16}, Nullable: true})
	require.NoError(t, err)
	primitive = node.(*schema.PrimitiveNode)
	assert.Equal(t, parquet.Types.FixedLenByteArray, primitive.PhysicalType())
	assert.Equal(t, 16, primitive.TypeLength())
}
