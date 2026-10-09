package types

import (
	"strings"

	"github.com/datazip-inc/olake/utils"
	"github.com/parquet-go/parquet-go"
)

// fixedBinaryFamily registers fixed_binary(%d): one width, and it must be positive. A fixed width
// is a promise about every value, so two different widths cannot meet in a third; instances that
// disagree fall to the family's parent in the typecast tree.
var fixedBinaryFamily = newTypeFamily(FixedBinary, validFixedBinary)

// validFixedBinary is fixedBinaryFamily's parameter check, named so FixedBinaryWidth can call it
// without its array escaping through the family's func field.
func validFixedBinary(p []int) bool {
	return len(p) == 1 && p[0] > 0
}

func fixedBinaryNode(params ...any) parquet.Node {
	return parquet.Leaf(parquet.FixedLenByteArrayType(params[0].(int)))
}

// FixedBinaryOf returns the DataType of a binary column that always holds exactly length bytes.
func FixedBinaryOf(length int) DataType {
	return FixedBinary.Of(length)
}

// IsIcebergBytes reports whether an iceberg type carries bytes, binary or fixed[n], and whether
// it is a fixed one.
func IsIcebergBytes(icebergType string) (isBytes, isFixed bool) {
	if icebergType == "binary" {
		return true, false
	}
	isFixed = strings.HasPrefix(icebergType, "fixed[")
	return isFixed, isFixed
}

// FixedBinaryWidth returns the width of a fixed_binary(n) DataType. It allocates nothing, so the
// parquet writer can check every value against its column.
func FixedBinaryWidth(d DataType) (int, bool) {
	var width [1]int
	if !utils.ScanType(d, FixedBinary, width[:]) || !validFixedBinary(width[:]) {
		return 0, false
	}
	return width[0], true
}

// icebergFixedBinary is fixedBinaryFamily's iceberg pattern, fixed[%d], read once for
// IcebergFixedWidth.
var icebergFixedBinary = fixedBinaryFamily.icebergPattern()

// IcebergFixedWidth returns the width of an iceberg fixed[n] type. Like FixedBinaryWidth it
// allocates nothing, so a writer can check every value against its column.
func IcebergFixedWidth(icebergType string) (int, bool) {
	var width [1]int
	if !utils.ScanType(icebergType, icebergFixedBinary, width[:]) || !validFixedBinary(width[:]) {
		return 0, false
	}
	return width[0], true
}
