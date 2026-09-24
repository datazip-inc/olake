package types

import (
	"strconv"
	"strings"

	"github.com/parquet-go/parquet-go"
)

// fixedBinaryFamily registers fixed_binary(%d): one width, and it must be positive. A fixed width
// is a promise about every value, so two different widths cannot meet in a third; instances that
// disagree fall to the family's parent in the typecast tree.
var fixedBinaryFamily = newTypeFamily(FixedBinary, func(p []int) bool { return len(p) == 1 && p[0] > 0 })

func fixedBinaryNode(params ...any) parquet.Node {
	return parquet.Leaf(parquet.FixedLenByteArrayType(params[0].(int)))
}

// FixedBinaryOf returns the DataType of a binary column that always holds exactly length bytes.
func FixedBinaryOf(length int) DataType {
	return FixedBinary.Of(length)
}

// IcebergBytesWidth reports whether an iceberg type carries bytes, along with the width of a fixed one.
func IcebergBytesWidth(icebergType string) (int, bool) {
	return bytesWidth(icebergType, "fixed[", "]")
}

// BytesWidth reports whether a column of type d carries bytes, along with the width of a fixed one:
// the DataType twin of IcebergBytesWidth.
func BytesWidth(d DataType) (int, bool) {
	return bytesWidth(string(d), "fixed_binary(", ")")
}

// bytesWidth reports whether typeName carries bytes, along with the width of a fixed one. Both type
// systems spell variable-length bytes "binary"; a fixed width sits between prefix and suffix.
func bytesWidth(typeName, prefix, suffix string) (int, bool) {
	if typeName == "binary" {
		return 0, true
	}
	digits, isFixed := strings.CutPrefix(typeName, prefix)
	if !isFixed {
		return 0, false
	}
	digits, closed := strings.CutSuffix(digits, suffix)
	if !closed {
		return 0, false
	}
	width, err := strconv.Atoi(digits)
	if err != nil || width <= 0 {
		return 0, false
	}
	return width, true
}
