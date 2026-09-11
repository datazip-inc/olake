package types

import "github.com/parquet-go/parquet-go"

// fixedBinaryFamily registers fixed_binary(%d): one width, and it must be positive. A fixed width
// is a promise about every value, so two different widths cannot meet in a third; instances that
// disagree fall to the family's parent in the typecast tree, which is also the type a value
// reveals when its width cannot be seen.
var fixedBinaryFamily = newTypeFamily(FixedBinary, func(p []int) bool { return len(p) == 1 && p[0] > 0 })

func fixedBinaryNode(params ...any) parquet.Node {
	return parquet.Leaf(parquet.FixedLenByteArrayType(params[0].(int)))
}

// FixedBinaryOf returns the DataType of a binary column that always holds exactly length bytes.
func FixedBinaryOf(length int) DataType {
	return FixedBinary.Of(length)
}

// FixedBinaryWidth returns the width of a fixed_binary column, or 0 if the type is not a fixed_binary instance.
func FixedBinaryWidth(d DataType) int {
	if f, params := instanceOf(d); f != nil && params != nil {
		return params[0]
	}
	return 0
}
