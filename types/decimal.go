package types

import (
	"math"
	"strings"

	"github.com/datazip-inc/olake/utils"
	"github.com/parquet-go/parquet-go"
)

// MaxDecimalPrecision is the widest decimal iceberg and spark can store.
const MaxDecimalPrecision = 38

// decimalFamily registers decimal(%d,%d): a precision of 1 to MaxDecimalPrecision digits, of which
// scale sit after the point.
var decimalFamily = newTypeFamily(Decimal, validDecimal[int])

// validDecimal also checks the int32 parameters IsDecimal scans, the type arrow takes them in, and the
// int64 ones database/sql reports.
func validDecimal[N int | int32 | int64](p []N) bool {
	return len(p) == 2 && p[0] >= 1 && p[0] <= MaxDecimalPrecision && p[1] <= p[0]
}

// NumericOf maps a source decimal column of precision digits, scale of them after the point, to decimal(p,s) where
// iceberg can hold it. Otherwise, and on older state so an existing destination column keeps its type, Float64.
func NumericOf(precision, scale int64) DataType {
	if !utils.IsDecimalSupported() || scale < 0 || !validDecimal([]int64{precision, scale}) {
		return Float64
	}
	return Decimal.Of(precision, scale)
}

// decimalNode stores the unscaled value in an int32 up to 9 digits, an int64 up to 18 and, past that, the
// fewest bytes of big-endian two's complement that hold it, as iceberg does.
func decimalNode(params ...any) parquet.Node {
	precision, scale := params[0].(int), params[1].(int)
	switch {
	case precision <= 9:
		return parquet.Decimal(scale, precision, parquet.Int32Type)
	case precision <= 18:
		return parquet.Decimal(scale, precision, parquet.Int64Type)
	default:
		fixed := parquet.Decimal(scale, precision, parquet.FixedLenByteArrayType(DecimalBytes(precision)))
		return parquet.Leaf(signedDecimal{fixed.Type()})
	}
}

// DecimalOf returns the DataType of an exact number of precision digits, scale of them after the
// point. Iceberg and spark hold no decimal past 38 digits or a negative scale, so those are String.
func DecimalOf(precision, scale int) DataType {
	if scale < 0 || !validDecimal([]int{precision, scale}) {
		return String
	}
	return Decimal.Of(precision, scale)
}

// IsDecimal reports whether d is a decimal(p,s) DataType, with its precision and scale, without allocating.
func IsDecimal(d DataType) (isDecimal bool, precision, scale int32) {
	var params [2]int32
	if !utils.ScanType(d, Decimal, params[:]) || !validDecimal(params[:]) {
		return false, 0, 0
	}
	return true, params[0], params[1]
}

// IsIcebergDecimal reports whether an iceberg type is a decimal(p, s).
func IsIcebergDecimal(icebergType string) bool {
	return strings.HasPrefix(icebergType, "decimal(")
}

// icebergDecimal is decimalFamily's iceberg pattern, decimal(%d, %d).
var icebergDecimal = decimalFamily.icebergPattern()

// IcebergDecimalParams returns the precision and scale of an iceberg decimal(p, s) type without
// allocating.
func IcebergDecimalParams(icebergType string) (precision, scale int32, ok bool) {
	var params [2]int32
	if !utils.ScanType(icebergType, icebergDecimal, params[:]) || !validDecimal(params[:]) {
		return 0, 0, false
	}
	return params[0], params[1], true
}

// DecimalBytes is the fewest bytes that hold every two's complement unscaled value of precision.
func DecimalBytes[N int | int32](precision N) N {
	n := N(1)
	for math.Pow(2, float64(8*n-1)) < math.Pow10(int(precision)) {
		n++
	}
	return n
}

// widerDecimal returns the narrowest decimal holding every value of two decimals, when one exists.
func widerDecimal(t1, t2 DataType) (DataType, bool) {
	ok1, p1, s1 := IsDecimal(t1)
	ok2, p2, s2 := IsDecimal(t2)
	if !ok1 || !ok2 {
		return "", false
	}
	scale := max(s1, s2)
	precision := max(p1-s1, p2-s2) + scale
	if precision > MaxDecimalPrecision {
		return "", false
	}
	return DecimalOf(int(precision), int(scale)), true
}
