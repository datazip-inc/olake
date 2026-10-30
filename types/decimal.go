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

// validDecimal also checks the int32 parameters DecimalParams scans, the type arrow takes them in.
func validDecimal[N int | int32](p []N) bool {
	return len(p) == 2 && p[0] >= 1 && p[0] <= MaxDecimalPrecision && p[1] <= p[0]
}

// decimalNode stores the unscaled value in an int32 up to 9 digits and an int64 up to 18. parquet-go
// bounds fixed bytes unsigned, mis-pruning negatives (parquet-go#595), so wider ones stay exact text.
func decimalNode(params ...any) parquet.Node {
	precision, scale := params[0].(int), params[1].(int)
	switch {
	case precision <= 9:
		return parquet.Decimal(scale, precision, parquet.Int32Type)
	case precision <= 18:
		return parquet.Decimal(scale, precision, parquet.Int64Type)
	default:
		return parquet.String()
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

// DecimalParams returns the precision and scale of a decimal(p,s) DataType without allocating.
func DecimalParams(d DataType) (precision, scale int32, ok bool) {
	var params [2]int32
	if !utils.ScanType(d, Decimal, params[:]) || !validDecimal(params[:]) {
		return 0, 0, false
	}
	return params[0], params[1], true
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
func DecimalBytes(precision int32) int32 {
	n := int32(1)
	for math.Pow(2, float64(8*n-1)) < math.Pow10(int(precision)) {
		n++
	}
	return n
}

// widerDecimal returns the narrowest decimal holding every value of two decimals, when one exists.
func widerDecimal(t1, t2 DataType) (DataType, bool) {
	p1, s1, ok1 := DecimalParams(t1)
	p2, s2, ok2 := DecimalParams(t2)
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
