package typeutils

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/datazip-inc/olake/types"
)

// Decimal is an exact value of a decimal(p,s) column: its unscaled integer times 10^-scale. It
// carries its type so destinations derive the column from the value, as they do for every type.
type Decimal struct {
	unscaled decimal128.Num
	scale    int32
	dataType types.DataType
}

// Type is the decimal(p,s) the value was read as.
func (d Decimal) Type() types.DataType {
	return d.dataType
}

// String is the exact value with every digit of its scale, the way databases print decimals.
func (d Decimal) String() string {
	return DecimalText(d.unscaled, d.scale)
}

// DecimalText prints an unscaled integer at scale exactly, with every digit of the scale.
func DecimalText(unscaled decimal128.Num, scale int32) string {
	digits := unscaled.Abs().BigInt().String()
	if scale > 0 {
		digits = strings.Repeat("0", max(int(scale)+1-len(digits), 0)) + digits
		digits = digits[:len(digits)-int(scale)] + "." + digits[len(digits)-int(scale):]
	}
	if unscaled.Sign() < 0 {
		return "-" + digits
	}
	return digits
}

// Value binds the decimal as its exact text, which a database casts back to the column's decimal; database/sql
// rejects a struct argument.
func (d Decimal) Value() (driver.Value, error) {
	return d.String(), nil
}

// MarshalJSON writes the value as an exact JSON number.
func (d Decimal) MarshalJSON() ([]byte, error) {
	return []byte(d.String()), nil
}

// Float64 is the float64 nearest to the value.
func (d Decimal) Float64() float64 {
	f, _ := strconv.ParseFloat(d.String(), 64)
	return f
}

// Cmp compares two decimals by value, whatever their scales.
func (d Decimal) Cmp(other Decimal) int {
	if d.scale == other.scale {
		return d.unscaled.Cmp(other.unscaled)
	}
	a, b := d.unscaled.BigInt(), other.unscaled.BigInt()
	if d.scale < other.scale {
		a.Mul(a, decimal128.GetScaleMultiplier(int(other.scale-d.scale)).BigInt())
	} else {
		b.Mul(b, decimal128.GetScaleMultiplier(int(d.scale-other.scale)).BigInt())
	}
	return a.Cmp(b)
}

// ReformatDecimal converts v to a value of dataType, a decimal(p,s), failing rather than round away a
// digit or overflow the precision. BigDecimal, a column's ValueType, keeps v's own scale.
func ReformatDecimal(dataType types.DataType, v any) (Decimal, error) {
	if dataType == types.BigDecimal {
		return exactDecimal(v)
	}
	if d, isDecimal := v.(Decimal); isDecimal && d.dataType == dataType {
		return d, nil
	}
	precision, scale, isDecimal := types.DecimalParams(dataType)
	if !isDecimal {
		return Decimal{}, fmt.Errorf("%s is not a decimal type", dataType)
	}
	unscaled, err := DecimalUnscaled(v, precision, scale)
	if err != nil {
		return Decimal{}, err
	}
	return Decimal{unscaled: unscaled, scale: scale, dataType: dataType}, nil
}

// DecimalUnscaled returns v's unscaled integer in a decimal(precision, scale) column, the form
// parquet and arrow store. It fails rather than round away a digit or overflow the precision.
func DecimalUnscaled(v any, precision, scale int32) (decimal128.Num, error) {
	unscaled, from, err := decimalOf(v)
	if err != nil {
		return decimal128.Num{}, err
	}
	if scale < from {
		if unscaled, err = unscaled.Rescale(from, scale); err != nil {
			return decimal128.Num{}, fmt.Errorf("value[%v] has more than the %d digits after the point a decimal(%d,%d) keeps", v, scale, precision, scale)
		}
	}
	grow := max(scale-from, 0)
	if !unscaled.Abs().Less(decimal128.GetScaleMultiplier(int(max(precision-grow, 0)))) {
		return decimal128.Num{}, fmt.Errorf("value[%v] does not fit a decimal(%d,%d)", v, precision, scale)
	}
	return unscaled.IncreaseScaleBy(grow), nil
}

// exactDecimal reads v at its own precision and scale, so comparing it with a column value loses
// nothing to rounding.
func exactDecimal(v any) (Decimal, error) {
	unscaled, scale, err := decimalOf(v)
	if err != nil {
		return Decimal{}, err
	}
	precision := max(scale, 1)
	for precision < types.MaxDecimalPrecision && !unscaled.Abs().Less(decimal128.GetScaleMultiplier(int(precision))) {
		precision++
	}
	return Decimal{unscaled: unscaled, scale: scale, dataType: types.DecimalOf(int(precision), int(scale))}, nil
}

// decimalOf reads v as an exact number: its unscaled integer and the scale it sits at.
func decimalOf(v any) (decimal128.Num, int32, error) {
	switch v := v.(type) {
	case Decimal:
		return v.unscaled, v.scale, nil
	case string:
		return parseDecimal(v)
	case []byte:
		return parseDecimal(string(v))
	case json.Number:
		return parseDecimal(v.String())
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return parseDecimal(fmt.Sprint(v))
	case float32:
		return parseDecimal(strconv.FormatFloat(float64(v), 'f', -1, 32))
	case float64:
		return parseDecimal(strconv.FormatFloat(v, 'f', -1, 64))
	case *any:
		return decimalOf(*v)
	case fmt.Stringer:
		return parseDecimal(v.String())
	default:
		return decimal128.Num{}, 0, fmt.Errorf("failed to change %v (type:%T) to decimal", v, v)
	}
}

// maxDecimalExponent caps a literal's exponent so the scale arithmetic cannot overflow.
const maxDecimalExponent = 1000

// errNaN is a NaN, which postgres numeric holds and no decimal can.
var errNaN = fmt.Errorf("failed to change %q to decimal", "NaN")

// parseDecimal reads a literal [+-]digits[.digits][(e|E)[+-]digits] exactly. Zeros that change
// nothing are dropped, so the scale is the fewest digits after the point that hold the value.
func parseDecimal(s string) (decimal128.Num, int32, error) {
	if s == "NaN" {
		return decimal128.Num{}, 0, errNaN
	}
	invalid := func() (decimal128.Num, int32, error) {
		return decimal128.Num{}, 0, fmt.Errorf("failed to change %q to decimal", s)
	}

	mantissa, exponent := s, 0
	if i := strings.IndexAny(s, "eE"); i >= 0 {
		e, err := strconv.Atoi(s[i+1:])
		if err != nil || e < -maxDecimalExponent || e > maxDecimalExponent {
			return invalid()
		}
		mantissa, exponent = s[:i], e
	}
	negative := false
	switch {
	case strings.HasPrefix(mantissa, "-"):
		negative, mantissa = true, mantissa[1:]
	case strings.HasPrefix(mantissa, "+"):
		mantissa = mantissa[1:]
	}
	whole, fraction, _ := strings.Cut(mantissa, ".")
	if len(whole)+len(fraction) == 0 {
		return invalid()
	}
	fraction = strings.TrimRight(fraction, "0")

	var unscaled decimal128.Num
	digits := 0
	for _, part := range [2]string{whole, fraction} {
		for i := 0; i < len(part); i++ {
			c := part[i]
			if c < '0' || c > '9' {
				return invalid()
			}
			if digits == 0 && c == '0' {
				continue
			}
			if digits++; digits > types.MaxDecimalPrecision {
				return decimal128.Num{}, 0, fmt.Errorf("value[%s] has more than %d significant digits", s, types.MaxDecimalPrecision)
			}
			unscaled = unscaled.Mul(decimal128.FromU64(10)).Add(decimal128.FromU64(uint64(c - '0')))
		}
	}
	if digits == 0 {
		return decimal128.Num{}, 0, nil
	}

	if negative {
		unscaled = unscaled.Negate()
	}
	scale := len(fraction) - exponent
	switch {
	case scale > types.MaxDecimalPrecision:
		return decimal128.Num{}, 0, fmt.Errorf("value[%s] has more than %d digits after the point", s, types.MaxDecimalPrecision)
	case scale < 0:
		if digits-scale > types.MaxDecimalPrecision {
			return decimal128.Num{}, 0, fmt.Errorf("value[%s] has more than %d digits", s, types.MaxDecimalPrecision)
		}
		return unscaled.IncreaseScaleBy(int32(-scale)), 0, nil
	}
	return unscaled, int32(scale), nil
}
