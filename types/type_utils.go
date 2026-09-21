package types

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/parquet-go/parquet-go"
)

// pqNodeConstructor builds a destination parquet node. A parameterised family receives the
// instance's parameters; every other type receives none.
type pqNodeConstructor func(params ...any) parquet.Node

// destinationType holds every destination-side mapping for one olake DataType. A parameterised
// family's iceberg type is a pattern with the same verbs as the family, rendered with the
// instance's parameters.
type destinationType struct {
	icebergType            string
	parquetNodeConstructor pqNodeConstructor
}

func leafNode(typ parquet.Type) pqNodeConstructor {
	return func(...any) parquet.Node { return parquet.Leaf(typ) }
}

func plainNode(construct func() parquet.Node) pqNodeConstructor {
	return func(...any) parquet.Node { return construct() }
}

func timestampNode(...any) parquet.Node { return parquet.Timestamp(parquet.Microsecond) }

// typeFamily is used for parameterised DataType
type typeFamily struct {
	pattern DataType
	parity  int
	valid   func(params []int) bool
}

// families is the roster of parameterised DataTypes. Each family describes itself in its own file.
var families = []*typeFamily{
	fixedBinaryFamily,
}

func newTypeFamily(pattern DataType, valid func([]int) bool) *typeFamily {
	return &typeFamily{
		pattern: pattern,
		parity:  strings.Count(string(pattern), "%d"),
		valid:   valid,
	}
}

// parse returns the parameters s carries if it is an instance of the family: every parameter
// present, numeric and legal for the family.
func (f *typeFamily) parse(s string) ([]int, bool) {
	return f.scan(string(f.pattern), s)
}

// scan reads the family's parameters out of s against pattern
func (f *typeFamily) scan(pattern, s string) ([]int, bool) {
	prefix, _, parameterised := strings.Cut(pattern, "%d")
	if !parameterised {
		return nil, false
	}

	// remove suffix and spaces
	s = strings.ReplaceAll(s[:len(s)-1], " ", "")

	commaSeperatedParams, found := strings.CutPrefix(s, prefix)
	if !found {
		return nil, false
	}

	paramsStrs := strings.Split(commaSeperatedParams, ",")
	if len(paramsStrs) != f.parity {
		return nil, false
	}
	params := make([]int, f.parity)
	for i, field := range paramsStrs {
		value, err := strconv.Atoi(field)
		if err != nil {
			return nil, false
		}
		params[i] = value
	}

	if f.valid != nil && !f.valid(params) {
		return nil, false
	}
	return params, true
}

// instance renders the family's pattern with params.
func (f *typeFamily) instance(params []int) DataType {
	return DataType(fmt.Sprintf(string(f.pattern), asAny(params)...))
}

func asAny(params []int) []any {
	out := make([]any, len(params))
	for i, p := range params {
		out[i] = p
	}
	return out
}

// instanceOf resolves d to its family and parameters: an instance yields both, a family's bare
// pattern yields the family and no parameters, and a plain type yields neither.
func instanceOf(d DataType) (*typeFamily, []int) {
	base := BaseOf(d)
	for _, f := range families {
		if f.pattern != base {
			continue
		}
		if d == base {
			return f, nil
		}

		params, ok := f.parse(string(d))
		if !ok {
			return nil, nil
		}
		return f, params
	}
	return nil, nil
}

// Of returns the instance of a parameterised family with the given parameters. A type that is
// not a family pattern, or the wrong number of parameters, yields the type unchanged.
func (d DataType) Of(params ...any) DataType {
	return DataType(fmt.Sprintf(string(d), params...))
}

// BaseOf returns the family pattern a parameterised instance belongs to, and every other DataType
// unchanged. The typecast tree and the destination mappings are keyed by the base.
func BaseOf(d DataType) DataType {
	open := strings.IndexByte(string(d), '(')
	if open < 0 {
		return d
	}
	switch d[:open] {
	case "fixed_binary":
		return FixedBinary
	default:
		return d
	}
}

// Params returns the parameters a parameterised instance carries; ok is false for every other
// DataType, a family's bare pattern included.
func (d DataType) Params() ([]int, bool) {
	if f, params := instanceOf(d); f != nil && params != nil {
		return params, true
	}
	return nil, false
}

// SameType reports whether a and b denote the same type, ignoring the parameters if any
func SameType(a, b DataType) bool {
	return BaseOf(a) == BaseOf(b)
}

// ParameterlessForm returns the type value detection yields for a parameterised type, since a
// value cannot reveal parameters: a fixed_binary(16) value is detected as binary. ok is false for
// a type that carries none, which detection can already name exactly.
func ParameterlessForm(d DataType) (DataType, bool) {
	if _, parameterised := d.Params(); !parameterised {
		return "", false
	}
	if parent, inTree := typeParent[BaseOf(d)]; inTree {
		return parent, true
	}
	return String, true
}

// Accepts reports whether a column of type d can hold values of a detected type without any change to the column
func (d DataType) Accepts(detected DataType) bool {
	if d == detected {
		return true
	}
	return GetCommonAncestorType(d, detected) == d
}

// icebergPattern returns the iceberg type a family's destination mapping renders its instances
// into, which is the key its reverse mapping is registered under.
func (f *typeFamily) icebergPattern() string {
	return destinationTypes[f.pattern].icebergType
}

// icebergInstanceOf resolves an iceberg type to the family it instantiates and its parameters,
// the mirror of instanceOf on the DataType side. A family with no mapping scans against an empty
// pattern, which reads no parameters and so matches nothing.
func icebergInstanceOf(icebergType string) (*typeFamily, []int) {
	for _, f := range families {
		if params, ok := f.scan(f.icebergPattern(), icebergType); ok {
			return f, params
		}
	}
	return nil, nil
}
