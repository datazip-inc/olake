package types

import (
	"fmt"
	"regexp"
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
	matcher *regexp.Regexp
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
		matcher: patternMatcher(string(pattern)),
		parity:  strings.Count(string(pattern), "%d"),
		valid:   valid,
	}
}

// patternMatcher turns a pattern with %d verbs into a regexp capturing each parameter. A comma
// may be followed by whitespace, which is how Iceberg prints a multi-parameter type.
func patternMatcher(pattern string) *regexp.Regexp {
	quoted := regexp.QuoteMeta(pattern)
	quoted = strings.ReplaceAll(quoted, "%d", `(\d+)`)
	quoted = strings.ReplaceAll(quoted, ",", `,\s*`)
	return regexp.MustCompile("^" + quoted + "$")
}

// parse returns the parameters s carries if it is an instance of the family: every parameter
// present, numeric and legal for the family.
func (f *typeFamily) parse(s string) ([]int, bool) {
	return f.capture(f.matcher.FindStringSubmatch(s))
}

func (f *typeFamily) capture(match []string) ([]int, bool) {
	if match == nil {
		return nil, false
	}
	params := make([]int, 0, f.parity)
	for _, group := range match[1:] {
		n, err := strconv.Atoi(group)
		if err != nil {
			return nil, false
		}
		params = append(params, n)
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
	if !strings.ContainsRune(string(d), '(') {
		return nil, nil
	}
	for _, f := range families {
		if d == f.pattern {
			return f, nil
		}
		if params, ok := f.parse(string(d)); ok {
			return f, params
		}
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
	if f, params := instanceOf(d); f != nil && params != nil {
		return f.pattern
	}
	return d
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

// resolve returns the DataType whose destination mappings apply to d and the parameters to
// render them with: an instance resolves to its family pattern, every other type is itself. A
// family's bare pattern is a placeholder rather than a type, so no column ever carries one.
func (d DataType) resolve() (DataType, []any) {
	if f, params := instanceOf(d); f != nil && params != nil {
		return f.pattern, asAny(params)
	}
	return d, nil
}

// icebergFamilyMatcher pairs a family with the matcher built from its iceberg pattern, so an
// iceberg type parses back into an instance carrying its parameters.
type icebergFamilyMatcher struct {
	family  *typeFamily
	matcher *regexp.Regexp
}

var icebergFamilyMatchers = func() []icebergFamilyMatcher {
	matchers := make([]icebergFamilyMatcher, 0, len(families))
	for _, f := range families {
		if mapping, ok := destinationTypes[f.pattern]; ok && strings.Contains(mapping.icebergType, "%d") {
			matchers = append(matchers, icebergFamilyMatcher{f, patternMatcher(mapping.icebergType)})
		}
	}
	return matchers
}()

// icebergInstance parses an iceberg type of a parameterised family into the instance it denotes.
func icebergInstance(icebergType string) (DataType, bool) {
	for _, m := range icebergFamilyMatchers {
		if params, ok := m.family.capture(m.matcher.FindStringSubmatch(icebergType)); ok {
			return m.family.instance(params), true
		}
	}
	return "", false
}
