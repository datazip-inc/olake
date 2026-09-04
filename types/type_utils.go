package types

import (
	"fmt"
	"regexp"
	"strconv"

	"github.com/parquet-go/parquet-go"
)

type pqNodeConstructor func() parquet.Node

// destinationType holds every destination-side mapping for one olake DataType.
type destinationType struct {
	icebergType string
	parquetNode pqNodeConstructor
}

func leafNode(typ parquet.Type) pqNodeConstructor {
	return func() parquet.Node { return parquet.Leaf(typ) }
}

func timestampNode() parquet.Node { return parquet.Timestamp(parquet.Microsecond) }

// Fixed-length binary is the one parameterised DataType: the byte length rides inside the
// type string as fixed_binary(n) so it survives the catalog JSON unchanged. Base() strips the
// parameter wherever the tree or the destination maps need the family.
var (
	fixedBinaryPattern  = regexp.MustCompile(`^` + regexp.QuoteMeta(string(FixedBinary)) + `\((\d+)\)$`)
	icebergFixedPattern = regexp.MustCompile(`^fixed\[(\d+)\]$`)
)

// FixedBinaryOf returns the DataType of a binary column that always holds exactly length bytes.
func FixedBinaryOf(length int) DataType {
	return DataType(fmt.Sprintf("%s(%d)", FixedBinary, length))
}

// FixedLength returns the byte length carried by a fixed_binary(n) DataType.
func (d DataType) FixedLength() (int, bool) {
	return parseLength(fixedBinaryPattern, string(d))
}

// Base strips the parameter from a parameterised DataType: fixed_binary(16) -> fixed_binary.
// Every other DataType is its own base.
func (d DataType) Base() DataType {
	if _, ok := d.FixedLength(); ok {
		return FixedBinary
	}
	return d
}

// IsBinary reports whether the DataType carries raw bytes: Binary or any fixed_binary(n).
func (d DataType) IsBinary() bool {
	base := d.Base()
	return base == Binary || base == FixedBinary
}

// icebergFixedLength returns the length of an iceberg fixed[n] type.
func icebergFixedLength(icebergType string) (int, bool) {
	return parseLength(icebergFixedPattern, icebergType)
}

func parseLength(pattern *regexp.Regexp, s string) (int, bool) {
	match := pattern.FindStringSubmatch(s)
	if match == nil {
		return 0, false
	}
	length, err := strconv.Atoi(match[1])
	if err != nil || length <= 0 {
		return 0, false
	}
	return length, true
}

func fixedBinaryNode(length int) parquet.Node {
	return parquet.Leaf(parquet.FixedLenByteArrayType(length))
}
