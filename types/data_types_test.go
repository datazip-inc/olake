package types

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

func TestToIceberg(t *testing.T) {
	testCases := []struct {
		dataType DataType
		expected string
	}{
		{Bool, "boolean"},
		{Int32, "int"},
		{Int64, "long"},
		{Float32, "float"},
		{Float64, "double"},
		{String, "string"},
		{Timestamp, "timestamptz"},
		{TimestampMilli, "timestamptz"},
		{TimestampMicro, "timestamptz"},
		{TimestampNano, "timestamptz"},
		{Object, "string"},
		{Array, "string"},
		{Binary, "binary"},
		{FixedBinaryOf(16), "fixed[16]"},
		{DataType("undeclared_type"), "string"},
	}

	for _, tc := range testCases {
		t.Run(string(tc.dataType), func(t *testing.T) {
			require.Equal(t, tc.expected, tc.dataType.ToIceberg())
		})
	}
}

func TestToNewParquet(t *testing.T) {
	testCases := []struct {
		dataType DataType
		expected parquet.Node
	}{
		{Bool, parquet.Leaf(parquet.BooleanType)},
		{Int32, parquet.Leaf(parquet.Int32Type)},
		{Int64, parquet.Leaf(parquet.Int64Type)},
		{Float32, parquet.Leaf(parquet.FloatType)},
		{Float64, parquet.Leaf(parquet.DoubleType)},
		{String, parquet.String()},
		{Timestamp, parquet.Timestamp(parquet.Microsecond)},
		{TimestampMilli, parquet.Timestamp(parquet.Microsecond)},
		{TimestampMicro, parquet.Timestamp(parquet.Microsecond)},
		{TimestampNano, parquet.Timestamp(parquet.Microsecond)},
		{Object, parquet.String()},
		{Array, parquet.String()},
		{Binary, parquet.Leaf(parquet.ByteArrayType)},
		{FixedBinaryOf(16), parquet.Leaf(parquet.FixedLenByteArrayType(16))},
		{DataType("undeclared_type"), parquet.Leaf(parquet.ByteArrayType)},
	}

	for _, tc := range testCases {
		t.Run(string(tc.dataType), func(t *testing.T) {
			node := tc.dataType.ToNewParquet()
			require.True(t, node.Optional(), "node for %s must be optional", tc.dataType)
			require.True(t, parquet.EqualNodes(parquet.Optional(tc.expected), node), "unexpected node for %s", tc.dataType)
		})
	}
}

func TestIcebergTypeToDatatype(t *testing.T) {
	testCases := []struct {
		iceType  string
		expected DataType
	}{
		{"boolean", Bool},
		{"int", Int32},
		{"long", Int64},
		{"float", Float32},
		{"double", Float64},
		{"timestamptz", TimestampMilli},
		{"string", String},
		{"binary", Binary},
		{"fixed[16]", FixedBinaryOf(16)},
		{"fixed[0]", String},   // iceberg fixed needs a positive length
		{"fixed[abc]", String}, // malformed length
		{"fixed", String},      // length missing
		{"undeclared_type", String},
	}

	for _, tc := range testCases {
		t.Run(tc.iceType, func(t *testing.T) {
			require.Equal(t, tc.expected, IcebergTypeToDatatype(tc.iceType))
		})
	}
}

func TestDeclaredTypesHaveExplicitIcebergMapping(t *testing.T) {
	for _, dataType := range declaredDataTypes(t) {
		if destinationTypes[dataType].icebergType == "" {
			t.Errorf("data type %s has no explicit iceberg mapping; add it to destinationTypes", dataType)
		}
	}
}

func TestDeclaredTypesHaveExplicitParquetMapping(t *testing.T) {
	for _, dataType := range declaredDataTypes(t) {
		construct := destinationTypes[dataType].parquetNodeConstructor
		if construct == nil {
			t.Errorf("data type %s has no explicit parquet mapping; add it to destinationTypes", dataType)
			continue
		}
		require.NotNil(t, construct(sampleParams(dataType)...), "parquet node constructor for %s returned nil", dataType)
	}
}

func TestDeclaredIcebergTypesHaveExplicitOlakeMapping(t *testing.T) {
	for _, iceType := range declaredIcebergDataTypes(t) {
		if strings.Contains(iceType, "%d") {
			// a family's pattern maps back through its parameters, not through the table
			family := familyProducing(t, iceType)
			params := sampleParams(family)
			rendered := fmt.Sprintf(iceType, params...)
			require.Equal(t, family.Of(params...), IcebergTypeToDatatype(rendered), "iceberg %s must parse back into its family", rendered)
			continue
		}
		if _, ok := icebergToDataType[iceType]; !ok {
			t.Errorf("iceberg type %s has no explicit olake mapping; add it to icebergToDataType", iceType)
		}
	}
}

func TestIcebergAndOlakeTypeMappingConsistency(t *testing.T) {
	// group olake data types by the iceberg type they map to
	icebergGroups := make(map[string][]DataType)
	for dataType, mapping := range destinationTypes {
		icebergGroups[mapping.icebergType] = append(icebergGroups[mapping.icebergType], dataType)
	}

	for iceType, group := range icebergGroups {
		if strings.Contains(iceType, "%d") {
			continue // a family's pattern is covered by TestDeclaredIcebergTypesHaveExplicitOlakeMapping
		}
		canonical, ok := icebergToDataType[iceType]
		if !ok {
			t.Errorf("iceberg type %s has no reverse mapping; add a canonical DataType to icebergToDataType", iceType)
			continue
		}
		if !slices.Contains(group, canonical) {
			t.Errorf("iceberg type %s reverse-maps to %s, which maps to %s instead", iceType, canonical, canonical.ToIceberg())
		}
	}

	for iceType, dataType := range icebergToDataType {
		if _, ok := icebergGroups[iceType]; !ok {
			t.Errorf("icebergToDataType maps orphan iceberg type %s to %s; no DataType produces it", iceType, dataType)
		}
	}
}

// declaredIcebergDataTypes returns the unique iceberg types produced by the
// destinationTypes mapping, sorted for deterministic failure output.
func declaredIcebergDataTypes(t *testing.T) []string {
	t.Helper()

	unique := make(map[string]struct{}, len(destinationTypes))
	for _, mapping := range destinationTypes {
		unique[mapping.icebergType] = struct{}{}
	}
	iceTypes := make([]string, 0, len(unique))
	for iceType := range unique {
		iceTypes = append(iceTypes, iceType)
	}
	slices.Sort(iceTypes)
	return iceTypes
}

// declaredDataTypes returns every constant declared with explicit type DataType
// in this package's source files.
func declaredDataTypes(t *testing.T) []DataType {
	t.Helper()

	// ignoreDeclaredTypes are data types that we don't need tests on. They are defined for fallback cases
	ignoreDeclaredTypes := map[DataType]struct{}{
		Null:    {},
		Unknown: {},
	}

	var dataTypes []DataType
	for _, file := range parsePackageFiles(t) {
		for _, decl := range file.Decls {
			genDecl, ok := decl.(*ast.GenDecl)
			if !ok || genDecl.Tok != token.CONST {
				continue
			}
			for _, spec := range genDecl.Specs {
				valueSpec := spec.(*ast.ValueSpec)
				ident, ok := valueSpec.Type.(*ast.Ident)
				if !ok || ident.Name != "DataType" {
					continue
				}
				for _, value := range valueSpec.Values {
					lit, ok := value.(*ast.BasicLit)
					if !ok || lit.Kind != token.STRING {
						continue
					}
					unquoted, err := strconv.Unquote(lit.Value)
					require.NoError(t, err)
					dataType := DataType(unquoted)
					if _, ok := ignoreDeclaredTypes[dataType]; !ok {
						dataTypes = append(dataTypes, dataType)
					}
				}
			}
		}
	}
	return dataTypes
}

func parsePackageFiles(t *testing.T) []*ast.File {
	t.Helper()
	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	fset := token.NewFileSet()
	var files []*ast.File
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		require.NoError(t, err)
		files = append(files, file)
	}
	return files
}

// sampleParams returns parameters an instance of dataType's family can be rendered with, all
// ones, and nothing for a plain type.
func sampleParams(dataType DataType) []any {
	f, _ := instanceOf(dataType)
	if f == nil {
		return nil
	}
	params := make([]any, f.parity)
	for i := range params {
		params[i] = 1
	}
	return params
}

// familyProducing returns the family whose destination mapping is the given iceberg pattern.
func familyProducing(t *testing.T, icebergPattern string) DataType {
	t.Helper()
	for dataType, mapping := range destinationTypes {
		if mapping.icebergType == icebergPattern {
			return dataType
		}
	}
	t.Fatalf("no DataType maps to iceberg %s", icebergPattern)
	return ""
}
