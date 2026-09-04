package types

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination/iceberg/proto"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

// TestGetCommonAncestorType pins the promotion rules of the typecast tree: the lowest common
// ancestor of two types is the narrowest type both can be cast to.
func TestGetCommonAncestorType(t *testing.T) {
	testCases := []struct {
		name     string
		t1, t2   DataType
		expected DataType
	}{
		{"same type resolves to itself", Int64, Int64, Int64},
		{"ancestor absorbs descendant", Int32, Int64, Int64},
		{"bool promotes to int32", Bool, Int32, Int32},
		{"int32 and float32 split at float64", Int32, Float32, Float64},
		{"int64 and float32 split at float64", Int64, Float32, Float64},
		{"numeric and timestamp only meet at string", Int64, Timestamp, String},
		{"string absorbs timestamps", String, TimestampNano, String},
		{"timestamp promotes to the wider precision", Timestamp, TimestampMicro, TimestampMicro},
		{"milli and nano promote to nano", TimestampMilli, TimestampNano, TimestampNano},
		{"object promotes to string", Object, Int32, String},
		{"array promotes to string", Array, TimestampNano, String},
		{"null promotes to string", Null, Int64, String},
		{"unknown promotes to string", Unknown, Int64, String},
		{"object and array meet at string", Object, Array, String},
		{"unknown resolves to itself", Unknown, Unknown, Unknown},
		{"string promotes to binary", String, Binary, Binary},
		{"binary resolves to itself", Binary, Binary, Binary},
		{"binary absorbs numerics", Int64, Binary, Binary},
		{"binary absorbs timestamps", TimestampNano, Binary, Binary},
		{"object meets binary through string", Object, Binary, Binary},
		{"fixed binary resolves to itself", FixedBinaryOf(16), FixedBinaryOf(16), FixedBinaryOf(16)},
		{"fixed binaries of different lengths widen to binary", FixedBinaryOf(16), FixedBinaryOf(32), Binary},
		{"length-less fixed binary widens to binary against a sized one", FixedBinary, FixedBinaryOf(16), Binary},
		{"binary absorbs fixed binary", FixedBinaryOf(16), Binary, Binary},
		{"fixed binary and string meet at binary", FixedBinaryOf(16), String, Binary},
		{"fixed binary and numerics meet at binary", FixedBinaryOf(16), Int64, Binary},
		{"fixed binary and object meet at binary", FixedBinaryOf(16), Object, Binary},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, GetCommonAncestorType(tc.t1, tc.t2))
			require.Equal(t, tc.expected, GetCommonAncestorType(tc.t2, tc.t1), "GetCommonAncestorType must be symmetric")
		})
	}
}

// TestGetCommonAncestorTypeUndeclared asserts that a DataType absent from the typecast tree
// falls back to String regardless of pairing or argument position.
func TestGetCommonAncestorTypeUndeclared(t *testing.T) {
	undeclared := DataType("undeclared_type")
	for _, other := range treeDataTypes(t) {
		require.Equal(t, String, GetCommonAncestorType(undeclared, other), "ancestor of %s and %s must be String", undeclared, other)
		require.Equal(t, String, GetCommonAncestorType(other, undeclared), "ancestor of %s and %s must be String", other, undeclared)
	}
	require.Equal(t, String, GetCommonAncestorType(undeclared, undeclared))
}

// TestGetCommonAncestorTypeInvariants sweeps every pair of tree types: the result must be
// symmetric, identity on equal inputs, and an ancestor of both inputs.
func TestGetCommonAncestorTypeInvariants(t *testing.T) {
	ancestors := treeAncestors(t)
	all := treeDataTypes(t)
	for _, a := range all {
		require.Equal(t, a, GetCommonAncestorType(a, a), "ancestor of %s with itself must be %s", a, a)
		for _, b := range all {
			ancestor := GetCommonAncestorType(a, b)
			require.Equal(t, ancestor, GetCommonAncestorType(b, a), "ancestor of %s and %s must be symmetric", a, b)
			require.Contains(t, ancestors[a], ancestor, "ancestor %s of %s and %s must be an ancestor of %s", ancestor, a, b, a)
			require.Contains(t, ancestors[b], ancestor, "ancestor %s of %s and %s must be an ancestor of %s", ancestor, a, b, b)
		}
	}
}

// TestTypecastTreeHasAllDeclaredTypes asserts that typecastTree and the declared DataType
// constants stay in lockstep: every declared type appears in the tree exactly once, and the
// tree holds nothing undeclared.
func TestTypecastTreeHasAllDeclaredTypes(t *testing.T) {
	seen := make(map[DataType]int)
	var walk func(node *typeNode)
	walk = func(node *typeNode) {
		seen[node.t]++
		for _, child := range node.children {
			walk(child)
		}
	}
	walk(typecastTree)

	for _, dataType := range declaredDataTypes(t) {
		if seen[dataType] != 1 {
			t.Errorf("data type %s appears %d times in typecastTree; every declared type must appear exactly once", dataType, seen[dataType])
		}
	}
	for dataType := range seen {
		if !slices.Contains(declaredDataTypes(t), dataType) {
			t.Errorf("typecastTree holds %s, which is not a declared DataType", dataType)
		}
	}
}

// TestPropertyDataType asserts that a property resolves its type set to a single destination
// type: singletons resolve to themselves, Null is stripped before promotion, and mixed sets
// promote through the typecast tree.
func TestPropertyDataType(t *testing.T) {
	testCases := []struct {
		name     string
		types    []DataType
		expected DataType
	}{
		{"single type resolves to itself", []DataType{Int64}, Int64},
		{"only null resolves to null", []DataType{Null}, Null},
		{"empty type set resolves to null", nil, Null},
		{"null is stripped alongside a real type", []DataType{Null, Int64}, Int64},
		{"mixed numerics promote to ancestor", []DataType{Int32, Float32}, Float64},
		{"incompatible types meet at string", []DataType{Int64, Timestamp}, String},
		{"string with binary promotes to binary", []DataType{String, Binary}, Binary},
		{"fixed binary keeps its length", []DataType{Null, FixedBinaryOf(16)}, FixedBinaryOf(16)},
		{"mixed fixed binary lengths widen to binary", []DataType{FixedBinaryOf(16), FixedBinaryOf(32)}, Binary},
		{"null with incompatible types still meets at string", []DataType{Null, Int64, Timestamp}, String},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			property := &Property{Type: NewSet(tc.types...)}
			require.Equal(t, tc.expected, property.DataType())
		})
	}
}

func TestPropertyNullable(t *testing.T) {
	require.True(t, (&Property{Type: NewSet(Null, Int64)}).Nullable())
	require.True(t, (&Property{Type: NewSet(Null)}).Nullable())
	require.False(t, (&Property{Type: NewSet(Int64)}).Nullable())
}

// TestTypeSchemaAddTypes asserts that AddTypes creates properties with reformatted destination
// column names and merges subsequent types into the existing property.
func TestTypeSchemaAddTypes(t *testing.T) {
	schema := NewTypeSchema()
	schema.AddTypes("User ID", false, Int64)
	schema.AddTypes("_meta_col", true, String)

	require.ElementsMatch(t, []string{"User ID", "_meta_col"}, schema.ColumnNames())

	found, prop := schema.GetProperty("User ID")
	require.True(t, found)
	require.Equal(t, "user_id", prop.DestinationColumnName, "destination column name must be the reformatted source name")
	require.False(t, prop.OlakeColumn)
	require.Equal(t, Int64, prop.DataType())

	found, metaProp := schema.GetProperty("_meta_col")
	require.True(t, found)
	require.True(t, metaProp.OlakeColumn)

	// adding more types to an existing column merges into the same property
	schema.AddTypes("User ID", false, Float32)
	require.Equal(t, Float64, prop.DataType())

	found, missing := schema.GetProperty("missing")
	require.False(t, found)
	require.Nil(t, missing)
}

func TestTypeSchemaGetType(t *testing.T) {
	schema := NewTypeSchema()
	schema.AddTypes("User ID", false, Int64)

	// properties built by AddTypes carry destination column names, so lookup uses the source name
	dataType, err := schema.GetType("User ID")
	require.NoError(t, err)
	require.Equal(t, Int64, dataType)

	_, err = schema.GetType("missing")
	require.Error(t, err)
}

// TestTypeSchemaGetTypeLegacyCatalog covers catalogs written before destination_column_name
// existed: properties are stored under reformatted keys with the field empty, so GetType must
// reformat the lookup key itself.
func TestTypeSchemaGetTypeLegacyCatalog(t *testing.T) {
	schema := NewTypeSchema()
	require.NoError(t, json.Unmarshal([]byte(`{"properties": {"user_id": {"type": ["integer"]}}}`), schema))
	require.False(t, schema.HasDestinationColumnName())

	dataType, err := schema.GetType("User ID")
	require.NoError(t, err)
	require.Equal(t, Int64, dataType)
}

func TestTypeSchemaHasDestinationColumnName(t *testing.T) {
	schema := NewTypeSchema()
	require.False(t, schema.HasDestinationColumnName(), "empty schema has no destination column names")

	schema.AddTypes("User ID", false, Int64)
	require.True(t, schema.HasDestinationColumnName())
}

// TestTypeSchemaOverride asserts that Override replaces stored properties, preserves the
// nullability of the property it replaces, and stores unseen columns as-is.
func TestTypeSchemaOverride(t *testing.T) {
	schema := NewTypeSchema()
	schema.AddTypes("nullable_col", false, Int64, Null)
	schema.AddTypes("plain_col", false, Int64)

	schema.Override(map[string]*Property{
		"nullable_col": {Type: NewSet(String)},
		"plain_col":    {Type: NewSet(String)},
		"new_col":      {Type: NewSet(Bool)},
	})

	_, prop := schema.GetProperty("nullable_col")
	require.Equal(t, String, prop.DataType(), "override must replace the stored types")
	require.True(t, prop.Nullable(), "override must preserve nullability of the replaced property")

	_, prop = schema.GetProperty("plain_col")
	require.False(t, prop.Nullable())

	_, prop = schema.GetProperty("new_col")
	require.Equal(t, Bool, prop.DataType())
}

func TestTypeSchemaJSONRoundTrip(t *testing.T) {
	schema := NewTypeSchema()
	schema.AddTypes("User ID", false, Int64, Null)
	schema.AddTypes("_meta_col", true, String)
	schema.AddTypes("Digest", false, FixedBinaryOf(32))

	data, err := json.Marshal(schema)
	require.NoError(t, err)

	restored := NewTypeSchema()
	require.NoError(t, json.Unmarshal(data, restored))

	require.ElementsMatch(t, schema.ColumnNames(), restored.ColumnNames())

	found, prop := restored.GetProperty("User ID")
	require.True(t, found)
	require.Equal(t, Int64, prop.DataType())
	require.True(t, prop.Nullable())
	require.Equal(t, "user_id", prop.DestinationColumnName)

	found, prop = restored.GetProperty("_meta_col")
	require.True(t, found)
	require.True(t, prop.OlakeColumn)

	// the fixed length rides inside the type string, so it must survive the catalog round trip
	found, prop = restored.GetProperty("Digest")
	require.True(t, found)
	require.Equal(t, FixedBinaryOf(32), prop.DataType())
	require.Equal(t, "fixed[32]", prop.DataType().ToIceberg())
}

func parquetFieldNames(schema *parquet.Schema) []string {
	names := make([]string, 0, len(schema.Fields()))
	for _, field := range schema.Fields() {
		names = append(names, field.Name())
	}
	return names
}

func TestTypeSchemaToParquet(t *testing.T) {
	schema := NewTypeSchema()
	schema.AddTypes("User ID", false, Int64)
	schema.AddTypes("_meta_col", true, String)
	stream := &ConfiguredStream{Stream: &Stream{Name: "test_stream", Schema: schema}}

	defaultColumns := []string{constants.OlakeID, constants.OlakeTimestamp, constants.OpType, constants.CdcTimestamp}

	t.Run("all columns with reformatted names", func(t *testing.T) {
		names := parquetFieldNames(schema.ToParquet(false, stream))
		require.ElementsMatch(t, append([]string{"user_id", "_meta_col"}, defaultColumns...), names)
	})

	t.Run("default columns keep only olake columns plus stringified data", func(t *testing.T) {
		names := parquetFieldNames(schema.ToParquet(true, stream))
		require.ElementsMatch(t, append([]string{"_meta_col", constants.StringifiedData}, defaultColumns...), names)
	})

	t.Run("unselected columns are dropped", func(t *testing.T) {
		selective := &ConfiguredStream{
			Stream:         &Stream{Name: "test_stream", Schema: schema},
			StreamMetadata: StreamMetadata{SelectedColumns: &SelectedColumns{Columns: []string{"_meta_col"}}},
		}
		names := parquetFieldNames(schema.ToParquet(false, selective))
		require.ElementsMatch(t, append([]string{"_meta_col"}, defaultColumns...), names)
	})

	t.Run("source column names preserved when enabled", func(t *testing.T) {
		source := &ConfiguredStream{
			Stream:         &Stream{Name: "test_stream", Schema: schema},
			StreamMetadata: StreamMetadata{UseSourceColumnNames: true},
		}
		names := parquetFieldNames(schema.ToParquet(false, source))
		require.Contains(t, names, "User ID")
	})
}

func TestTypeSchemaToIceberg(t *testing.T) {
	schema := NewTypeSchema()
	schema.AddTypes("User ID", false, Int64)
	schema.AddTypes("Part Col", false, Timestamp)
	schema.AddTypes("_meta_col", true, String)
	stream := &ConfiguredStream{Stream: &Stream{Name: "test_stream", Schema: schema}}

	fieldTypes := func(fields []*proto.IcebergPayload_SchemaField) map[string]string {
		out := make(map[string]string, len(fields))
		for _, field := range fields {
			out[field.Key] = field.IceType
		}
		return out
	}

	t.Run("all columns with iceberg types", func(t *testing.T) {
		require.Equal(t, map[string]string{
			"user_id":   "long",
			"part_col":  "timestamptz",
			"_meta_col": "string",
		}, fieldTypes(schema.ToIceberg(false, stream)))
	})

	t.Run("default columns keep only olake columns plus stringified data", func(t *testing.T) {
		require.Equal(t, map[string]string{
			"_meta_col":               "string",
			constants.StringifiedData: "string",
		}, fieldTypes(schema.ToIceberg(true, stream)))
	})

	t.Run("include columns pull partition columns into default mode", func(t *testing.T) {
		// includeColumns match on the source column name, output keeps the resolved name
		require.Equal(t, map[string]string{
			"_meta_col":               "string",
			"part_col":                "timestamptz",
			constants.StringifiedData: "string",
		}, fieldTypes(schema.ToIceberg(true, stream, "Part Col")))
	})
}

// treeDataTypes returns every DataType present in typecastTree.
func treeDataTypes(t *testing.T) []DataType {
	t.Helper()
	var all []DataType
	var walk func(node *typeNode)
	walk = func(node *typeNode) {
		all = append(all, node.t)
		for _, child := range node.children {
			walk(child)
		}
	}
	walk(typecastTree)
	return all
}

// treeAncestors returns, for every DataType in typecastTree, the set of its ancestors
// (including itself).
func treeAncestors(t *testing.T) map[DataType]map[DataType]bool {
	t.Helper()
	ancestors := make(map[DataType]map[DataType]bool)
	var walk func(node *typeNode, path []DataType)
	walk = func(node *typeNode, path []DataType) {
		path = append(path, node.t)
		set := make(map[DataType]bool, len(path))
		for _, ancestor := range path {
			set[ancestor] = true
		}
		ancestors[node.t] = set
		for _, child := range node.children {
			walk(child, path)
		}
	}
	walk(typecastTree, nil)
	return ancestors
}
