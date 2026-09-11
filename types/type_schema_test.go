package types

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination/iceberg/proto"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetCommonAncestorType(t *testing.T) {
	testCases := []struct {
		name     string
		t1, t2   DataType
		expected DataType
	}{
		{
			name:     "same type resolves to itself",
			t1:       Int64,
			t2:       Int64,
			expected: Int64,
		},
		{
			name:     "ancestor absorbs descendant",
			t1:       Int32,
			t2:       Int64,
			expected: Int64,
		},
		{
			name:     "bool promotes to int32",
			t1:       Bool,
			t2:       Int32,
			expected: Int32,
		},
		{
			name:     "int32 and float32 split at float64",
			t1:       Int32,
			t2:       Float32,
			expected: Float64,
		},
		{
			name:     "int64 and float32 split at float64",
			t1:       Int64,
			t2:       Float32,
			expected: Float64,
		},
		{
			name:     "numeric and timestamp only meet at string",
			t1:       Int64,
			t2:       Timestamp,
			expected: String,
		},
		{
			name:     "string absorbs timestamps",
			t1:       String,
			t2:       TimestampNano,
			expected: String,
		},
		{
			name:     "timestamp promotes to the wider precision",
			t1:       Timestamp,
			t2:       TimestampMicro,
			expected: TimestampMicro,
		},
		{
			name:     "milli and nano promote to nano",
			t1:       TimestampMilli,
			t2:       TimestampNano,
			expected: TimestampNano,
		},
		{
			name:     "object promotes to string",
			t1:       Object,
			t2:       Int32,
			expected: String,
		},
		{
			name:     "array promotes to string",
			t1:       Array,
			t2:       TimestampNano,
			expected: String,
		},
		{
			name:     "null promotes to string",
			t1:       Null,
			t2:       Int64,
			expected: String,
		},
		{
			name:     "unknown promotes to string",
			t1:       Unknown,
			t2:       Int64,
			expected: String,
		},
		{
			name:     "object and array meet at string",
			t1:       Object,
			t2:       Array,
			expected: String,
		},
		{
			name:     "unknown resolves to itself",
			t1:       Unknown,
			t2:       Unknown,
			expected: Unknown,
		},
		{
			name:     "string promotes to binary",
			t1:       String,
			t2:       Binary,
			expected: Binary,
		},
		{
			name:     "binary resolves to itself",
			t1:       Binary,
			t2:       Binary,
			expected: Binary,
		},
		{
			name:     "binary absorbs numerics",
			t1:       Int64,
			t2:       Binary,
			expected: Binary,
		},
		{
			name:     "binary absorbs timestamps",
			t1:       TimestampNano,
			t2:       Binary,
			expected: Binary,
		},
		{
			name:     "object meets binary through string",
			t1:       Object,
			t2:       Binary,
			expected: Binary,
		},
		{
			name:     "fixed binary resolves to itself",
			t1:       FixedBinaryOf(16),
			t2:       FixedBinaryOf(16),
			expected: FixedBinaryOf(16),
		},
		{
			name:     "fixed binaries of different lengths widen to binary",
			t1:       FixedBinaryOf(16),
			t2:       FixedBinaryOf(32),
			expected: Binary,
		},
		{
			name:     "length-less fixed binary widens to binary against a sized one",
			t1:       FixedBinary,
			t2:       FixedBinaryOf(16),
			expected: Binary,
		},
		{
			name:     "binary absorbs fixed binary",
			t1:       FixedBinaryOf(16),
			t2:       Binary,
			expected: Binary,
		},
		{
			name:     "fixed binary and string meet at binary",
			t1:       FixedBinaryOf(16),
			t2:       String,
			expected: Binary,
		},
		{
			name:     "fixed binary and numerics meet at binary",
			t1:       FixedBinaryOf(16),
			t2:       Int64,
			expected: Binary,
		},
		{
			name:     "fixed binary and object meet at binary",
			t1:       FixedBinaryOf(16),
			t2:       Object,
			expected: Binary,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, GetCommonAncestorType(tc.t1, tc.t2))
			require.Equal(t, tc.expected, GetCommonAncestorType(tc.t2, tc.t1), "GetCommonAncestorType must be symmetric")
		})
	}
}

func TestGetCommonAncestorTypeUndeclared(t *testing.T) {
	undeclared := DataType("undeclared_type")
	for _, other := range treeDataTypes(t) {
		require.Equal(t, String, GetCommonAncestorType(undeclared, other), "ancestor of %s and %s must be String", undeclared, other)
		require.Equal(t, String, GetCommonAncestorType(other, undeclared), "ancestor of %s and %s must be String", other, undeclared)
	}
	require.Equal(t, undeclared, GetCommonAncestorType(undeclared, undeclared), "a type is its own ancestor, declared or not")
}

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

func TestTypecastTreeShape(t *testing.T) {
	require.Equal(t, Binary, typecastTree.t)
	var underBinary []DataType
	for _, child := range typecastTree.children {
		underBinary = append(underBinary, child.t)
	}
	require.ElementsMatch(t, []DataType{String, FixedBinary}, underBinary)
}

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

func TestPropertyDataType(t *testing.T) {
	testCases := []struct {
		name     string
		types    []DataType
		expected DataType
	}{
		{
			name:     "single type resolves to itself",
			types:    []DataType{Int64},
			expected: Int64,
		},
		{
			name:     "only null resolves to null",
			types:    []DataType{Null},
			expected: Null,
		},
		{
			name:     "empty type set resolves to null",
			types:    nil,
			expected: Null,
		},
		{
			name:     "null is stripped alongside a real type",
			types:    []DataType{Null, Int64},
			expected: Int64,
		},
		{
			name:     "mixed numerics promote to ancestor",
			types:    []DataType{Int32, Float32},
			expected: Float64,
		},
		{
			name:     "incompatible types meet at string",
			types:    []DataType{Int64, Timestamp},
			expected: String,
		},
		{
			name:     "string with binary promotes to binary",
			types:    []DataType{String, Binary},
			expected: Binary,
		},
		{
			name:     "fixed binary keeps its length",
			types:    []DataType{Null, FixedBinaryOf(16)},
			expected: FixedBinaryOf(16),
		},
		{
			name:     "mixed fixed binary lengths widen to binary",
			types:    []DataType{FixedBinaryOf(16), FixedBinaryOf(32)},
			expected: Binary,
		},
		{
			name:     "null with incompatible types still meets at string",
			types:    []DataType{Null, Int64, Timestamp},
			expected: String,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			property := &Property{Type: NewSet(tc.types...)}
			require.Equal(t, tc.expected, property.DataType())
		})
	}
}

func TestPropertyNullable(t *testing.T) {
	testCases := []struct {
		name     string
		types    []DataType
		nullable bool
	}{
		{
			name:     "null alongside a real type",
			types:    []DataType{Null, Int64},
			nullable: true,
		},
		{
			name:     "only null",
			types:    []DataType{Null},
			nullable: true,
		},
		{
			name:     "no null",
			types:    []DataType{Int64},
			nullable: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.nullable, (&Property{Type: NewSet(tc.types...)}).Nullable())
		})
	}
}

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

func TestTypeSchemaToIcebergColumnSelection(t *testing.T) {
	testCases := []struct {
		name            string
		selectedColumns []string
		syncNewColumns  bool
		defaultColumns  bool
		includeColumns  []string
		expected        []string
	}{
		{
			name:            "nil selection includes all regular columns",
			selectedColumns: nil,
			expected:        []string{"id", "email", constants.OlakeID, constants.OlakeTimestamp, constants.OpType, constants.CdcTimestamp},
		},
		{
			name:            "blank selection includes only default columns",
			selectedColumns: []string{},
			defaultColumns:  true,
			expected:        []string{constants.OlakeID, constants.OlakeTimestamp, constants.OpType, constants.CdcTimestamp, constants.StringifiedData},
		},
		{
			name:            "one selected column",
			selectedColumns: []string{"id"},
			expected:        []string{"id", constants.OlakeID, constants.OlakeTimestamp, constants.OpType, constants.CdcTimestamp},
		},
		{
			name:            "all selected columns",
			selectedColumns: []string{"id", "email"},
			expected:        []string{"id", "email", constants.OlakeID, constants.OlakeTimestamp, constants.OpType, constants.CdcTimestamp},
		},
		{
			name:            "one selected column with sync new columns",
			selectedColumns: []string{"id"},
			syncNewColumns:  true,
			expected:        []string{"id", constants.OlakeID, constants.OlakeTimestamp, constants.OpType, constants.CdcTimestamp},
		},
		{
			name:            "all selected columns with sync new columns",
			selectedColumns: []string{"id", "email"},
			syncNewColumns:  true,
			expected:        []string{"id", "email", constants.OlakeID, constants.OlakeTimestamp, constants.OpType, constants.CdcTimestamp},
		},
		{
			name:            "default columns include selected regular column",
			selectedColumns: []string{"id"},
			defaultColumns:  true,
			includeColumns:  []string{"id"},
			expected:        []string{"id", constants.OlakeID, constants.OlakeTimestamp, constants.OpType, constants.CdcTimestamp, constants.StringifiedData},
		},
		{
			name:            "default columns include selected email column",
			selectedColumns: []string{"email"},
			defaultColumns:  true,
			includeColumns:  []string{"email"},
			expected:        []string{"email", constants.OlakeID, constants.OlakeTimestamp, constants.OpType, constants.CdcTimestamp, constants.StringifiedData},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			stream := NewStream("users", "public", nil)
			stream.UpsertField("id", Int64, false, false)
			stream.UpsertField("email", String, false, false)
			stream.UpsertField(constants.OlakeID, String, false, true)
			stream.UpsertField(constants.OlakeTimestamp, TimestampMicro, false, true)
			stream.UpsertField(constants.OpType, String, false, true)
			stream.UpsertField(constants.CdcTimestamp, TimestampMicro, true, true)
			configured := &ConfiguredStream{
				Stream: stream,
				StreamMetadata: StreamMetadata{
					SelectedColumns: &SelectedColumns{
						Columns:        testCase.selectedColumns,
						SyncNewColumns: testCase.syncNewColumns,
					},
				},
			}

			fields := stream.Schema.ToIceberg(testCase.defaultColumns, configured, testCase.includeColumns...)
			fieldNames := make([]string, 0, len(fields))
			for _, field := range fields {
				fieldNames = append(fieldNames, field.Key)
			}
			assert.ElementsMatch(t, testCase.expected, fieldNames)
		})
	}
}
