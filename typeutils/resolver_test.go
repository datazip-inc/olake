package typeutils

import (
	"testing"
	"time"

	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolve(t *testing.T) {
	testCases := []struct {
		name     string
		objects  []map[string]interface{}
		expected map[string]struct {
			dataType types.DataType
			nullable bool
		}
	}{
		{
			name: "basic types",
			objects: []map[string]interface{}{
				{
					"string_field": "test string",
					"int_field":    int32(42),
					"float_field":  float64(3.14),
					"bool_field":   true,
				},
			},
			expected: map[string]struct {
				dataType types.DataType
				nullable bool
			}{
				"string_field": {dataType: types.String, nullable: false},
				"int_field":    {dataType: types.Int32, nullable: false},
				"float_field":  {dataType: types.Float64, nullable: false},
				"bool_field":   {dataType: types.Bool, nullable: false},
			},
		},
		{
			name: "all integer types",
			objects: []map[string]interface{}{
				{
					"int_field":    int(42),
					"int8_field":   int8(42),
					"int16_field":  int16(42),
					"int32_field":  int32(42),
					"int64_field":  int64(42),
					"uint_field":   uint(42),
					"uint8_field":  uint8(42),
					"uint16_field": uint16(42),
					"uint32_field": uint32(42),
					"uint64_field": uint64(42),
				},
			},
			expected: map[string]struct {
				dataType types.DataType
				nullable bool
			}{
				"int_field":    {dataType: types.Int32, nullable: false},
				"int8_field":   {dataType: types.Int32, nullable: false},
				"int16_field":  {dataType: types.Int32, nullable: false},
				"int32_field":  {dataType: types.Int32, nullable: false},
				"int64_field":  {dataType: types.Int64, nullable: false},
				"uint_field":   {dataType: types.Int32, nullable: false},
				"uint8_field":  {dataType: types.Int32, nullable: false},
				"uint16_field": {dataType: types.Int32, nullable: false},
				"uint32_field": {dataType: types.Int32, nullable: false},
				"uint64_field": {dataType: types.Int64, nullable: false},
			},
		},
		{
			name: "float types",
			objects: []map[string]interface{}{
				{
					"float32_field": float32(3.14),
					"float64_field": float64(2.71828),
				},
			},
			expected: map[string]struct {
				dataType types.DataType
				nullable bool
			}{
				"float32_field": {dataType: types.Float32, nullable: false},
				"float64_field": {dataType: types.Float64, nullable: false},
			},
		},
		{
			name: "timestamp strings",
			objects: []map[string]interface{}{
				{
					"timestamp_field":         "2024-03-19T15:30:00Z",
					"timestamp_ms_field":      "2024-03-19T15:30:00.123Z",
					"timestamp_micros_field":  "2024-03-19T15:30:00.123456Z",
					"timestamp_nanos_field":   "2024-03-19T15:30:00.123456789Z",
					"invalid_timestamp_field": "not a timestamp",
				},
			},
			expected: map[string]struct {
				dataType types.DataType
				nullable bool
			}{
				"timestamp_field":         {dataType: types.Timestamp, nullable: false},
				"timestamp_ms_field":      {dataType: types.TimestampMilli, nullable: false},
				"timestamp_micros_field":  {dataType: types.TimestampMicro, nullable: false},
				"timestamp_nanos_field":   {dataType: types.TimestampNano, nullable: false},
				"invalid_timestamp_field": {dataType: types.String, nullable: false},
			},
		},
		{
			name: "pointer types",
			objects: []map[string]interface{}{
				{
					"int_ptr":     func() *int { v := 42; return &v }(),
					"string_ptr":  func() *string { v := "test"; return &v }(),
					"bool_ptr":    func() *bool { v := true; return &v }(),
					"float64_ptr": func() *float64 { v := 3.14; return &v }(),
					"nil_ptr":     (*int)(nil),
				},
			},
			expected: map[string]struct {
				dataType types.DataType
				nullable bool
			}{
				"int_ptr":     {dataType: types.Int32, nullable: false},
				"string_ptr":  {dataType: types.String, nullable: false},
				"bool_ptr":    {dataType: types.Bool, nullable: false},
				"float64_ptr": {dataType: types.Float64, nullable: false},
				"nil_ptr":     {dataType: types.Null, nullable: true},
			},
		},
		{
			name: "nullable fields",
			objects: []map[string]interface{}{
				{
					"field1": "value1",
					"field2": int32(123),
				},
				{
					"field1": "value2",
					// field2 is missing, should be marked as nullable
				},
			},
			expected: map[string]struct {
				dataType types.DataType
				nullable bool
			}{
				"field1": {dataType: types.String, nullable: false},
				"field2": {dataType: types.Int32, nullable: true},
			},
		},
		{
			name: "array and slice types",
			objects: []map[string]interface{}{
				{
					"int_array":    [3]int{1, 2, 3},
					"string_array": [2]string{"a", "b"},
					"int_slice":    []int{1, 2, 3},
					"string_slice": []string{"a", "b"},
					"empty_slice":  []int{},
					"nil_slice":    []int(nil),
				},
			},
			expected: map[string]struct {
				dataType types.DataType
				nullable bool
			}{
				"int_array":    {dataType: types.Array, nullable: false},
				"string_array": {dataType: types.Array, nullable: false},
				"int_slice":    {dataType: types.Array, nullable: false},
				"string_slice": {dataType: types.Array, nullable: false},
				"empty_slice":  {dataType: types.Array, nullable: false},
				"nil_slice":    {dataType: types.Array, nullable: false},
			},
		},
		{
			name: "map types",
			objects: []map[string]interface{}{
				{
					"string_map": map[string]string{"key1": "value1", "key2": "value2"},
					"int_map":    map[string]int{"key1": 1, "key2": 2},
					"nested_map": map[string]map[string]int{"outer": {"inner": 1}},
					"empty_map":  map[string]interface{}{},
					"nil_map":    map[string]interface{}(nil),
				},
			},
			expected: map[string]struct {
				dataType types.DataType
				nullable bool
			}{
				"string_map": {dataType: types.Object, nullable: false},
				"int_map":    {dataType: types.Object, nullable: false},
				"nested_map": {dataType: types.Object, nullable: false},
				"empty_map":  {dataType: types.Object, nullable: false},
				"nil_map":    {dataType: types.Object, nullable: false},
			},
		},
		{
			name: "time types",
			objects: []map[string]interface{}{
				{
					"time_sec":    time.Date(2024, 3, 19, 15, 30, 0, 0, time.UTC),
					"time_ms":     time.Date(2024, 3, 19, 15, 30, 0, 123000000, time.UTC),
					"time_micros": time.Date(2024, 3, 19, 15, 30, 0, 123456000, time.UTC),
					"time_nanos":  time.Date(2024, 3, 19, 15, 30, 0, 123456789, time.UTC),
					"nil_time":    (*time.Time)(nil),
				},
			},
			expected: map[string]struct {
				dataType types.DataType
				nullable bool
			}{
				"time_sec":    {dataType: types.Timestamp, nullable: false},
				"time_ms":     {dataType: types.TimestampMilli, nullable: false},
				"time_micros": {dataType: types.TimestampMicro, nullable: false},
				"time_nanos":  {dataType: types.TimestampNano, nullable: false},
				"nil_time":    {dataType: types.Null, nullable: true},
			},
		},
		{
			name: "unknown types",
			objects: []map[string]interface{}{
				{
					"chan_field":   make(chan int),
					"func_field":   func() {},
					"struct_field": struct{}{},
					"nil_unknown":  nil,
				},
			},
			expected: map[string]struct {
				dataType types.DataType
				nullable bool
			}{
				"chan_field":   {dataType: types.Unknown, nullable: false},
				"func_field":   {dataType: types.Unknown, nullable: false},
				"struct_field": {dataType: types.Unknown, nullable: false},
				"nil_unknown":  {dataType: types.Null, nullable: true},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a new stream for testing
			stream := types.NewStream("test_stream", "test_namespace")

			err := Resolve(stream, tc.objects...)

			require.NoError(t, err)

			for fieldName, expected := range tc.expected {
				// Check if the field exists in the stream's schema
				property, found := stream.Schema.Properties.Load(fieldName)
				assert.True(t, found, "Field %s should exist in schema", fieldName)

				if found {
					prop := property.(*types.Property)
					typeSet := *(prop.Type)

					// Check field type
					assert.True(t, typeSet.Exists(expected.dataType),
						"Field %s should have type %s, got %v",
						fieldName, expected.dataType, typeSet)

					// Check nullability
					if expected.nullable {
						assert.True(t, typeSet.Exists(types.Null),
							"Field %s should be nullable", fieldName)
					} else {
						assert.False(t, typeSet.Exists(types.Null),
							"Field %s should not be nullable", fieldName)
					}
				}
			}
		})
	}
}

// TestResolveWithEmptyInput tests the edge case of resolving with no objects
func TestResolveWithEmptyInput(t *testing.T) {
	stream := types.NewStream("empty_stream", "test_namespace")

	err := Resolve(stream, []map[string]interface{}{}...)

	assert.NoError(t, err)

	assert.NotNil(t, stream.Schema)

	count := 0
	stream.Schema.Properties.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	assert.Equal(t, 0, count, "Expected empty schema")
}
