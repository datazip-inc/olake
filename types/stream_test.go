package types

import (
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestStream_NewStream(t *testing.T) {

	tests := []struct {
		//test identifier
		testName string

		//test values
		name           string
		namespace      string
		sourceDatabase *string
		viperPrefix    string

		//expected values
		expectedName      string
		expectedNamespace string
		expectedDatabase  string
		expectedTable     string
	}{
		{
			testName:       "stream with empty 'name' but all other fields filled in",
			name:           "",
			namespace:      "grades",
			sourceDatabase: stringPtr("gradesdb"),

			expectedName:      "",
			expectedNamespace: "grades",
			expectedDatabase:  "gradesdb:grades",
			expectedTable:     "",
		},
		{
			testName:       "stream with empty 'namespace' but all other fields filled in",
			name:           "students",
			namespace:      "",
			sourceDatabase: stringPtr("gradesdb"),

			expectedName:      "students",
			expectedNamespace: "",
			expectedDatabase:  "gradesdb",
			expectedTable:     "students",
		},
		{
			testName:       "stream with nil 'sourceDatabase' but all other fields filled in",
			name:           "students",
			namespace:      "grades",
			sourceDatabase: nil,

			expectedName:      "students",
			expectedNamespace: "grades",
			expectedDatabase:  ":grades",
			expectedTable:     "students",
		},
		{
			testName:       "stream with empty 'sourceDatabase' but all other fields filled in",
			name:           "students",
			namespace:      "grades",
			sourceDatabase: stringPtr(""),

			expectedName:      "students",
			expectedNamespace: "grades",
			expectedDatabase:  ":grades",
			expectedTable:     "students",
		},
		{
			testName:       "stream with all fields empty or nil",
			name:           "",
			namespace:      "",
			sourceDatabase: nil,

			expectedName:      "",
			expectedNamespace: "",
			expectedDatabase:  "",
			expectedTable:     "",
		},
		{
			testName:       "stream with all fields filled in",
			name:           "students",
			namespace:      "grades",
			sourceDatabase: stringPtr("gradesdb"),

			expectedName:      "students",
			expectedNamespace: "grades",
			expectedDatabase:  "gradesdb:grades",
			expectedTable:     "students",
		},
		{
			testName:       "stream with uppercase and special characters in 'name' and 'namespace",
			name:           "User-Orders.v2",
			namespace:      "My.Schema",
			sourceDatabase: nil,

			expectedName:      "User-Orders.v2",
			expectedNamespace: "My.Schema",
			expectedDatabase:  ":my_schema",
			expectedTable:     "user_orders_v2",
		},
		{
			testName:       "stream with a custom prefix",
			name:           "students",
			namespace:      "grades",
			sourceDatabase: stringPtr("gradesdb"),
			viperPrefix:    "analytics",

			expectedName:      "students",
			expectedNamespace: "grades",
			expectedDatabase:  "analytics_gradesdb:grades",
			expectedTable:     "students",
		},
	}

	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {

			if tt.viperPrefix != "" {
				viper.Set(constants.DestinationDatabasePrefix, tt.viperPrefix)
				t.Cleanup(func() { viper.Set(constants.DestinationDatabasePrefix, "") })
			}

			asserts := assert.New(t)

			stream := NewStream(tt.name, tt.namespace, tt.sourceDatabase)

			asserts.Equal(tt.expectedName, stream.Name)
			asserts.Equal(tt.expectedNamespace, stream.Namespace)
			asserts.NotNil(stream.SupportedSyncModes, "SupportedSyncModes should be initialized")
			asserts.NotNil(stream.SourceDefinedPrimaryKey, "SourceDefinedPrimaryKey should be initialized")
			asserts.NotNil(stream.AvailableCursorFields, "AvailableCursorFields should be initialized")
			asserts.NotNil(stream.Schema, "Schema should be initialized")
			asserts.Equal(tt.expectedDatabase, stream.DestinationDatabase)
			asserts.Equal(tt.expectedTable, stream.DestinationTable)
		})
	}
}

func stringPtr(s string) *string {
	return &s
}

func TestStream_WithSyncMode(t *testing.T) {
	tests := []struct {
		name             string
		modes            []SyncMode
		expectedModes    []SyncMode
		notExpectedModes []SyncMode
	}{
		{
			name:             "single mode",
			modes:            []SyncMode{FULLREFRESH},
			expectedModes:    []SyncMode{FULLREFRESH},
			notExpectedModes: []SyncMode{INCREMENTAL, CDC},
		},
		{
			name:             "multiple modes",
			modes:            []SyncMode{FULLREFRESH, INCREMENTAL},
			expectedModes:    []SyncMode{FULLREFRESH, INCREMENTAL},
			notExpectedModes: []SyncMode{CDC},
		},
		{
			name:             "all modes",
			modes:            []SyncMode{FULLREFRESH, INCREMENTAL, CDC, STRICTCDC},
			expectedModes:    []SyncMode{FULLREFRESH, INCREMENTAL, CDC, STRICTCDC},
			notExpectedModes: []SyncMode{},
		},
		{
			name:             "duplicate modes",
			modes:            []SyncMode{FULLREFRESH, FULLREFRESH, INCREMENTAL},
			expectedModes:    []SyncMode{FULLREFRESH, INCREMENTAL},
			notExpectedModes: []SyncMode{CDC},
		},
		{
			name:             "empty modes",
			modes:            []SyncMode{},
			expectedModes:    []SyncMode{},
			notExpectedModes: []SyncMode{FULLREFRESH, INCREMENTAL, CDC},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := NewStream("users", "public", nil)
			outputStream := stream.WithSyncMode(tt.modes...)

			// check if it returns the exact same pointer
			assert.Same(t, stream, outputStream, "Should return the same instance")

			// check if the set contains added modes
			for _, mode := range tt.expectedModes {
				assert.True(t, outputStream.SupportedSyncModes.Exists(mode), "Should contain %v", mode)
			}

			// check if the set does not contain other modes
			for _, mode := range tt.notExpectedModes {
				assert.False(t, outputStream.SupportedSyncModes.Exists(mode), "Should not contain %v", mode)
			}
		})
	}
}

func TestStream_WithPrimaryKey(t *testing.T) {
	tests := []struct {
		name            string
		keys            []string
		expectedKeys    []string
		notExpectedKeys []string
	}{
		{
			name:            "single key",
			keys:            []string{"id"},
			expectedKeys:    []string{"id"},
			notExpectedKeys: []string{"user_uuid", "created_at"},
		},
		{
			name:            "multiple keys",
			keys:            []string{"id", "user_uuid"},
			expectedKeys:    []string{"id", "user_uuid"},
			notExpectedKeys: []string{"created_at"},
		},
		{
			name:            "composite key",
			keys:            []string{"tenant_id", "user_id", "order_id"},
			expectedKeys:    []string{"tenant_id", "user_id", "order_id"},
			notExpectedKeys: []string{"id"},
		},
		{
			name:            "duplicate keys",
			keys:            []string{"id", "id", "user_uuid"},
			expectedKeys:    []string{"id", "user_uuid"},
			notExpectedKeys: []string{"created_at"},
		},
		{
			name:            "empty keys",
			keys:            []string{},
			expectedKeys:    []string{},
			notExpectedKeys: []string{"id", "user_uuid"},
		},
		{
			name:            "keys with underscores",
			keys:            []string{"user_id", "order_id"},
			expectedKeys:    []string{"user_id", "order_id"},
			notExpectedKeys: []string{"userid", "orderid"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := NewStream("users", "public", nil)
			returnedStream := stream.WithPrimaryKey(tt.keys...)

			assert.Same(t, stream, returnedStream, "Should return the same instance")

			for _, key := range tt.expectedKeys {
				assert.True(t, stream.SourceDefinedPrimaryKey.Exists(key), "Should contain '%s'", key)
			}

			for _, key := range tt.notExpectedKeys {
				assert.False(t, stream.SourceDefinedPrimaryKey.Exists(key), "Should not contain '%s'", key)
			}
		})
	}
}

func TestStream_WithCursorField(t *testing.T) {
	tests := []struct {
		name              string
		fields            []string
		expectedFields    []string
		notExpectedFields []string
	}{
		{
			name:              "single field",
			fields:            []string{"updated_at"},
			expectedFields:    []string{"updated_at"},
			notExpectedFields: []string{"inserted_at", "id"},
		},
		{
			name:              "multiple fields",
			fields:            []string{"updated_at", "inserted_at"},
			expectedFields:    []string{"updated_at", "inserted_at"},
			notExpectedFields: []string{"id"},
		},
		{
			name:              "timestamp fields",
			fields:            []string{"created_at", "updated_at", "deleted_at"},
			expectedFields:    []string{"created_at", "updated_at", "deleted_at"},
			notExpectedFields: []string{"id"},
		},
		{
			name:              "duplicate fields",
			fields:            []string{"updated_at", "updated_at", "inserted_at"},
			expectedFields:    []string{"updated_at", "inserted_at"},
			notExpectedFields: []string{"id"},
		},
		{
			name:              "empty fields",
			fields:            []string{},
			expectedFields:    []string{},
			notExpectedFields: []string{"updated_at", "inserted_at"},
		},
		{
			name:              "fields with underscores",
			fields:            []string{"last_modified", "date_created"},
			expectedFields:    []string{"last_modified", "date_created"},
			notExpectedFields: []string{"lastmodified", "datecreated"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := NewStream("users", "public", nil)
			outputStream := stream.WithCursorField(tt.fields...)

			assert.Same(t, stream, outputStream, "Should return the same instance")

			for _, field := range tt.expectedFields {
				assert.True(t, stream.AvailableCursorFields.Exists(field), "Should contain '%s'", field)
			}

			for _, field := range tt.notExpectedFields {
				assert.False(t, stream.AvailableCursorFields.Exists(field), "Should not contain '%s'", field)
			}
		})
	}
}

func TestStream_WithSchema(t *testing.T) {
	t.Run("set new schema", func(t *testing.T) {
		stream := NewStream("users", "public", nil)
		newSchema := NewTypeSchema()
		returnedStream := stream.WithSchema(newSchema)

		assert.Same(t, stream, returnedStream, "Should return the same stream instance")
		assert.Same(t, newSchema, stream.Schema, "Stream.Schema should point to the input schema")
	})

	t.Run("replace existing schema", func(t *testing.T) {
		stream := NewStream("users", "public", nil)
		firstSchema := NewTypeSchema()
		secondSchema := NewTypeSchema()

		stream.WithSchema(firstSchema)
		assert.Same(t, firstSchema, stream.Schema, "First schema should be set")

		returnedStream := stream.WithSchema(secondSchema)
		assert.Same(t, stream, returnedStream, "Should return the same stream instance")
		assert.Same(t, secondSchema, stream.Schema, "Second schema should replace first schema")
		assert.NotSame(t, firstSchema, stream.Schema, "First schema should no longer be referenced")
	})
}

func TestStream_Wrap(t *testing.T) {
	tests := []struct {
		name      string
		syncIndex int
	}{
		{
			name:      "wrap with index 0",
			syncIndex: 0,
		},
		{
			name:      "wrap with index 1",
			syncIndex: 1,
		},
		{
			name:      "wrap with negative index",
			syncIndex: -1,
		},
		{
			name:      "wrap with large index",
			syncIndex: 100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := NewStream("users", "public", nil)
			configuredStream := stream.Wrap(tt.syncIndex)

			assert.NotNil(t, configuredStream, "Should return a configuredStream")
			assert.Same(t, stream, configuredStream.Stream, "Should wrap the exact same stream instance")
		})
	}
}
