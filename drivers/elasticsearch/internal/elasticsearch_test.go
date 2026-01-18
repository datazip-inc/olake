package driver

import (
	"testing"

	"github.com/datazip-inc/olake/types"
)

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name: "valid config with host and port",
			config: Config{
				Host:     "localhost",
				Port:     9200,
				Username: "elastic",
				Password: "changeme",
				Index:    "test-index",
			},
			wantErr: false,
		},
		{
			name: "valid config with cloud_id",
			config: Config{
				CloudID: "my-deployment:dXMtZWFzdC0x",
				APIKey:  "my-api-key",
				Index:   "test-index",
			},
			wantErr: false,
		},
		{
			name: "missing host and cloud_id",
			config: Config{
				Username: "elastic",
				Password: "changeme",
				Index:    "test-index",
			},
			wantErr: true,
		},
		{
			name: "missing authentication",
			config: Config{
				Host:  "localhost",
				Port:  9200,
				Index: "test-index",
			},
			wantErr: true,
		},
		{
			name: "missing index",
			config: Config{
				Host:     "localhost",
				Port:     9200,
				Username: "elastic",
				Password: "changeme",
			},
			wantErr: true,
		},
		{
			name: "invalid port",
			config: Config{
				Host:     "localhost",
				Port:     99999,
				Username: "elastic",
				Password: "changeme",
				Index:    "test-index",
			},
			wantErr: true,
		},
		{
			name: "host with http prefix",
			config: Config{
				Host:     "http://localhost",
				Port:     9200,
				Username: "elastic",
				Password: "changeme",
				Index:    "test-index",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Config.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestTypeMapping(t *testing.T) {
	tests := []struct {
		esType   string
		expected types.DataType
	}{
		{"text", types.String},
		{"keyword", types.String},
		{"long", types.Int64},
		{"integer", types.Int64},
		{"short", types.Int64},
		{"byte", types.Int64},
		{"double", types.Float64},
		{"float", types.Float64},
		{"half_float", types.Float64},
		{"scaled_float", types.Float64},
		{"date", types.Timestamp},
		{"boolean", types.Bool},
		{"object", types.String},
		{"nested", types.String},
		{"geo_point", types.String},
		{"ip", types.String},
	}

	for _, tt := range tests {
		t.Run(tt.esType, func(t *testing.T) {
			got, exists := esTypeToDataTypes[tt.esType]
			if !exists {
				t.Errorf("Type %s not found in mapping", tt.esType)
				return
			}
			if got != tt.expected {
				t.Errorf("esTypeToDataTypes[%s] = %v, want %v", tt.esType, got, tt.expected)
			}
		})
	}
}

func TestDriverInterface(t *testing.T) {
	driver := &Elasticsearch{
		config: &Config{},
	}

	// Test Type
	if driver.Type() != "elasticsearch" {
		t.Errorf("Type() = %v, want elasticsearch", driver.Type())
	}

	// Test CDCSupported
	if driver.CDCSupported() {
		t.Error("CDCSupported() = true, want false")
	}

	// Test MaxConnections with default
	driver.config.MaxThreads = 0
	if driver.MaxConnections() != 0 {
		t.Errorf("MaxConnections() with 0 threads = %v, want 0", driver.MaxConnections())
	}

	// Test MaxConnections with custom value
	driver.config.MaxThreads = 10
	if driver.MaxConnections() != 10 {
		t.Errorf("MaxConnections() = %v, want 10", driver.MaxConnections())
	}

	// Test MaxRetries
	driver.config.RetryCount = 3
	if driver.MaxRetries() != 3 {
		t.Errorf("MaxRetries() = %v, want 3", driver.MaxRetries())
	}
}

func TestBuildIncrementalQuery(t *testing.T) {
	driver := &Elasticsearch{}

	tests := []struct {
		name         string
		cursorField  string
		lastCursor   interface{}
		expectRange  bool
	}{
		{
			name:        "with cursor value",
			cursorField: "@timestamp",
			lastCursor:  "2025-01-01T00:00:00Z",
			expectRange: true,
		},
		{
			name:        "without cursor value",
			cursorField: "@timestamp",
			lastCursor:  nil,
			expectRange: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := driver.buildIncrementalQuery(tt.cursorField, tt.lastCursor)
			
			if query == nil {
				t.Error("buildIncrementalQuery() returned nil")
				return
			}

			// Check if query has the expected structure
			if _, ok := query["query"]; !ok {
				t.Error("Query missing 'query' field")
			}
		})
	}
}

func TestConfigDefaults(t *testing.T) {
	config := &Config{
		Host:     "localhost",
		Port:     9200,
		Username: "elastic",
		Password: "changeme",
		Index:    "test-index",
	}

	err := config.Validate()
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	// Check defaults are set
	if config.MaxThreads == 0 {
		t.Error("MaxThreads should be set to default value")
	}
}

func TestProcessProperties(t *testing.T) {
	driver := &Elasticsearch{}
	stream := types.NewStream("test_index", "elasticsearch", nil)

	properties := map[string]interface{}{
		"id": map[string]interface{}{
			"type": "keyword",
		},
		"name": map[string]interface{}{
			"type": "text",
		},
		"age": map[string]interface{}{
			"type": "integer",
		},
		"user_id": map[string]interface{}{
			"type": "long",
		},
		"score": map[string]interface{}{
			"type": "float",
		},
		"rating": map[string]interface{}{
			"type": "double",
		},
		"is_active": map[string]interface{}{
			"type": "boolean",
		},
		"created_date": map[string]interface{}{
			"type": "date",
		},
		"last_login": map[string]interface{}{
			"type": "date",
		},
		"metadata": map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"department": map[string]interface{}{
					"type": "keyword",
				},
				"level": map[string]interface{}{
					"type": "integer",
				},
			},
		},
		"addresses": map[string]interface{}{
			"type": "nested",
			"properties": map[string]interface{}{
				"street": map[string]interface{}{
					"type": "text",
				},
				"city": map[string]interface{}{
					"type": "keyword",
				},
			},
		},
	}

	driver.processProperties(stream, properties, "")

	// Verify all fields were added
	expectedFields := []string{
		"id", "name", "age", "user_id", "score", "rating", "is_active",
		"created_date", "last_login", "metadata.department", "metadata.level",
		"addresses.street", "addresses.city",
	}

	for _, field := range expectedFields {
		found, _ := stream.Schema.GetProperty(field)
		if !found {
			t.Errorf("Field %s not found in schema", field)
		}
	}
}

func TestStateManagement(t *testing.T) {
	stream := types.NewStream("test_index", "elasticsearch", nil)
	stream.UpsertField("timestamp", types.Timestamp, true)

	// Verify field was added
	found, _ := stream.Schema.GetProperty("timestamp")
	if !found {
		t.Error("timestamp field not found in schema")
	}
}

func TestStreamInterface(t *testing.T) {
	driver := &Elasticsearch{
		config: &Config{
			Host:     "localhost",
			Port:     9200,
			Username: "elastic",
			Password: "changeme",
			Index:    "test-index",
		},
	}

	// Test Type
	if driver.Type() != "elasticsearch" {
		t.Errorf("Type() = %v, want elasticsearch", driver.Type())
	}

	// Test CDCSupported
	if driver.CDCSupported() {
		t.Error("CDCSupported() = true, want false")
	}

	// Test StateType
	if driver.StateType() != types.GlobalType {
		t.Errorf("StateType() = %v, want GlobalType", driver.StateType())
	}

	// Test MaxConnections
	driver.config.MaxThreads = 10
	if driver.MaxConnections() != 10 {
		t.Errorf("MaxConnections() = %v, want 10", driver.MaxConnections())
	}

	// Test MaxRetries
	driver.config.RetryCount = 3
	if driver.MaxRetries() != 3 {
		t.Errorf("MaxRetries() = %v, want 3", driver.MaxRetries())
	}
}
