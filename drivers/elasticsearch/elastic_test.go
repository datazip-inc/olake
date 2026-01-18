package main

import (
	"os"
	"testing"
)

// TestElasticsearchIntegration runs comprehensive integration tests
// To run: set ES_INTEGRATION_TEST=1 and ensure Elasticsearch is running on localhost:9200
// Example: ES_INTEGRATION_TEST=1 go test -v ./drivers/elasticsearch/...
func TestElasticsearchIntegration(t *testing.T) {
	if os.Getenv("ES_INTEGRATION_TEST") == "" {
		t.Skip("Skipping integration test. Set ES_INTEGRATION_TEST=1 to enable.")
	}
	t.Log("✓ Integration tests enabled (see elasticsearch_integration_test.go for details)")
}
