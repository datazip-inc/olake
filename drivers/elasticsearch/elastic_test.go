package main

import (
	"os"
	"testing"
)

// TestFullBackfill_LocalElasticsearch is a placeholder integration test
// that can be run manually against a live Elasticsearch instance.
// To run: set ES_INTEGRATION_TEST=1 and use ./elastic-driver discover
func TestFullBackfill_LocalElasticsearch(t *testing.T) {
	if os.Getenv("ES_INTEGRATION_TEST") == "" {
		t.Skip("Skipping integration test. Set ES_INTEGRATION_TEST=1 to enable.")
	}
	t.Log("✓ Integration test placeholder (driver tested via discover/spec commands)")
}
