package driver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils/testutils"
	"github.com/stretchr/testify/require"
)

// Elasticsearch test constants
const (
	ElasticsearchPort = 9200
	ElasticsearchURL  = "http://localhost:9200"
	TestIndex         = "olake_test_integration"
	TestNamespace     = "elasticsearch_test"
)

// Test data variables
var (
	ExpectedElasticsearchData = map[string]interface{}{
		"id":         "1",
		"name":       "John Doe",
		"age":        30,
		"salary":     75000.50,
		"active":     true,
		"created_at": arrow.Timestamp(time.Date(2023, 1, 1, 10, 0, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
		"tags":       `["tag1","tag2"]`,
		"metadata":   `{"department":"Engineering","level":5}`,
	}

	ExpectedUpdatedData = map[string]interface{}{
		"id":         "1",
		"name":       "John Smith",
		"age":        31,
		"salary":     85000.75,
		"active":     false,
		"created_at": arrow.Timestamp(time.Date(2023, 1, 1, 10, 0, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
		"tags":       `["tag1","tag2","tag3"]`,
		"metadata":   `{"department":"Management","level":7}`,
	}

	ElasticsearchToDestinationSchema = map[string]string{
		"id":         "string",
		"name":       "string",
		"age":        "bigint",
		"salary":     "double",
		"active":     "boolean",
		"created_at": "timestamp",
		"tags":       "string",
		"metadata":   "string",
	}

	UpdatedElasticsearchToDestinationSchema = map[string]string{
		"id":         "string",
		"name":       "string",
		"age":        "bigint",
		"salary":     "double",
		"active":     "boolean",
		"created_at": "timestamp",
		"tags":       "string",
		"metadata":   "string",
	}
)

// ExecuteQuery manages test data in Elasticsearch
func ExecuteQuery(ctx context.Context, t *testing.T, streams []string, operation string, fileConfig bool) {
	t.Helper()

	indexName := streams[0]

	switch operation {
	case "create":
		createTestIndex(ctx, t, indexName)

	case "drop":
		deleteTestIndex(ctx, t, indexName)

	case "clean":
		deleteTestIndex(ctx, t, indexName)
		createTestIndex(ctx, t, indexName)

	case "add":
		createTestIndex(ctx, t, indexName)
		insertInitialTestData(ctx, t, indexName)

	case "insert":
		insertIncrementalData(ctx, t, indexName)

	case "update":
		updateTestData(ctx, t, indexName)

	case "delete":
		deleteTestData(ctx, t, indexName)

	case "evolve-schema":
		// For Elasticsearch, schema evolution is implicit when adding new fields or changing types
		// We'll add a new optional field to the document
		addNewFieldToDocument(ctx, t, indexName)

	default:
		t.Logf("Unknown operation: %s", operation)
	}
}

// createTestIndex creates a new Elasticsearch index for testing
func createTestIndex(ctx context.Context, t *testing.T, indexName string) {
	t.Helper()

	// First, delete if exists
	req, err := http.NewRequestWithContext(ctx, "DELETE", fmt.Sprintf("%s/%s", ElasticsearchURL, indexName), nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Create index with mapping
	mapping := map[string]interface{}{
		"mappings": map[string]interface{}{
			"properties": map[string]interface{}{
				"id": map[string]interface{}{
					"type": "keyword",
				},
				"name": map[string]interface{}{
					"type": "text",
				},
				"age": map[string]interface{}{
					"type": "integer",
				},
				"salary": map[string]interface{}{
					"type": "double",
				},
				"active": map[string]interface{}{
					"type": "boolean",
				},
				"created_at": map[string]interface{}{
					"type":   "date",
					"format": "strict_date_optional_time||epoch_millis",
				},
				"tags": map[string]interface{}{
					"type": "text",
				},
				"metadata": map[string]interface{}{
					"type": "object",
				},
			},
		},
	}

	body, err := json.Marshal(mapping)
	require.NoError(t, err)

	req, err = http.NewRequestWithContext(ctx, "PUT", fmt.Sprintf("%s/%s", ElasticsearchURL, indexName), bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.True(t, resp.StatusCode >= 200 && resp.StatusCode < 300, "Failed to create index, status: %d", resp.StatusCode)
	t.Logf("Created index: %s", indexName)
}

// deleteTestIndex deletes an Elasticsearch index
func deleteTestIndex(ctx context.Context, t *testing.T, indexName string) {
	t.Helper()

	req, err := http.NewRequestWithContext(ctx, "DELETE", fmt.Sprintf("%s/%s", ElasticsearchURL, indexName), nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	t.Logf("Deleted index: %s", indexName)
}

// insertInitialTestData inserts test documents
func insertInitialTestData(ctx context.Context, t *testing.T, indexName string) {
	t.Helper()

	doc := map[string]interface{}{
		"id":         "1",
		"name":       "John Doe",
		"age":        30,
		"salary":     75000.50,
		"active":     true,
		"created_at": "2023-01-01T10:00:00Z",
		"tags":       []string{"tag1", "tag2"},
		"metadata": map[string]interface{}{
			"department": "Engineering",
			"level":      5,
		},
	}

	// Insert 5 documents for full refresh test
	for i := 1; i <= 5; i++ {
		doc["id"] = fmt.Sprintf("%d", i)
		doc["name"] = fmt.Sprintf("Person %d", i)
		doc["age"] = 25 + i
		doc["salary"] = 70000.0 + float64(i*1000)

		body, err := json.Marshal(doc)
		require.NoError(t, err)

		req, err := http.NewRequestWithContext(ctx, "POST",
			fmt.Sprintf("%s/%s/_doc/%d?refresh=true", ElasticsearchURL, indexName, i),
			bytes.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		require.True(t, resp.StatusCode >= 200 && resp.StatusCode < 300,
			"Failed to insert document, status: %d", resp.StatusCode)
	}

	t.Logf("Inserted 5 test documents into index: %s", indexName)
}

// insertIncrementalData inserts new documents for incremental sync
func insertIncrementalData(ctx context.Context, t *testing.T, indexName string) {
	t.Helper()

	doc := map[string]interface{}{
		"id":         "6",
		"name":       "Jane Doe",
		"age":        28,
		"salary":     80000.00,
		"active":     true,
		"created_at": "2024-01-15T14:30:00Z",
		"tags":       []string{"tag1"},
		"metadata": map[string]interface{}{
			"department": "Sales",
			"level":      3,
		},
	}

	body, err := json.Marshal(doc)
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(ctx, "POST",
		fmt.Sprintf("%s/%s/_doc/6?refresh=true", ElasticsearchURL, indexName),
		bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.True(t, resp.StatusCode >= 200 && resp.StatusCode < 300,
		"Failed to insert incremental document, status: %d", resp.StatusCode)

	t.Logf("Inserted incremental document into index: %s", indexName)
}

// updateTestData updates test documents for incremental sync
func updateTestData(ctx context.Context, t *testing.T, indexName string) {
	t.Helper()

	updateDoc := map[string]interface{}{
		"doc": map[string]interface{}{
			"id":     "1",
			"name":   "John Smith",
			"age":    31,
			"salary": 85000.75,
			"active": false,
			"tags":   []string{"tag1", "tag2", "tag3"},
			"metadata": map[string]interface{}{
				"department": "Management",
				"level":      7,
			},
		},
	}

	body, err := json.Marshal(updateDoc)
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(ctx, "POST",
		fmt.Sprintf("%s/%s/_update/1?refresh=true", ElasticsearchURL, indexName),
		bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.True(t, resp.StatusCode >= 200 && resp.StatusCode < 300,
		"Failed to update document, status: %d", resp.StatusCode)

	t.Logf("Updated document in index: %s", indexName)
}

// deleteTestData deletes a document
func deleteTestData(ctx context.Context, t *testing.T, indexName string) {
	t.Helper()

	req, err := http.NewRequestWithContext(ctx, "DELETE",
		fmt.Sprintf("%s/%s/_doc/1?refresh=true", ElasticsearchURL, indexName), nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.True(t, resp.StatusCode >= 200 && resp.StatusCode < 300,
		"Failed to delete document, status: %d", resp.StatusCode)

	t.Logf("Deleted document from index: %s", indexName)
}

// addNewFieldToDocument adds a new field to simulate schema evolution
func addNewFieldToDocument(ctx context.Context, t *testing.T, indexName string) {
	t.Helper()

	updateDoc := map[string]interface{}{
		"doc": map[string]interface{}{
			"email": "john.smith@example.com",
		},
	}

	body, err := json.Marshal(updateDoc)
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(ctx, "POST",
		fmt.Sprintf("%s/%s/_update/1?refresh=true", ElasticsearchURL, indexName),
		bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.True(t, resp.StatusCode >= 200 && resp.StatusCode < 300,
		"Failed to add field for schema evolution, status: %d", resp.StatusCode)

	t.Logf("Added new field for schema evolution in index: %s", indexName)
}

// TestElasticsearchIntegration tests full integration of Elasticsearch driver
func TestElasticsearchIntegration(t *testing.T) {
	t.Parallel()
	testConfig := &testutils.IntegrationTest{
		TestConfig:                       testutils.GetTestConfig(string(constants.Elasticsearch)),
		Namespace:                        TestNamespace,
		ExpectedData:                     ExpectedElasticsearchData,
		ExpectedUpdatedData:              ExpectedUpdatedData,
		DestinationDataTypeSchema:        ElasticsearchToDestinationSchema,
		UpdatedDestinationDataTypeSchema: UpdatedElasticsearchToDestinationSchema,
		ExecuteQuery:                     ExecuteQuery,
		DestinationDB:                    "elasticsearch_elasticsearch_test",
		CursorField:                      "created_at",
		PartitionRegex:                   "/{id}",
	}
	testConfig.TestIntegration(t)
}

// TestElasticsearchPerformance tests performance of Elasticsearch driver
func TestElasticsearchPerformance(t *testing.T) {
	config := &testutils.PerformanceTest{
		TestConfig:      testutils.GetTestConfig(string(constants.Elasticsearch)),
		Namespace:       TestNamespace,
		BackfillStreams: []string{TestIndex},
		CDCStreams:      []string{TestIndex + "_cdc"},
		ExecuteQuery:    ExecuteQuery,
	}

	config.TestPerformance(t)
}
