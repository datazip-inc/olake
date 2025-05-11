package types

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetWrappedCatalog(t *testing.T) {
	testCases := []struct {
		name     string
		streams  []*Stream
		expected *Catalog
	}{
		{
			name:    "empty streams",
			streams: []*Stream{},
			expected: &Catalog{
				Streams:         []*ConfiguredStream{},
				SelectedStreams: make(map[string][]StreamMetadata),
			},
		},
		{
			name: "single stream",
			streams: []*Stream{
				{
					Name:      "test_stream",
					Namespace: "test_namespace",
				},
			},
			expected: &Catalog{
				Streams: []*ConfiguredStream{
					{
						Stream: &Stream{
							Name:      "test_stream",
							Namespace: "test_namespace",
						},
					},
				},
				SelectedStreams: map[string][]StreamMetadata{
					"test_namespace": {
						{
							StreamName:     "test_stream",
							PartitionRegex: "",
						},
					},
				},
			},
		},
		{
			name: "multiple streams same namespace",
			streams: []*Stream{
				{
					Name:      "stream1",
					Namespace: "namespace1",
				},
				{
					Name:      "stream2",
					Namespace: "namespace1",
				},
			},
			expected: &Catalog{
				Streams: []*ConfiguredStream{
					{
						Stream: &Stream{
							Name:      "stream1",
							Namespace: "namespace1",
						},
					},
					{
						Stream: &Stream{
							Name:      "stream2",
							Namespace: "namespace1",
						},
					},
				},
				SelectedStreams: map[string][]StreamMetadata{
					"namespace1": {
						{
							StreamName:     "stream1",
							PartitionRegex: "",
						},
						{
							StreamName:     "stream2",
							PartitionRegex: "",
						},
					},
				},
			},
		},
		{
			name: "multiple streams different namespaces",
			streams: []*Stream{
				{
					Name:      "stream1",
					Namespace: "namespace1",
				},
				{
					Name:      "stream2",
					Namespace: "namespace2",
				},
			},
			expected: &Catalog{
				Streams: []*ConfiguredStream{
					{
						Stream: &Stream{
							Name:      "stream1",
							Namespace: "namespace1",
						},
					},
					{
						Stream: &Stream{
							Name:      "stream2",
							Namespace: "namespace2",
						},
					},
				},
				SelectedStreams: map[string][]StreamMetadata{
					"namespace1": {
						{
							StreamName:     "stream1",
							PartitionRegex: "",
						},
					},
					"namespace2": {
						{
							StreamName:     "stream2",
							PartitionRegex: "",
						},
					},
				},
			},
		},
		{
			name: "stream with full properties",
			streams: []*Stream{
				{
					Name:      "users",
					Namespace: "public",
					Schema: &TypeSchema{
						Properties: sync.Map{},
					},
					SupportedSyncModes:      NewSet[SyncMode]("incremental", "full_refresh"),
					SourceDefinedPrimaryKey: NewSet("id"),
					AvailableCursorFields:   NewSet("updated_at"),
					AdditionalProperties:    `{"replication_method": "incremental"}`,
					SyncMode:                "incremental",
				},
			},
			expected: &Catalog{
				Streams: []*ConfiguredStream{
					{
						Stream: &Stream{
							Name:      "users",
							Namespace: "public",
							Schema: &TypeSchema{
								Properties: sync.Map{},
							},
							SupportedSyncModes:      NewSet[SyncMode]("incremental", "full_refresh"),
							SourceDefinedPrimaryKey: NewSet("id"),
							AvailableCursorFields:   NewSet("updated_at"),
							AdditionalProperties:    `{"replication_method": "incremental"}`,
							SyncMode:                "incremental",
						},
					},
				},
				SelectedStreams: map[string][]StreamMetadata{
					"public": {
						{
							StreamName:     "users",
							PartitionRegex: "",
						},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := GetWrappedCatalog(tc.streams)

			// Verify the number of streams
			assert.Equal(t, len(tc.expected.Streams), len(result.Streams), "Number of streams should match")

			// Verify each stream
			for i, expectedStream := range tc.expected.Streams {
				assert.Equal(t, expectedStream.Stream.Name, result.Streams[i].Stream.Name, "Stream name should match")
				assert.Equal(t, expectedStream.Stream.Namespace, result.Streams[i].Stream.Namespace, "Stream namespace should match")
			}

			// Verify selected streams
			assert.Equal(t, len(tc.expected.SelectedStreams), len(result.SelectedStreams), "Number of namespaces should match")

			for namespace, expectedMetadata := range tc.expected.SelectedStreams {
				resultMetadata, exists := result.SelectedStreams[namespace]
				assert.True(t, exists, "Namespace should exist in result")
				assert.Equal(t, len(expectedMetadata), len(resultMetadata), "Number of streams in namespace should match")

				for i, expected := range expectedMetadata {
					assert.Equal(t, expected.StreamName, resultMetadata[i].StreamName, "Stream name in metadata should match")
					assert.Equal(t, expected.PartitionRegex, resultMetadata[i].PartitionRegex, "Partition regex should match")
				}
			}
		})
	}
}
