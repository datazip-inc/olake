package protocol

import (
	"testing"

	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
)

func TestCatalogDefaultMode(t *testing.T) {
	// Create a catalog with DefaultMode set
	catalog := &types.Catalog{
		DefaultMode: types.FULLREFRESH,
		Streams: []*types.ConfiguredStream{
			{
				Stream: &types.Stream{
					Name:      "stream1",
					Namespace: "test",
					SyncMode:  types.CDC, // This shouldn't be overridden
				},
			},
			{
				Stream: &types.Stream{
					Name:      "stream2",
					Namespace: "test",
					SyncMode:  "", // This should be set to DefaultMode
				},
			},
		},
	}

	// Apply DefaultMode to streams without a specific sync mode
	for i := range catalog.Streams {
		if catalog.Streams[i].Stream.SyncMode == "" {
			catalog.Streams[i].Stream.SyncMode = catalog.DefaultMode
		}
	}

	// Check that stream1 kept its original sync mode
	assert.Equal(t, types.CDC, catalog.Streams[0].Stream.SyncMode, "Stream1 should keep its CDC sync mode")

	// Check that stream2 got the default sync mode
	assert.Equal(t, types.FULLREFRESH, catalog.Streams[1].Stream.SyncMode, "Stream2 should get the default sync mode")
} 