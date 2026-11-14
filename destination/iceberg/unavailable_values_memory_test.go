package iceberg

import (
	"strings"
	"testing"

	"github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datazip-inc/olake/types"
)

// Over its share, rows written in earlier batches go first: they sit in closed files, while a
// recent row may sit in a file still open that a read would have to close early.
func TestTrimWriteCacheDropsLeastRecentRowsFirst(t *testing.T) {
	resolver := testResolver(t, true, &testWriter{locations: map[string]types.RowLocation{}}, &testTableIndexClient{table: testTable{}})
	marker := testRecord("0", "u", map[string]any{"id": 0, "payload": placeholder}, "payload")

	// more than this thread's share once another thread caches, so dropping it alone brings the
	// thread under half its share
	require.NoError(t, testRecoverAll(resolver, []types.RawRecord{
		marker, testRecord("old", "c", map[string]any{"payload": strings.Repeat("x", int(maxCachedBytes/2)+1)}),
	}))
	require.NoError(t, testRecoverAll(resolver, []types.RawRecord{
		marker, testRecord("new", "c", map[string]any{"payload": "recent"}),
	}))

	// another thread starts caching, which halves this thread's share
	cachingThreads.Add(1)
	t.Cleanup(func() { cachingThreads.Add(-1) })
	resolver.keepCachedValuesWithinBudget()

	assert.NotContains(t, resolver.cachedValues.rows, "old")
	assert.Contains(t, resolver.cachedValues.rows, "new")
	assert.LessOrEqual(t, resolver.cachedValues.bytes, maxCachedBytes/4)
}

func TestValueSize(t *testing.T) {
	testCases := []struct {
		name     string
		value    any
		expected int64
	}{
		{name: "NULL", value: nil, expected: 0},
		{name: "text", value: "abc", expected: 3},
		{name: "raw JSON read back without normalization", value: json.RawMessage(`{"k":1}`), expected: 7},
		{name: "array, by its elements", value: []any{"abc", "de"}, expected: 5},
		{name: "number", value: 1.5, expected: 16},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, valueSize(tc.value))
		})
	}
}
