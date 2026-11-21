package destination

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
	fetcher := testValueFetcher(t, true, &testRowReader{})
	marker := testRecord("0", "u", map[string]any{"id": 0, "payload": placeholder}, "payload")

	// more than this thread's share once another thread caches, so dropping it alone brings the
	// thread under half its share
	require.NoError(t, testRecoverAll(fetcher, []types.RawRecord{
		marker, testRecord("old", "c", map[string]any{"payload": strings.Repeat("x", int(maxCachedBytes/2)+1)}),
	}))
	require.NoError(t, testRecoverAll(fetcher, []types.RawRecord{
		marker, testRecord("new", "c", map[string]any{"payload": "recent"}),
	}))

	// another thread starts caching, which halves this thread's share
	cachingThreads.Add(1)
	t.Cleanup(func() { cachingThreads.Add(-1) })
	fetcher.keepCachedValuesWithinBudget()

	assert.NotContains(t, fetcher.cachedValues.rows, "old")
	assert.Contains(t, fetcher.cachedValues.rows, "new")
	assert.LessOrEqual(t, fetcher.cachedValues.bytes, maxCachedBytes/4)
}

// Over its share, trim goes down to half of it, not just under it, so the next batches do not trim again at once.
func TestTrimDropsToHalfTheShare(t *testing.T) {
	cache := newCachedValues()
	columns := map[string]struct{}{"payload": {}}
	// each row counts 1000 bytes: a 1 byte id, the row and value overheads (384 + 24) and 591 bytes of value
	for _, olakeID := range []string{"1", "2", "3", "4", "5"} {
		cache.remember(olakeID, map[string]any{"payload": strings.Repeat("x", 591)}, columns)
	}
	require.Equal(t, int64(5000), cache.bytes)

	assert.Zero(t, cache.trim(5000), "at its share nothing is dropped")
	assert.Equal(t, 3, cache.trim(4000), "5000 bytes over a 4000 share goes down to 2000")
	assert.Equal(t, int64(2000), cache.bytes)
	assert.Contains(t, cache.rows, "4")
	assert.Contains(t, cache.rows, "5")
}

// bytes must follow the rows exactly, else trim drops too much or too little without anyone noticing.
func TestCachedValuesBytesFollowTheRows(t *testing.T) {
	cache := newCachedValues()
	columns := map[string]struct{}{"payload": {}}
	steps := []struct {
		name  string
		apply func()
	}{
		{name: "a row is remembered", apply: func() { cache.remember("1", map[string]any{"payload": "abc"}, columns) }},
		{name: "the row is remembered with a longer value", apply: func() { cache.remember("1", map[string]any{"payload": "abcdef"}, columns) }},
		{name: "the row is deleted", apply: func() { cache.markDeleted("1") }},
		{name: "the deleted row is remembered again", apply: func() { cache.remember("1", map[string]any{"payload": "x"}, columns) }},
		{name: "a row never remembered is deleted", apply: func() { cache.markDeleted("2") }},
	}

	for _, step := range steps {
		step.apply()

		expected := int64(0)
		for olakeID, row := range cache.rows {
			expected += row.size(olakeID)
		}
		assert.Equal(t, expected, cache.bytes, step.name)
	}
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
