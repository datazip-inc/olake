package indexdb

import (
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testOptions(t *testing.T) Options {
	t.Helper()
	return Options{
		Dir:          t.TempDir(),
		CacheSize:    8 * mib,
		MemTableSize: 2 * mib,
		MaxOpenFiles: 64,
	}
}

func openTestIndex(t *testing.T, opts Options) *pebbleIndex {
	t.Helper()
	index, err := openIndex(opts.Dir+"/stream", opts)
	require.NoError(t, err)
	return index
}

// countPrefix reports how many keys the index holds in one key family.
func countPrefix(t *testing.T, index *pebbleIndex, prefix byte) int {
	t.Helper()

	lower := []byte{prefix}
	iter, err := index.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: prefixEnd(lower)})
	require.NoError(t, err)
	defer func() { require.NoError(t, iter.Close()) }()

	count := 0
	for ok := iter.First(); ok; ok = iter.Next() {
		count++
	}
	return count
}

func put(b *types.RowIndexBatch, key, path string, position int64) {
	b.Put(key, types.RowLocation{FilePath: path, Position: position})
}

// applyAt commits a batch and moves the checkpoint, which is what a writer
// thread does once its destination commit has succeeded.
func applyAt(t *testing.T, index types.TableIndex, batch *types.RowIndexBatch, snapshotID int64) {
	t.Helper()
	require.NoError(t, index.Apply(batch, &snapshotID))
}

func requireLocation(t *testing.T, index types.TableIndex, key, path string, position int64) {
	t.Helper()
	loc, found, err := index.Lookup(key)
	require.NoError(t, err)
	require.True(t, found, "expected key[%s] to be indexed", key)
	assert.Equal(t, types.RowLocation{FilePath: path, Position: position}, loc)
}

func requireAbsent(t *testing.T, index types.TableIndex, key string) {
	t.Helper()
	_, found, err := index.Lookup(key)
	require.NoError(t, err)
	assert.False(t, found, "expected key[%s] to be absent", key)
}
