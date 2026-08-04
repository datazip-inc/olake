package indexdb

import (
	"context"
	"fmt"
	"testing"

	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPebbleStoreIsolatesAndDropsStreams(t *testing.T) {
	ctx := context.Background()
	store := NewPebbleStore(testOptions(t))
	defer func() { require.NoError(t, store.Close()) }()

	first, err := store.Open(ctx, "public.orders")
	require.NoError(t, err)
	second, err := store.Open(ctx, "public.customers")
	require.NoError(t, err)

	batch := types.NewRowIndexBatch(first)
	put(batch, "shared-key", "s3://bucket/orders.parquet", 1)
	applyAt(t, first, batch, 1)

	requireLocation(t, first, "shared-key", "s3://bucket/orders.parquet", 1)
	requireAbsent(t, second, "shared-key")

	// Reopening the same stream must hand back the live index, not a new one.
	again, err := store.Open(ctx, "public.orders")
	require.NoError(t, err)
	assert.Same(t, first, again)

	require.NoError(t, store.Drop(ctx, "public.orders"))
	rebuilt, err := store.Open(ctx, "public.orders")
	require.NoError(t, err)
	requireAbsent(t, rebuilt, "shared-key")
}

// The configured sizes describe one stream, so a stream's cache is its own and
// what one stream reads cannot evict what another is holding.
func TestPebbleStoreGivesEachStreamItsOwnCache(t *testing.T) {
	ctx := context.Background()
	store := NewPebbleStore(testOptions(t))
	defer func() { require.NoError(t, store.Close()) }()

	busy, err := store.Open(ctx, "public.orders")
	require.NoError(t, err)
	idle, err := store.Open(ctx, "public.customers")
	require.NoError(t, err)

	// Only one stream is written to and read back, so only one stream has any
	// reason to hold cached blocks.
	batch := types.NewRowIndexBatch(busy)
	for i := 0; i < 512; i++ {
		put(batch, fmt.Sprintf("key-%d", i), "s3://bucket/orders.parquet", int64(i))
	}
	applyAt(t, busy, batch, 1)

	// Blocks only enter a cache once they live in an sstable rather than in the
	// memtable, so the write has to be flushed before a read goes through one.
	require.NoError(t, busy.(*pebbleIndex).db.Flush())
	requireLocation(t, busy, "key-0", "s3://bucket/orders.parquet", 0)

	// The untouched stream reporting nothing is what shows the two databases are
	// not drawing on a single cache.
	assert.NotZero(t, busy.(*pebbleIndex).db.Metrics().BlockCache.Count)
	assert.Zero(t, idle.(*pebbleIndex).db.Metrics().BlockCache.Count)
}

func TestPebbleStoreRejectsUseAfterClose(t *testing.T) {
	ctx := context.Background()
	store := NewPebbleStore(testOptions(t))
	require.NoError(t, store.Close())

	_, err := store.Open(ctx, "public.orders")
	require.Error(t, err)

	// Closing twice must stay harmless, since cleanup paths may run more than once.
	require.NoError(t, store.Close())
}

func TestIndexDirNameSeparatesCollidingStreamIDs(t *testing.T) {
	first := indexDirName("public.orders")
	second := indexDirName("public/orders")

	assert.NotEqual(t, first, second, "stream ids that sanitize alike must not share a database")
	assert.Regexp(t, `^[a-zA-Z0-9_.-]+$`, first)
	assert.Regexp(t, `^[a-zA-Z0-9_.-]+$`, second)
}
