package indexdb

import (
	"context"
	"fmt"
	"sync"
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

func put(t *testing.T, txn types.IndexTxn, key, path string, position int64) {
	t.Helper()
	require.NoError(t, txn.Put(key, types.RowLocation{FilePath: path, Position: position}))
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

func TestPebbleIndexCommitPersistsAcrossReopen(t *testing.T) {
	opts := testOptions(t)

	index := openTestIndex(t, opts)
	txn, err := index.NewTxn()
	require.NoError(t, err)

	put(t, txn, "a", "s3://bucket/data-1.parquet", 0)
	put(t, txn, "b", "s3://bucket/data-1.parquet", 7)
	put(t, txn, "c", "s3://bucket/data-2.parquet", 3)
	require.NoError(t, txn.Commit())
	require.NoError(t, index.SetIndexedSnapshot(42))
	require.NoError(t, index.Close())

	reopened := openTestIndex(t, opts)
	defer func() { require.NoError(t, reopened.Close()) }()

	requireLocation(t, reopened, "a", "s3://bucket/data-1.parquet", 0)
	requireLocation(t, reopened, "b", "s3://bucket/data-1.parquet", 7)
	requireLocation(t, reopened, "c", "s3://bucket/data-2.parquet", 3)
	requireAbsent(t, reopened, "missing")

	snapshotID, ok, err := reopened.IndexedSnapshot()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, int64(42), snapshotID)
}

func TestPebbleIndexNoSnapshotBeforeBootstrap(t *testing.T) {
	index := openTestIndex(t, testOptions(t))
	defer func() { require.NoError(t, index.Close()) }()

	_, ok, err := index.IndexedSnapshot()
	require.NoError(t, err)
	assert.False(t, ok, "a fresh index must report no snapshot so callers know to bootstrap")
}

func TestPebbleIndexTxnLookupSeesOwnWrites(t *testing.T) {
	index := openTestIndex(t, testOptions(t))
	defer func() { require.NoError(t, index.Close()) }()

	txn, err := index.NewTxn()
	require.NoError(t, err)
	put(t, txn, "a", "s3://bucket/data-1.parquet", 11)

	loc, found, err := txn.Lookup("a")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, types.RowLocation{FilePath: "s3://bucket/data-1.parquet", Position: 11}, loc)
	require.NoError(t, txn.Commit())
}

func TestPebbleIndexRollbackRestoresPreTxnState(t *testing.T) {
	index := openTestIndex(t, testOptions(t))
	defer func() { require.NoError(t, index.Close()) }()

	committed, err := index.NewTxn()
	require.NoError(t, err)
	put(t, committed, "kept", "s3://bucket/data-1.parquet", 1)
	put(t, committed, "moved", "s3://bucket/data-1.parquet", 2)
	put(t, committed, "removed", "s3://bucket/data-1.parquet", 3)
	require.NoError(t, committed.Commit())

	rolled, err := index.NewTxn()
	require.NoError(t, err)
	put(t, rolled, "moved", "s3://bucket/data-2.parquet", 9)
	require.NoError(t, rolled.Delete("removed"))
	put(t, rolled, "added", "s3://bucket/data-2.parquet", 10)
	require.NoError(t, rolled.Rollback())

	requireLocation(t, index, "kept", "s3://bucket/data-1.parquet", 1)
	requireLocation(t, index, "moved", "s3://bucket/data-1.parquet", 2)
	requireLocation(t, index, "removed", "s3://bucket/data-1.parquet", 3)
	requireAbsent(t, index, "added")
}

// A key mutated several times inside one txn must roll back to the value the txn
// started from, not to an intermediate one.
func TestPebbleIndexRollbackIgnoresIntermediateWrites(t *testing.T) {
	index := openTestIndex(t, testOptions(t))
	defer func() { require.NoError(t, index.Close()) }()

	seed, err := index.NewTxn()
	require.NoError(t, err)
	put(t, seed, "a", "s3://bucket/data-0.parquet", 5)
	require.NoError(t, seed.Commit())

	rolled, err := index.NewTxn()
	require.NoError(t, err)
	put(t, rolled, "a", "s3://bucket/data-1.parquet", 6)
	put(t, rolled, "a", "s3://bucket/data-2.parquet", 7)
	require.NoError(t, rolled.Delete("a"))
	require.NoError(t, rolled.Rollback())

	requireLocation(t, index, "a", "s3://bucket/data-0.parquet", 5)
}

// An index closed while a txn was still open must roll that txn back at the next
// open, so it never points at data files that were never committed.
func TestPebbleIndexRecoversInterruptedTxnOnOpen(t *testing.T) {
	opts := testOptions(t)
	index := openTestIndex(t, opts)

	seed, err := index.NewTxn()
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		put(t, seed, fmt.Sprintf("key-%d", i), "s3://bucket/committed.parquet", int64(i))
	}
	require.NoError(t, seed.Commit())

	// Exceed the in-memory batch threshold so the abandoned writes really do
	// reach pebble and recovery has something to undo.
	abandoned, err := index.NewTxn()
	require.NoError(t, err)
	for i := 0; i < batchEntries+128; i++ {
		put(t, abandoned, fmt.Sprintf("key-%d", i), "s3://bucket/abandoned.parquet", int64(i)+1000)
	}
	require.NoError(t, index.Close())

	recovered := openTestIndex(t, opts)
	defer func() { require.NoError(t, recovered.Close()) }()

	for i := 0; i < 3; i++ {
		requireLocation(t, recovered, fmt.Sprintf("key-%d", i), "s3://bucket/committed.parquet", int64(i))
	}
	for i := 3; i < batchEntries+128; i++ {
		requireAbsent(t, recovered, fmt.Sprintf("key-%d", i))
	}
}

func TestPebbleIndexRejectsUseAfterCompletion(t *testing.T) {
	index := openTestIndex(t, testOptions(t))
	defer func() { require.NoError(t, index.Close()) }()

	txn, err := index.NewTxn()
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	assert.ErrorIs(t, txn.Put("a", types.RowLocation{FilePath: "s3://bucket/f.parquet"}), errTxnDone)
	assert.ErrorIs(t, txn.Delete("a"), errTxnDone)
	assert.ErrorIs(t, txn.Commit(), errTxnDone)
	assert.ErrorIs(t, txn.Rollback(), errTxnDone)
}

func TestPebbleIndexRejectsNegativePosition(t *testing.T) {
	index := openTestIndex(t, testOptions(t))
	defer func() { require.NoError(t, index.Close()) }()

	txn, err := index.NewTxn()
	require.NoError(t, err)
	require.Error(t, txn.Put("a", types.RowLocation{FilePath: "s3://bucket/f.parquet", Position: -1}))
	require.NoError(t, txn.Rollback())
}

func TestPebbleIndexTruncateClearsEverything(t *testing.T) {
	index := openTestIndex(t, testOptions(t))
	defer func() { require.NoError(t, index.Close()) }()

	txn, err := index.NewTxn()
	require.NoError(t, err)
	put(t, txn, "a", "s3://bucket/data-1.parquet", 1)
	require.NoError(t, txn.Commit())
	require.NoError(t, index.SetIndexedSnapshot(7))

	require.NoError(t, index.Truncate())

	requireAbsent(t, index, "a")
	_, ok, err := index.IndexedSnapshot()
	require.NoError(t, err)
	assert.False(t, ok)

	// The interning dictionary was cleared too, so ids restart without stranding
	// row values that point at them.
	refilled, err := index.NewTxn()
	require.NoError(t, err)
	put(t, refilled, "a", "s3://bucket/data-9.parquet", 4)
	require.NoError(t, refilled.Commit())
	requireLocation(t, index, "a", "s3://bucket/data-9.parquet", 4)
}

func TestPebbleIndexConcurrentPutsAcrossDisjointKeys(t *testing.T) {
	index := openTestIndex(t, testOptions(t))
	defer func() { require.NoError(t, index.Close()) }()

	const threads, perThread = 8, 256

	var wg sync.WaitGroup
	errs := make([]error, threads)
	for thread := 0; thread < threads; thread++ {
		wg.Add(1)
		go func(thread int) {
			defer wg.Done()

			txn, err := index.NewTxn()
			if err != nil {
				errs[thread] = err
				return
			}
			for i := 0; i < perThread; i++ {
				loc := types.RowLocation{
					FilePath: fmt.Sprintf("s3://bucket/data-%d.parquet", thread),
					Position: int64(i),
				}
				if err := txn.Put(fmt.Sprintf("t%d-k%d", thread, i), loc); err != nil {
					errs[thread] = err
					return
				}
			}
			errs[thread] = txn.Commit()
		}(thread)
	}
	wg.Wait()

	for thread, err := range errs {
		require.NoError(t, err, "thread %d", thread)
	}
	for thread := 0; thread < threads; thread++ {
		for i := 0; i < perThread; i++ {
			requireLocation(t, index,
				fmt.Sprintf("t%d-k%d", thread, i),
				fmt.Sprintf("s3://bucket/data-%d.parquet", thread),
				int64(i))
		}
	}
}

func TestPebbleStoreIsolatesAndDropsStreams(t *testing.T) {
	ctx := context.Background()
	store := NewPebbleStore(testOptions(t))
	defer func() { require.NoError(t, store.Close()) }()

	first, err := store.Open(ctx, "public.orders")
	require.NoError(t, err)
	second, err := store.Open(ctx, "public.customers")
	require.NoError(t, err)

	txn, err := first.NewTxn()
	require.NoError(t, err)
	put(t, txn, "shared-key", "s3://bucket/orders.parquet", 1)
	require.NoError(t, txn.Commit())

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

func TestIndexDirNameSeparatesCollidingStreamIDs(t *testing.T) {
	first := indexDirName("public.orders")
	second := indexDirName("public/orders")

	assert.NotEqual(t, first, second, "stream ids that sanitize alike must not share a database")
	assert.Regexp(t, `^[a-zA-Z0-9_.-]+$`, first)
	assert.Regexp(t, `^[a-zA-Z0-9_.-]+$`, second)
}

func TestPrefixEnd(t *testing.T) {
	assert.Equal(t, []byte{0x02}, prefixEnd([]byte{0x01}))
	assert.Equal(t, []byte{0x01, 0x03}, prefixEnd([]byte{0x01, 0x02}))
	assert.Equal(t, []byte{0x02}, prefixEnd([]byte{0x01, 0xff}))
	assert.Nil(t, prefixEnd([]byte{0xff, 0xff}))
}

func TestEncodeDecodeFileValueRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		path      string
		seqNumber int64
	}{
		{"s3://bucket/data-1.parquet", 0},
		{"s3://bucket/data-2.parquet", 1},
		{"", 1 << 40},
		{"a", -1},
	} {
		path, seqNumber, err := decodeFileValue(encodeFileValue(tc.path, tc.seqNumber))
		require.NoError(t, err)
		assert.Equal(t, tc.path, path)
		assert.Equal(t, tc.seqNumber, seqNumber)
	}

	// A path without the sequence number prefix is not silently readable as one.
	_, _, err := decodeFileValue([]byte("short"))
	require.Error(t, err)
}

func TestPebbleIndexRecordsFileSequenceNumber(t *testing.T) {
	index := openTestIndex(t, testOptions(t))
	defer func() { require.NoError(t, index.Close()) }()

	txn, err := index.NewTxn()
	require.NoError(t, err)
	require.NoError(t, txn.Put("a", types.RowLocation{
		FilePath:  "s3://bucket/data-1.parquet",
		Position:  4,
		SeqNumber: 9,
	}))
	require.NoError(t, txn.Commit())

	loc, found, err := index.Lookup("a")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, int64(9), loc.SeqNumber)
}

func TestPebbleIndexFillsInSequenceNumberLearnedLater(t *testing.T) {
	index := openTestIndex(t, testOptions(t))
	defer func() { require.NoError(t, index.Close()) }()

	const path = "s3://bucket/data-1.parquet"

	// A row indexed while its file is being written has no sequence number yet.
	txn, err := index.NewTxn()
	require.NoError(t, err)
	require.NoError(t, txn.Put("a", types.RowLocation{FilePath: path, Position: 0}))
	require.NoError(t, txn.Commit())

	loc, _, err := index.Lookup("a")
	require.NoError(t, err)
	require.Equal(t, types.UnknownSeqNumber, loc.SeqNumber)
	assert.False(t, loc.PrecedesDelete(100), "a row of an uncommitted file predates no delete")

	// A later scan reads the committed number off the table, which reaches every
	// row of that file because they share one dictionary entry.
	txn, err = index.NewTxn()
	require.NoError(t, err)
	require.NoError(t, txn.Put("b", types.RowLocation{FilePath: path, Position: 1, SeqNumber: 7}))
	require.NoError(t, txn.Commit())

	for _, key := range []string{"a", "b"} {
		loc, found, err := index.Lookup(key)
		require.NoError(t, err)
		require.True(t, found, "key[%s]", key)
		assert.Equal(t, int64(7), loc.SeqNumber, "key[%s]", key)
		assert.True(t, loc.PrecedesDelete(7), "key[%s]", key)
		assert.False(t, loc.PrecedesDelete(6), "key[%s]", key)
	}
}

func TestPebbleIndexDiscardsAnIndexOfAnotherFormat(t *testing.T) {
	opts := testOptions(t)

	index := openTestIndex(t, opts)
	txn, err := index.NewTxn()
	require.NoError(t, err)
	put(t, txn, "a", "s3://bucket/data-1.parquet", 0)
	require.NoError(t, txn.Commit())
	require.NoError(t, index.SetIndexedSnapshot(42))

	// Stamp a foreign version to stand in for an index written by other code.
	batch := index.db.NewBatch()
	require.NoError(t, setCounter(batch, metaFormatVersion, formatVersion+1))
	require.NoError(t, batch.Commit(pebble.Sync))
	require.NoError(t, batch.Close())
	require.NoError(t, index.Close())

	reopened := openTestIndex(t, opts)
	defer func() { require.NoError(t, reopened.Close()) }()

	requireAbsent(t, reopened, "a")
	_, indexed, err := reopened.IndexedSnapshot()
	require.NoError(t, err)
	assert.False(t, indexed, "a discarded index must ask to be rebuilt")
}

func TestEncodeDecodeRowRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		fileID   uint64
		position uint64
		want     int64
	}{{0, 0, 0}, {1, 127, 127}, {128, 128, 128}, {1 << 20, 1 << 40, 1 << 40}} {
		fileID, position, err := decodeRow(encodeRow(tc.fileID, tc.position))
		require.NoError(t, err)
		assert.Equal(t, tc.fileID, fileID)
		assert.Equal(t, tc.want, position)
	}
}
