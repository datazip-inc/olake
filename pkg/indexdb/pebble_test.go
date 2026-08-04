package indexdb

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// retiredUndoPrefix is the key family the undo log used to occupy. Nothing may
// write there any more; the guard below is what keeps it that way.
const retiredUndoPrefix byte = 0x04

func TestPebbleIndexApplyPersistsAcrossReopen(t *testing.T) {
	opts := testOptions(t)

	index := openTestIndex(t, opts)
	batch := types.NewRowIndexBatch(index)
	put(batch, "a", "s3://bucket/data-1.parquet", 0)
	put(batch, "b", "s3://bucket/data-1.parquet", 7)
	put(batch, "c", "s3://bucket/data-2.parquet", 3)
	applyAt(t, index, batch, 42)
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

// This is the property that replaces the undo log: a thread whose destination
// commit never happens simply never applies its batch, so there is nothing on
// disk to unwind.
func TestPebbleIndexUnappliedBatchLeavesIndexUntouched(t *testing.T) {
	opts := testOptions(t)
	index := openTestIndex(t, opts)

	committed := types.NewRowIndexBatch(index)
	put(committed, "kept", "s3://bucket/data-1.parquet", 1)
	put(committed, "moved", "s3://bucket/data-1.parquet", 2)
	put(committed, "removed", "s3://bucket/data-1.parquet", 3)
	applyAt(t, index, committed, 5)

	// A thread that writes a great deal and then dies before its Iceberg commit.
	abandoned := types.NewRowIndexBatch(index)
	for i := 0; i < batchEntries+128; i++ {
		put(abandoned, fmt.Sprintf("key-%d", i), "s3://bucket/abandoned.parquet", int64(i))
	}
	abandoned.Put("moved", types.RowLocation{FilePath: "s3://bucket/abandoned.parquet", Position: 99})
	abandoned.Delete("removed")
	require.NoError(t, index.Close())

	recovered := openTestIndex(t, opts)
	defer func() { require.NoError(t, recovered.Close()) }()

	requireLocation(t, recovered, "kept", "s3://bucket/data-1.parquet", 1)
	requireLocation(t, recovered, "moved", "s3://bucket/data-1.parquet", 2)
	requireLocation(t, recovered, "removed", "s3://bucket/data-1.parquet", 3)
	for i := 0; i < batchEntries+128; i += 512 {
		requireAbsent(t, recovered, fmt.Sprintf("key-%d", i))
	}

	snapshotID, ok, err := recovered.IndexedSnapshot()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, int64(5), snapshotID)
}

func TestRowIndexBatchLookupSeesOwnWritesBeforeIndex(t *testing.T) {
	index := openTestIndex(t, testOptions(t))
	defer func() { require.NoError(t, index.Close()) }()

	seed := types.NewRowIndexBatch(index)
	put(seed, "stale", "s3://bucket/data-0.parquet", 4)
	put(seed, "doomed", "s3://bucket/data-0.parquet", 5)
	applyAt(t, index, seed, 1)

	batch := types.NewRowIndexBatch(index)
	put(batch, "fresh", "s3://bucket/data-1.parquet", 11)
	put(batch, "stale", "s3://bucket/data-1.parquet", 12)
	batch.Delete("doomed")

	// A row this thread just wrote resolves to where it is about to land.
	loc, found, err := batch.Lookup("fresh")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, types.RowLocation{FilePath: "s3://bucket/data-1.parquet", Position: 11}, loc)

	// A pending put shadows the committed location it will replace.
	loc, found, err = batch.Lookup("stale")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, types.RowLocation{FilePath: "s3://bucket/data-1.parquet", Position: 12}, loc)

	// A pending delete hides a row that is still committed.
	_, found, err = batch.Lookup("doomed")
	require.NoError(t, err)
	assert.False(t, found)

	// Untouched keys fall through to the committed index.
	_, found, err = batch.Lookup("absent")
	require.NoError(t, err)
	assert.False(t, found)

	// None of it has reached the index until Apply runs.
	requireLocation(t, index, "stale", "s3://bucket/data-0.parquet", 4)
	requireLocation(t, index, "doomed", "s3://bucket/data-0.parquet", 5)
	requireAbsent(t, index, "fresh")
}

func TestPebbleIndexApplyDeletesRemoveCommittedRows(t *testing.T) {
	index := openTestIndex(t, testOptions(t))
	defer func() { require.NoError(t, index.Close()) }()

	seed := types.NewRowIndexBatch(index)
	put(seed, "a", "s3://bucket/data-1.parquet", 1)
	put(seed, "b", "s3://bucket/data-1.parquet", 2)
	applyAt(t, index, seed, 1)

	removal := types.NewRowIndexBatch(index)
	removal.Delete("a")
	applyAt(t, index, removal, 2)

	requireAbsent(t, index, "a")
	requireLocation(t, index, "b", "s3://bucket/data-1.parquet", 2)
}

// A batch far larger than one pebble write batch has to be handed over in
// pieces, and every piece has to survive.
func TestPebbleIndexApplyChunksLargeBatches(t *testing.T) {
	opts := testOptions(t)
	index := openTestIndex(t, opts)

	batch := types.NewRowIndexBatch(index)
	const entries = batchEntries*3 + 17
	for i := 0; i < entries; i++ {
		put(batch, fmt.Sprintf("key-%d", i), "s3://bucket/data-1.parquet", int64(i))
	}
	applyAt(t, index, batch, 9)
	require.NoError(t, index.Close())

	reopened := openTestIndex(t, opts)
	defer func() { require.NoError(t, reopened.Close()) }()

	for i := 0; i < entries; i += 331 {
		requireLocation(t, reopened, fmt.Sprintf("key-%d", i), "s3://bucket/data-1.parquet", int64(i))
	}
	snapshotID, ok, err := reopened.IndexedSnapshot()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, int64(9), snapshotID)
}

// An Apply that lands rows but never reaches its checkpoint is what a crash
// partway through a large scan looks like. The index is then behind rather than
// wrong, and replaying the scan converges on the same state.
func TestPebbleIndexApplyWithoutSnapshotLeavesCheckpointBehind(t *testing.T) {
	opts := testOptions(t)
	index := openTestIndex(t, opts)

	seed := types.NewRowIndexBatch(index)
	put(seed, "a", "s3://bucket/data-0.parquet", 1)
	applyAt(t, index, seed, 3)

	partial := types.NewRowIndexBatch(index)
	put(partial, "b", "s3://bucket/data-1.parquet", 2)
	require.NoError(t, index.Apply(partial, nil))
	require.NoError(t, index.Close())

	recovered := openTestIndex(t, opts)
	defer func() { require.NoError(t, recovered.Close()) }()

	snapshotID, ok, err := recovered.IndexedSnapshot()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, int64(3), snapshotID, "an unfinished apply must not advance the checkpoint")

	// Replaying the same facts is harmless, which is what makes rescanning from
	// the older checkpoint a complete repair.
	replay := types.NewRowIndexBatch(recovered)
	put(replay, "b", "s3://bucket/data-1.parquet", 2)
	applyAt(t, recovered, replay, 4)

	requireLocation(t, recovered, "a", "s3://bucket/data-0.parquet", 1)
	requireLocation(t, recovered, "b", "s3://bucket/data-1.parquet", 2)
}

// The undo key family is retired. Writing there again would resurrect a whole
// class of recovery bugs the buffered design exists to avoid.
func TestPebbleIndexWritesNoUndoRecords(t *testing.T) {
	index := openTestIndex(t, testOptions(t))
	defer func() { require.NoError(t, index.Close()) }()

	seed := types.NewRowIndexBatch(index)
	for i := 0; i < batchEntries+128; i++ {
		put(seed, fmt.Sprintf("key-%d", i), "s3://bucket/data-1.parquet", int64(i))
	}
	applyAt(t, index, seed, 1)

	overwrite := types.NewRowIndexBatch(index)
	for i := 0; i < batchEntries+128; i++ {
		put(overwrite, fmt.Sprintf("key-%d", i), "s3://bucket/data-2.parquet", int64(i)+1000)
	}
	overwrite.Delete("key-0")
	applyAt(t, index, overwrite, 2)

	assert.Zero(t, countPrefix(t, index, retiredUndoPrefix))
}

func TestPebbleIndexRejectsNegativePosition(t *testing.T) {
	index := openTestIndex(t, testOptions(t))
	defer func() { require.NoError(t, index.Close()) }()

	batch := types.NewRowIndexBatch(index)
	put(batch, "a", "s3://bucket/f.parquet", -1)
	require.Error(t, index.Apply(batch, nil))
}

func TestPebbleIndexTruncateClearsEverything(t *testing.T) {
	index := openTestIndex(t, testOptions(t))
	defer func() { require.NoError(t, index.Close()) }()

	batch := types.NewRowIndexBatch(index)
	put(batch, "a", "s3://bucket/data-1.parquet", 1)
	applyAt(t, index, batch, 7)

	require.NoError(t, index.Truncate())

	requireAbsent(t, index, "a")
	_, ok, err := index.IndexedSnapshot()
	require.NoError(t, err)
	assert.False(t, ok, "an emptied index must ask to be rebuilt rather than be trusted")

	// The interning dictionary was cleared too, so ids restart without stranding
	// row values that point at them.
	refilled := types.NewRowIndexBatch(index)
	put(refilled, "a", "s3://bucket/data-9.parquet", 4)
	applyAt(t, index, refilled, 8)
	requireLocation(t, index, "a", "s3://bucket/data-9.parquet", 4)
}

func TestPebbleIndexConcurrentApplyAcrossDisjointKeys(t *testing.T) {
	index := openTestIndex(t, testOptions(t))
	defer func() { require.NoError(t, index.Close()) }()

	const threads, perThread = 8, 256

	var wg sync.WaitGroup
	errs := make([]error, threads)
	for thread := 0; thread < threads; thread++ {
		wg.Add(1)
		go func(thread int) {
			defer wg.Done()

			batch := types.NewRowIndexBatch(index)
			for i := 0; i < perThread; i++ {
				put(batch, fmt.Sprintf("t%d-k%d", thread, i),
					fmt.Sprintf("s3://bucket/data-%d.parquet", thread), int64(i))
			}
			snapshotID := int64(thread)
			errs[thread] = index.Apply(batch, &snapshotID)
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

// A dictionary entry is the path verbatim, so nothing about a path may be
// assumed: an empty one, one carrying bytes that are not valid UTF-8, and one
// long enough to span a block all have to survive the round trip.
func TestPebbleIndexRoundTripsArbitraryFilePaths(t *testing.T) {
	index := openTestIndex(t, testOptions(t))
	defer func() { require.NoError(t, index.Close()) }()

	paths := []string{
		"s3://bucket/data-1.parquet",
		"",
		"s3://bucket/prefix with spaces/\x00\xff\xfe.parquet",
		"s3://bucket/" + strings.Repeat("deep/", 200) + "data.parquet",
	}

	batch := types.NewRowIndexBatch(index)
	for i, path := range paths {
		put(batch, fmt.Sprintf("key-%d", i), path, int64(i))
	}
	applyAt(t, index, batch, 1)

	for i, path := range paths {
		requireLocation(t, index, fmt.Sprintf("key-%d", i), path, int64(i))
	}
}

// Pebble builds no bloom filters unless the options ask for them, and a missing
// filter is invisible except as slower lookups. Backfill lookups are
// overwhelmingly misses, so this guards the case that matters most.
func TestPebbleIndexFiltersOutAbsentKeys(t *testing.T) {
	index := openTestIndex(t, testOptions(t))
	defer func() { require.NoError(t, index.Close()) }()

	batch := types.NewRowIndexBatch(index)
	for i := 0; i < 2048; i++ {
		put(batch, fmt.Sprintf("present-%d", i), "s3://bucket/data-1.parquet", int64(i))
	}
	applyAt(t, index, batch, 1)

	// Filters live in sstables, so the rows have to leave the memtable before a
	// lookup can consult one.
	require.NoError(t, index.db.Flush())

	for i := 0; i < 2048; i++ {
		requireAbsent(t, index, fmt.Sprintf("absent-%d", i))
	}

	// A hit is the filter answering "not in this table" without the data block
	// being read, which is exactly what an unfiltered index cannot do.
	assert.NotZero(t, index.db.Metrics().Filter.Hits, "lookups are not consulting a bloom filter")
}

func TestPebbleIndexInternsEachFilePathOnce(t *testing.T) {
	index := openTestIndex(t, testOptions(t))
	defer func() { require.NoError(t, index.Close()) }()

	const path = "s3://bucket/data-1.parquet"

	first, err := index.fileID(path)
	require.NoError(t, err)

	// Re-interning a known path must reuse its id rather than burn another one;
	// a table's rows share a handful of files and pay two bytes each for it.
	second, err := index.fileID(path)
	require.NoError(t, err)
	assert.Equal(t, first, second)
	assert.Equal(t, uint64(1), index.nextFileID)

	other, err := index.fileID("s3://bucket/data-2.parquet")
	require.NoError(t, err)
	assert.NotEqual(t, first, other)
	assert.Equal(t, uint64(2), index.nextFileID)
}

func TestPebbleIndexDiscardsAnIndexOfAnotherFormat(t *testing.T) {
	opts := testOptions(t)

	index := openTestIndex(t, opts)
	batch := types.NewRowIndexBatch(index)
	put(batch, "a", "s3://bucket/data-1.parquet", 0)
	applyAt(t, index, batch, 42)

	// Stamp a foreign version to stand in for an index written by other code.
	stamp := index.db.NewBatch()
	require.NoError(t, setCounter(stamp, metaFormatVersion, formatVersion+1))
	require.NoError(t, stamp.Commit(pebble.Sync))
	require.NoError(t, stamp.Close())
	require.NoError(t, index.Close())

	reopened := openTestIndex(t, opts)
	defer func() { require.NoError(t, reopened.Close()) }()

	requireAbsent(t, reopened, "a")
	_, indexed, err := reopened.IndexedSnapshot()
	require.NoError(t, err)
	assert.False(t, indexed, "a discarded index must ask to be rebuilt")
}
