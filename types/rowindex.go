package types

import "context"

// RowLocation addresses a single row within one data file of a destination table.
type RowLocation struct {
	FilePath string
	Position int64
}

// TableIndexStore hands out one TableIndex per stream. Each stream is backed by
// its own database so that a rebuild, drop, or corruption of one stream's index
// cannot affect another's.
type TableIndexStore interface {
	// Open returns the index for streamID, creating an empty one on first use.
	// Repeated calls for the same streamID return the same TableIndex.
	Open(ctx context.Context, streamID string) (TableIndex, error)
	// Drop discards streamID's index entirely, forcing a rebuild on next Open.
	Drop(ctx context.Context, streamID string) error
	// Close releases every open index.
	Close() error
}

// TableIndex is a durable map from a row identifier (_olake_id) to the location
// of that row in the destination table, plus the snapshot the map is consistent
// with. Implementations must be safe for concurrent use.
//
// The index carries no transactions and no undo log. Everything it holds is a
// fact re-derivable by scanning the destination table, so instead of unwinding a
// failed write the index simply refuses to advance its checkpoint: a checkpoint
// behind the table's snapshot tells the next sync to rescan the difference, and
// replaying a scan produces the same entries it produced the first time.
//
// The rule that keeps that reasoning valid is that a row location must never
// reach the index before the destination commit that created the file it points
// into. Writers therefore buffer their changes in a RowIndexBatch and hand the
// whole batch over once the destination has accepted the files.
type TableIndex interface {
	// Lookup returns the indexed location of key. found is false when the index
	// holds no live row for key.
	Lookup(key string) (loc RowLocation, found bool, err error)
	// Apply installs every change in batch. A non-nil snapshotID additionally
	// advances the checkpoint in the same durable commit as those changes, so
	// the index never claims to be current at a snapshot whose rows it is
	// missing. A nil snapshotID leaves the checkpoint where it was; an
	// interrupted Apply is repaired by the next sync rescanning from the older
	// checkpoint.
	//
	// Callers must only pass a snapshotID once the destination has committed the
	// files the batch refers to.
	Apply(batch *RowIndexBatch, snapshotID *int64) error
	// IndexedSnapshot returns the destination snapshot ID this index reflects.
	// ok is false while the index has never been populated, which is the signal
	// that a full bootstrap scan is required.
	IndexedSnapshot() (snapshotID int64, ok bool, err error)
	// Truncate removes all entries and clears the indexed snapshot. Clearing the
	// checkpoint is what makes it safe to start a from-scratch rebuild without a
	// rollback path: a rebuild that dies partway leaves no checkpoint, and an
	// index with no checkpoint is rebuilt again rather than trusted.
	Truncate() error
	Close() error
}

// rowIndexChange is one pending mutation. A change with deleted set removes the
// row; the location is meaningless in that case.
type rowIndexChange struct {
	loc     RowLocation
	deleted bool
}

// RowIndexBatch buffers the index changes of a single writer thread until the
// destination commit that makes them real, and answers that thread's lookups
// from the buffer before falling through to the committed index.
//
// Buffering rather than writing through is what removes the need for an undo
// log: a thread that dies, or whose destination commit fails, has written
// nothing to the index at all, so there is nothing to unwind.
//
// A batch is not safe for concurrent use. Each writer thread owns one, which
// also means threads no longer observe one another's uncommitted locations.
type RowIndexBatch struct {
	index   TableIndex
	changes map[string]rowIndexChange
}

// NewRowIndexBatch returns an empty batch that resolves misses against index.
func NewRowIndexBatch(index TableIndex) *RowIndexBatch {
	return &RowIndexBatch{index: index, changes: make(map[string]rowIndexChange)}
}

// Put records key as living at loc once this batch is applied.
func (b *RowIndexBatch) Put(key string, loc RowLocation) {
	b.changes[key] = rowIndexChange{loc: loc}
}

// Delete marks key as no longer live once this batch is applied.
func (b *RowIndexBatch) Delete(key string) {
	b.changes[key] = rowIndexChange{deleted: true}
}

// Lookup resolves key against this batch's pending changes first, so a row
// written moments ago by the same thread resolves to where it is about to land
// rather than to the stale location the last sync committed.
func (b *RowIndexBatch) Lookup(key string) (RowLocation, bool, error) {
	if change, pending := b.changes[key]; pending {
		if change.deleted {
			return RowLocation{}, false, nil
		}
		return change.loc, true, nil
	}
	if b.index == nil {
		return RowLocation{}, false, nil
	}
	return b.index.Lookup(key)
}

// Len reports how many rows the batch will touch.
func (b *RowIndexBatch) Len() int {
	return len(b.changes)
}

// Range hands every pending change to fn, stopping at the first error.
func (b *RowIndexBatch) Range(fn func(key string, loc RowLocation, deleted bool) error) error {
	for key, change := range b.changes {
		if err := fn(key, change.loc, change.deleted); err != nil {
			return err
		}
	}
	return nil
}

// Reset empties the batch so the same buffer can serve the next commit cycle.
func (b *RowIndexBatch) Reset() {
	b.changes = make(map[string]rowIndexChange)
}
