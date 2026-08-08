package types

// RowLocation addresses a single row within one data file of a destination table.
type RowLocation struct {
	FilePath string
	Position int64
}

// tableIndex is interface for index store
type TableIndex interface {
	// lookup for keys in index store
	Lookup(key string) (loc RowLocation, found bool, err error)
	// commit changes to index store
	Commit(batch *RowIndexBatch, snapshotID *int64) error
	// LastCommittedSnapshot returns the last committed snapshot ID
	LastCommittedSnapshot() (snapshotID int64, ok bool, err error)
	// truncate index store
	Truncate() error
	Close() error
}

// rowIndexChange is one pending mutation. A change with deleted set removes the
// row; the location is meaningless in that case.
type rowIndexChange struct {
	loc     RowLocation
	deleted bool
}

// rowIndexBatch buffers the index changes of a single writer thread until the destination commit happens
type RowIndexBatch struct {
	index   TableIndex
	changes map[string]rowIndexChange
}

// NewRowIndexBatch returns an empty batch that resolves misses against index.
func NewRowIndexBatch(index TableIndex) *RowIndexBatch {
	if index == nil {
		return nil
	}

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

// Range hands every pending change to fn, stopping at the first error.
func (b *RowIndexBatch) Range(fn func(key string, loc RowLocation, deleted bool) error) error {
	for key, change := range b.changes {
		if err := fn(key, change.loc, change.deleted); err != nil {
			return err
		}
	}
	return nil
}
