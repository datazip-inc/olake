package types

// RowLocation addresses a single row within one data file of a destination table.
type RowLocation struct {
	FilePath string
	Position int64
}

// tableIndex is interface for index store
type StreamIndex interface {
	// lookup for keys in index store
	Lookup(key string) (loc RowLocation, found bool, err error)
	// commit changes to index store
	Commit(batch *StreamIndexThread, snapshotID *int64) error
	// LastCommittedSnapshot returns the last committed snapshot ID
	LastCommittedSnapshot() (snapshotID int64, err error)
	// truncate index store
	Truncate() error
	Close() error
}

// streamIndexChange is one pending mutation.
type streamIndexChange struct {
	loc RowLocation
}

// StreamIndexThread buffers the index changes of a single writer thread until the destination commit happens
type StreamIndexThread struct {
	index   StreamIndex
	changes map[string]streamIndexChange
}

// NewStreamIndexThread returns an empty batch that resolves misses against index.
func NewStreamIndexThread(index StreamIndex) *StreamIndexThread {
	if index == nil {
		return nil
	}

	return &StreamIndexThread{index: index, changes: make(map[string]streamIndexChange)}
}

// Put records key as living at loc once this batch is applied.
func (b *StreamIndexThread) Put(key string, loc RowLocation) {
	b.changes[key] = streamIndexChange{loc: loc}
}

// Lookup resolves key against this batch's pending changes first, so a row
// written moments ago by the same thread resolves to where it is about to land
// rather than to the stale location the last sync committed.
func (b *StreamIndexThread) Lookup(key string) (RowLocation, bool, error) {
	if change, pending := b.changes[key]; pending {
		return change.loc, true, nil
	}
	if b.index == nil {
		return RowLocation{}, false, nil
	}
	return b.index.Lookup(key)
}

// Range hands every pending change to fn, stopping at the first error.
func (b *StreamIndexThread) Range(fn func(key string, loc RowLocation) error) error {
	for key, change := range b.changes {
		if err := fn(key, change.loc); err != nil {
			return err
		}
	}
	return nil
}
