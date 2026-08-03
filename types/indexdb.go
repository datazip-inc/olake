package types

import "context"

// DeleteMode selects how a destination represents the removal of a row that a
// previous sync already committed.
type DeleteMode string

const (
	// DeleteModeEquality writes Iceberg equality delete files keyed on the table's
	// identifier field. Readers resolve them by matching key values, so no row
	// index is needed and this mode carries no bootstrap cost.
	DeleteModeEquality DeleteMode = "eq"
	// DeleteModePosition writes Iceberg positional delete files, which address a
	// row as (data file, ordinal). Producing them requires a durable
	// identifier -> RowLocation index of every live row in the table.
	DeleteModePosition DeleteMode = "pos"
	// DeleteModeDeletionVector writes Iceberg v3 deletion vectors.
	// TODO: not implemented; validation rejects it.
	DeleteModeDeletionVector DeleteMode = "dv"
)

// NeedsRowIndex reports whether the mode can only be served by maintaining a
// TableIndex alongside the destination table.
func (m DeleteMode) NeedsRowIndex() bool {
	return m == DeleteModePosition
}

// UnknownSeqNumber marks a row whose data file has no sequence number yet.
// Iceberg assigns a data file its sequence number when the snapshot that adds it
// is committed, so a row indexed while its file is still being written cannot
// know one. Iceberg sequence numbers start at 1, which leaves zero free to mean
// "not known yet".
//
// A row carrying it must be treated as newer than any delete already in the
// table: the file is being added by the sync in progress, so nothing committed
// before it can apply to it.
const UnknownSeqNumber int64 = 0

// RowLocation addresses a single row within one data file of a destination table.
type RowLocation struct {
	FilePath string
	Position int64
	// SeqNumber is the data sequence number of the file holding the row, or
	// UnknownSeqNumber. It is what decides whether a delete recorded elsewhere in
	// the table predates this row and therefore must not remove it.
	SeqNumber int64
}

// PrecedesDelete reports whether a delete recorded at deleteSeqNumber applies to
// this row. Iceberg only lets a delete remove rows from data files whose
// sequence number does not exceed the delete's own, and a row whose file has no
// sequence number yet is newer than anything already committed.
func (l RowLocation) PrecedesDelete(deleteSeqNumber int64) bool {
	if l.SeqNumber == UnknownSeqNumber {
		return false
	}
	return l.SeqNumber <= deleteSeqNumber
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
type TableIndex interface {
	// Lookup returns the indexed location of key. found is false when the index
	// holds no live row for key.
	Lookup(key string) (loc RowLocation, found bool, err error)
	// NewTxn stages mutations for one writer thread. Writes only become
	// permanent on Commit; Rollback restores every key the txn overwrote.
	NewTxn() (IndexTxn, error)
	// IndexedSnapshot returns the destination snapshot ID this index reflects.
	// ok is false while the index has never been populated, which is the signal
	// that a full bootstrap scan is required.
	IndexedSnapshot() (snapshotID int64, ok bool, err error)
	// SetIndexedSnapshot records the snapshot the index is now consistent with.
	SetIndexedSnapshot(snapshotID int64) error
	// Truncate removes all entries and clears the indexed snapshot.
	Truncate() error
	Close() error
}

// IndexTxn accumulates index mutations for a single writer thread and ties them
// to that thread's destination commit.
//
// Writes are applied to the index as they arrive rather than buffered in memory,
// so a txn may span an arbitrarily large backfill without unbounded growth. The
// cost of that choice is that concurrent txns on one stream observe each other's
// uncommitted writes; callers must therefore partition keys across threads,
// which every current sync path already does (backfill chunks are disjoint and
// CDC runs a single writer per stream).
type IndexTxn interface {
	// Lookup resolves key against the index including this txn's own writes.
	Lookup(key string) (loc RowLocation, found bool, err error)
	// Put records key as living at loc, replacing any previous location.
	Put(key string, loc RowLocation) error
	// Delete drops key, marking the row as no longer live.
	Delete(key string) error
	// Commit makes this txn's writes permanent and discards its undo log.
	Commit() error
	// Rollback restores the pre-txn location of every key the txn mutated.
	Rollback() error
}
