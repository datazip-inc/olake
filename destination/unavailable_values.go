package destination

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
)

// RowRead names one stored row to read back, by its _olake_id, and the destination columns wanted from it.
type RowRead struct {
	OlakeID string
	Columns []string
}

// RowReader is a destination that can read stored rows back. Recovering values the source could not
// send (Postgres unchanged TOAST) is built on it.
type RowReader interface {
	// ReadRows reads back the latest stored values of the given rows and passes each to emit with its index
	// in rows; nil values means the row is not stored.
	ReadRows(ctx context.Context, rows []RowRead, emit func(row int, values map[string]any) error) error
}

// destinationValuesFetcher fills the columns the source could not send (Postgres unchanged TOAST) with the row's
// previous value: what this thread last wrote for the row, else what the destination table stores for it.
type destinationValuesFetcher struct {
	stream types.StreamInterface
	// reader reads a row's previous version back from the destination table: the destination's part of the work.
	reader RowReader

	unavailableColumns map[string]struct{} // every column this thread has seen unavailable; only these are cached
	cachedValues       *cachedValues       // per row, the values this thread last wrote for it
	// read holds the values read from the destination until fillRecord moves them into their records. A row read for
	// a record past a call's prefix waits here for the next call, which then does not read it again. A read starts by
	// clearing it, so it holds at most one read: the destination's limit on one read (Iceberg: 1 GB) and the row that
	// passes it. That clears nothing unless a row this thread had cached was dropped from the cache in between.
	read map[string]map[string]any

	recovered   int64 // values filled into records
	fromTable   int64 // values read from the destination
	unrecovered int64 // values left as the placeholder
}

// RecoverUnavailableValues fills in the values the source could not send (Postgres unchanged TOAST) with each row's
// previous value. It fills records in order up to MaxDestinationBatchBytes and returns how many it filled, at least 1;
// the caller writes those and calls again with the rest. It reads once, at the first record that needs a value not in
// memory, and stops at a record that read did not answer. Only a lone first record may pass the limit.
func (f *destinationValuesFetcher) RecoverUnavailableValues(ctx context.Context, records []types.RawRecord) (int, error) {
	hasUnavailableValues := f.findUnavailableValues(records)
	if !hasUnavailableValues && f.cachedValues.isEmpty() {
		return len(records), nil // nothing to fill, and nothing cached that these writes could make stale
	}
	defer f.keepCachedValuesWithinBudget()

	size, read := int64(0), false
	for idx := range records {
		record := &records[idx]
		if !read && f.needsRead(record) {
			read = true
			clear(f.read) // what is still kept is read again with the rest, so the map never holds more than one read
			if err := f.readRows(ctx, f.rowsToRead(records[idx:])); err != nil {
				return 0, err
			}
		}

		recordSize := int64(0) // a batch with nothing to fill is written as it is
		if hasUnavailableValues {
			recordSize = f.filledSize(record)
		}
		if idx > 0 && (f.needsRead(record) || size+recordSize > constants.MaxDestinationBatchBytes) {
			return idx, nil // the next call goes on from this record
		}
		f.fillRecord(record)
		size += recordSize
	}
	return len(records), nil
}

// findUnavailableValues adds the records' unavailable columns to the columns this thread caches, and reports whether
// any record has one.
func (f *destinationValuesFetcher) findUnavailableValues(records []types.RawRecord) bool {
	found := false
	for idx := range records {
		for _, column := range records[idx].UnavailableColumns {
			if len(f.unavailableColumns) == 0 {
				cachingThreads.Add(1) // this thread starts caching, so it takes a share of the budget
			}
			f.unavailableColumns[column] = struct{}{}
			found = true
		}
	}
	return found
}

// keepCachedValuesWithinBudget drops this thread's oldest cached rows once its cache is over its share of the budget.
// It runs only after the records are filled, as a later record may need what an earlier one in the same call wrote.
func (f *destinationValuesFetcher) keepCachedValuesWithinBudget() {
	before := len(f.cachedValues.rows)
	share := maxCachedBytes / cachingThreads.Load() // this thread is one of them, so never a division by zero
	if dropped := f.cachedValues.trim(share); dropped > 0 {
		logger.Debugf("Stream[%s]: dropped %d of %d cached row(s), %d bytes left", f.stream.ID(), dropped, before, f.cachedValues.bytes)
	}
}

// needsRead reports whether some of the record's previous values must still be read from the destination.
func (f *destinationValuesFetcher) needsRead(record *types.RawRecord) bool {
	return len(f.columnsToRead(record)) > 0
}

// columnsToRead lists the record's unavailable columns whose previous value is not in memory yet.
func (f *destinationValuesFetcher) columnsToRead(record *types.RawRecord) []string {
	olakeID := olakeIDOf(record)
	var columns []string
	for _, column := range record.UnavailableColumns {
		if _, found := f.previousValue(olakeID, column); !found {
			columns = append(columns, column)
		}
	}
	return columns
}

// previousValue is the row's value of column before this record: what this thread last wrote for it, else what was
// read from the destination. A row deleted earlier in this sync gives the placeholder, so its old values never come back.
func (f *destinationValuesFetcher) previousValue(olakeID, column string) (any, bool) {
	if value, found := f.cachedValues.value(olakeID, column); found {
		return value, true
	}
	value, found := f.read[olakeID][column]
	return value, found
}

// filledSize is the size of the record's values once fillRecord has filled them in.
func (f *destinationValuesFetcher) filledSize(record *types.RawRecord) int64 {
	olakeID := olakeIDOf(record)
	size := dataSize(record.Data)
	for _, column := range record.UnavailableColumns {
		if value, found := f.previousValue(olakeID, column); found && value != constants.UnavailableValue {
			size += valueSize(value) - valueSize(record.Data[column])
		}
	}
	return size
}

// fillRecord writes the row's previous values into the record's unavailable columns and caches what the record
// writes, the placeholder too, so the row's next change needs no read. A delete caches only that the row is gone.
func (f *destinationValuesFetcher) fillRecord(record *types.RawRecord) {
	olakeID := olakeIDOf(record)
	if isDelete(record) {
		f.cachedValues.markDeleted(olakeID)
		return
	}

	for _, column := range record.UnavailableColumns {
		if value, found := f.previousValue(olakeID, column); found && value != constants.UnavailableValue {
			record.Data[column] = value
			f.recovered++
			continue
		}
		f.unrecovered++
	}
	delete(f.read, olakeID) // what the record writes is cached; a value read kept past a cache trim would be stale
	f.cachedValues.remember(olakeID, record.Data, f.unavailableColumns)
}

// Close gives back this thread's share of the cache budget and logs what it recovered. It frees the cached values
// now, because this writer's commit and other streams' commits run after it.
func (f *destinationValuesFetcher) Close() {
	if len(f.unavailableColumns) > 0 {
		cachingThreads.Add(-1)
	}
	f.unavailableColumns, f.cachedValues, f.read = nil, nil, nil

	if f.recovered+f.unrecovered == 0 {
		return
	}
	logger.Infof("Stream[%s]: recovered %d unavailable column value(s), read %d from the destination table, %d could not be recovered",
		f.stream.ID(), f.recovered, f.fromTable, f.unrecovered)
}

// olakeIDOf is the key of the record's row.
func olakeIDOf(record *types.RawRecord) string {
	return record.OlakeColumns[constants.OlakeID].(string)
}

// isDelete reports whether the record deletes its row.
func isDelete(record *types.RawRecord) bool {
	return record.OlakeColumns[constants.OpType].(string) == "d"
}

// newDestinationValuesFetcher sets up recovery for a writer thread whose writer can read rows back from the destination
// and whose stream keeps a row index, else nil. The writer only reads rows back; which rows to read and what their
// values mean is decided here.
func newDestinationValuesFetcher(stream types.StreamInterface, writer Writer, index types.StreamIndex) *destinationValuesFetcher {
	reader, ok := writer.(RowReader)
	if !ok || index == nil {
		return nil // a destination that cannot read rows back (Parquet), or no row index (equality or append mode)
	}
	return &destinationValuesFetcher{
		stream:             stream,
		reader:             reader,
		unavailableColumns: make(map[string]struct{}),
		cachedValues:       newCachedValues(),
		read:               make(map[string]map[string]any),
	}
}

// recoverUnavailableValues fills the values the source could not send into a prefix of records whose values stay
// within MaxDestinationBatchBytes, so no write exceeds the 2 GB message limit and recovered values are written
// before more are read. The whole batch for a thread without one.
func (wt *WriterThread) recoverUnavailableValues(ctx context.Context, records []types.RawRecord) (int, error) {
	if wt.destinationValuesFetcher == nil {
		return len(records), nil
	}
	filled, err := wt.destinationValuesFetcher.RecoverUnavailableValues(ctx, records)
	if err != nil {
		return 0, fmt.Errorf("failed to recover unavailable values: %w", err)
	}
	return filled, nil
}

// closeUnavailableValues frees the cache and gives back its share of the budget. It runs before the
// writer commits, because this writer's commit and other streams' commits come after it.
func (wt *WriterThread) closeUnavailableValues() {
	if wt.destinationValuesFetcher != nil {
		wt.destinationValuesFetcher.Close()
	}
}
