package iceberg

import (
	"context"
	"math"
	"slices"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination/iceberg/proto"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
)

// unavailableValuesResolver fills the columns the source could not send (Postgres unchanged TOAST) with the row's
// previous value: what this thread last wrote for the row, else what the destination table stores for it.
type unavailableValuesResolver struct {
	threadID string
	stream   types.StreamInterface
	// index locates a row's previous version: this thread's uncommitted writes first, then the committed index.
	index *types.StreamIndexThread
	// tableIndexClient calls the Java table index service, which reads stored values back out of the data files.
	tableIndexClient proto.TableIndexServiceClient
	// ensureReadable closes, without committing, the data files this thread's writer still has open, as an open
	// file cannot be read; a no-op for the legacy writer, whose open files Java closes.
	ensureReadable func(ctx context.Context, paths []string) error

	unavailableColumns map[string]struct{} // every column this thread has seen unavailable; only these are cached
	cachedValues       *cachedValues       // per row, the values this thread last wrote for it
	readBuffer         *readBuffer         // values read from the table, until they are filled into records
	lastRead           readSize            // how big the last read's rows were, to size the next read

	recovered  int64 // values filled into records
	fromTable  int64 // values read from the destination
	unresolved int64 // values left as the placeholder
}

func newUnavailableValuesResolver(threadID string, stream types.StreamInterface, writer Writer, index *types.StreamIndexThread, tableIndexClient proto.TableIndexServiceClient) *unavailableValuesResolver {
	ensureReadable := func(context.Context, []string) error { return nil }
	if withOpenFiles, ok := writer.(openFileWriter); ok {
		ensureReadable = withOpenFiles.EnsureReadable
	}
	return &unavailableValuesResolver{
		threadID:           threadID,
		stream:             stream,
		index:              index,
		tableIndexClient:   tableIndexClient,
		ensureReadable:     ensureReadable,
		unavailableColumns: make(map[string]struct{}),
		cachedValues:       newCachedValues(),
		readBuffer:         newReadBuffer(),
	}
}

// Resolve fills in the values the source could not send (Postgres unchanged TOAST) with each row's previous value.
// It fills records in order up to maxBytes and returns how many it filled; the caller writes those and calls again.
func (r *unavailableValuesResolver) Resolve(ctx context.Context, records []types.RawRecord, maxBytes int64) (int, error) {
	hasUnavailableValues := r.findUnavailableValues(records)
	if !hasUnavailableValues && r.cachedValues.isEmpty() {
		return len(records), nil // nothing to fill, and nothing cached that these writes could make stale
	}
	defer r.keepWriteCacheWithinBudget()

	if err := r.readUncachedValuesFromTable(ctx, records, maxBytes); err != nil {
		return 0, err
	}
	filled := r.fillUnavailableValues(records, maxBytes)
	return filled, nil
}

// findUnavailableValues drops the unavailable columns that need no value (deselected ones, and all of a delete's, as
// it writes no values), adds the rest to the columns this thread caches, and reports whether any record has one.
func (r *unavailableValuesResolver) findUnavailableValues(records []types.RawRecord) bool {
	found := false
	for idx := range records {
		record := &records[idx]
		if isDelete(record) {
			record.UnavailableColumns = nil
			continue
		}
		record.UnavailableColumns = slices.DeleteFunc(record.UnavailableColumns, func(column string) bool {
			_, selected := record.Data[column]
			return !selected
		})
		for _, column := range record.UnavailableColumns {
			if len(r.unavailableColumns) == 0 {
				writeCacheThreads.Add(1) // this thread starts caching, so it takes a share of the budget
			}
			r.unavailableColumns[column] = struct{}{}
			found = true
		}
	}
	return found
}

// readUncachedValuesFromTable reads from the table, in one call, the values the records need that memory lacks, for
// as many records as fit in maxBytes. The first record is always filled, so if it did not fit, it is then read alone.
func (r *unavailableValuesResolver) readUncachedValuesFromTable(ctx context.Context, records []types.RawRecord, maxBytes int64) error {
	if err := r.readFromTable(ctx, records, maxBytes-r.readBuffer.bytes); err != nil {
		return err
	}
	if len(records) > 0 && r.needsRead(&records[0]) {
		return r.readFromTable(ctx, records[:1], math.MaxInt64)
	}
	return nil
}

// fillUnavailableValues fills the records in order and returns how many it filled: always the first, then each next
// one until a record needs a value not read yet or the total would pass maxBytes.
func (r *unavailableValuesResolver) fillUnavailableValues(records []types.RawRecord, maxBytes int64) int {
	size := int64(0)
	for idx := range records {
		record := &records[idx]
		recordSize := r.filledSize(record)
		if idx > 0 && (r.needsRead(record) || size+recordSize > maxBytes) {
			return idx // the next call goes on from this record
		}
		r.fillRecord(record)
		size += recordSize
	}
	return len(records)
}

// keepWriteCacheWithinBudget drops this thread's oldest cached rows once its cache is over its share of the budget.
// It runs only after the records are filled, as a later record may need what an earlier one in the same call wrote.
func (r *unavailableValuesResolver) keepWriteCacheWithinBudget() {
	before := len(r.cachedValues.rows)
	share := maxWriteCacheBytes / writeCacheThreads.Load() // this thread is one of them, so never a division by zero
	if dropped := r.cachedValues.trim(share); dropped > 0 {
		logger.Debugf("Thread[%s]: dropped %d of %d cached row(s), %d bytes left", r.threadID, dropped, before, r.cachedValues.bytes)
	}
}

// needsRead reports whether some of the record's previous values must still be read from the table.
func (r *unavailableValuesResolver) needsRead(record *types.RawRecord) bool {
	return len(r.columnsToRead(record)) > 0
}

// columnsToRead lists the record's unavailable columns whose previous value is not in memory yet.
func (r *unavailableValuesResolver) columnsToRead(record *types.RawRecord) []string {
	olakeID := olakeIDOf(record)
	var columns []string
	for _, column := range record.UnavailableColumns {
		if _, found := r.previousValue(olakeID, column); !found {
			columns = append(columns, column)
		}
	}
	return columns
}

// previousValue is the row's value of column before this record: what this thread last wrote for it, else what was
// read from the table. A row deleted earlier in this sync gives the placeholder, so its old values never come back.
func (r *unavailableValuesResolver) previousValue(olakeID, column string) (any, bool) {
	if value, found := r.cachedValues.value(olakeID, column); found {
		return value, true
	}
	return r.readBuffer.value(olakeID, column)
}

// filledSize is the size of the record's values once fillRecord has filled them in.
func (r *unavailableValuesResolver) filledSize(record *types.RawRecord) int64 {
	olakeID := olakeIDOf(record)
	size := dataSize(record.Data)
	for _, column := range record.UnavailableColumns {
		if value, found := r.previousValue(olakeID, column); found && value != constants.UnavailableValue {
			size += valueSize(value) - valueSize(record.Data[column])
		}
	}
	return size
}

// fillRecord writes the row's previous values into the record's unavailable columns and caches what the record
// writes, the placeholder too, so the row's next change needs no read. A delete caches only that the row is gone.
func (r *unavailableValuesResolver) fillRecord(record *types.RawRecord) {
	olakeID := olakeIDOf(record)
	if isDelete(record) {
		r.cachedValues.markDeleted(olakeID)
		return
	}

	for _, column := range record.UnavailableColumns {
		if value, found := r.previousValue(olakeID, column); found && value != constants.UnavailableValue {
			record.Data[column] = value
			r.recovered++
			continue
		}
		r.unresolved++
	}
	r.readBuffer.release(olakeID)
	r.cachedValues.remember(olakeID, record.Data, r.unavailableColumns)
}

// Close gives back this thread's share of the write cache budget and logs what it recovered. It frees the cache
// now, because this writer's commit and other streams' commits run after it.
func (r *unavailableValuesResolver) Close() {
	if len(r.unavailableColumns) > 0 {
		writeCacheThreads.Add(-1)
	}
	r.unavailableColumns, r.cachedValues, r.readBuffer = nil, nil, nil

	if r.recovered+r.unresolved == 0 {
		return
	}
	logger.Infof("Thread[%s]: recovered %d unavailable column value(s), read %d from the destination table, %d could not be recovered",
		r.threadID, r.recovered, r.fromTable, r.unresolved)
}

// olakeIDOf is the key of the record's row.
func olakeIDOf(record *types.RawRecord) string {
	return record.OlakeColumns[constants.OlakeID].(string)
}

// isDelete reports whether the record deletes its row.
func isDelete(record *types.RawRecord) bool {
	return record.OlakeColumns[constants.OpType].(string) == "d"
}
