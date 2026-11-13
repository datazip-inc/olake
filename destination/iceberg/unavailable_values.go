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
	// reader reads stored values out of data files (Java side).
	reader proto.TableIndexServiceClient
	// writer is this thread's writer when it keeps data files open (arrow): a read asks it to close them first,
	// as an open file cannot be read. nil for the legacy writer, whose open files Java closes.
	writer openFileWriter

	unavailableColumns map[string]struct{} // every column this thread has seen unavailable; only these are cached
	writeCache         *writeCache         // per row, the values this thread last wrote for it
	readBuffer         *readBuffer         // values read from the destination, until fill moves them into records
	lastRead           readSize            // how big the last read's rows were, to size the next read

	recovered  int64 // values filled into records
	fromTable  int64 // values read from the destination
	unresolved int64 // values left as the placeholder
}

func newUnavailableValuesResolver(threadID string, stream types.StreamInterface, writer Writer, index *types.StreamIndexThread, reader proto.TableIndexServiceClient) *unavailableValuesResolver {
	withOpenFiles, _ := writer.(openFileWriter)
	return &unavailableValuesResolver{
		threadID:           threadID,
		stream:             stream,
		index:              index,
		reader:             reader,
		writer:             withOpenFiles,
		unavailableColumns: make(map[string]struct{}),
		writeCache:         newWriteCache(),
		readBuffer:         newReadBuffer(),
	}
}

// RecoverUnavailableValues fills the unavailable values of a prefix of records and returns its length, at least
// 1. The caller writes that prefix and calls again with the rest, so the values filled at once stay within limit
// bytes. It reads once, at the first record missing a value, and ends the prefix at a record that read did not
// answer. Only a lone first record may pass the limit.
func (r *unavailableValuesResolver) RecoverUnavailableValues(ctx context.Context, records []types.RawRecord, limit int64) (int, error) {
	r.collectUnavailableColumns(records)
	if len(r.unavailableColumns) == 0 {
		return len(records), nil // no column was ever unavailable in this thread: nothing to fill or cache
	}
	defer r.trimWriteCache()

	size, read := int64(0), false
	for idx := range records {
		record := &records[idx]
		if !read && r.needsRead(record) {
			read = true
			if err := r.readMissing(ctx, records, idx, limit-size); err != nil {
				return 0, err
			}
		}

		recordSize := r.filledSize(record)
		if idx > 0 && (r.needsRead(record) || size+recordSize > limit) {
			return idx, nil // the next call goes on from this record
		}
		r.fill(record)
		size += recordSize
	}
	return len(records), nil
}

// collectUnavailableColumns drops the unavailable columns the stream does not sync and adds the rest to the
// columns this thread caches.
func (r *unavailableValuesResolver) collectUnavailableColumns(records []types.RawRecord) {
	for idx := range records {
		record := &records[idx]
		record.UnavailableColumns = slices.DeleteFunc(record.UnavailableColumns, func(column string) bool {
			_, selected := record.Data[column]
			return !selected
		})
		for _, column := range record.UnavailableColumns {
			if len(r.unavailableColumns) == 0 {
				writeCacheThreads.Add(1) // this thread starts caching, so it takes a share of the budget
			}
			r.unavailableColumns[column] = struct{}{}
		}
	}
}

// readMissing reads the values the records from idx on are missing, as many as fit in left: the bytes of limit the
// records before idx did not fill, less what the read buffer already holds for later records. The first record is
// filled whatever its size, so if it is still missing values, it is then read alone.
func (r *unavailableValuesResolver) readMissing(ctx context.Context, records []types.RawRecord, idx int, left int64) error {
	if err := r.readAhead(ctx, records[idx:], left-r.readBuffer.bytes); err != nil {
		return err
	}
	if idx == 0 && r.needsRead(&records[0]) {
		return r.readAhead(ctx, records[:1], math.MaxInt64)
	}
	return nil
}

// needsRead reports whether some unavailable value of the record is not known yet.
func (r *unavailableValuesResolver) needsRead(record *types.RawRecord) bool {
	return len(r.missing(record)) > 0
}

// missing lists the record's unavailable columns whose previous value is not known yet.
func (r *unavailableValuesResolver) missing(record *types.RawRecord) []string {
	olakeID := olakeIDOf(record)
	var missing []string
	for _, column := range record.UnavailableColumns {
		if _, found := r.previous(olakeID, column); !found {
			missing = append(missing, column)
		}
	}
	return missing
}

// previous is the row's value of column before this record: what this thread last wrote for the row, else what
// was read for it. A row deleted earlier in this sync gives the placeholder, so its old values never come back.
func (r *unavailableValuesResolver) previous(olakeID, column string) (any, bool) {
	if value, found := r.writeCache.value(olakeID, column); found {
		return value, true
	}
	return r.readBuffer.value(olakeID, column)
}

// filledSize is the size of the record's values once fill has filled them in.
func (r *unavailableValuesResolver) filledSize(record *types.RawRecord) int64 {
	olakeID := olakeIDOf(record)
	size := dataSize(record.Data)
	for _, column := range record.UnavailableColumns {
		if value, found := r.previous(olakeID, column); found && value != constants.UnavailableValue {
			size += valueSize(value) - valueSize(record.Data[column])
		}
	}
	return size
}

// fill writes the row's previous values into the record's unavailable columns and caches what the record writes,
// the placeholder too, so the row's next change needs no read. A delete caches only that the row is gone.
func (r *unavailableValuesResolver) fill(record *types.RawRecord) {
	olakeID := olakeIDOf(record)
	if isDelete(record) {
		r.writeCache.markDeleted(olakeID)
		return
	}

	for _, column := range record.UnavailableColumns {
		if value, found := r.previous(olakeID, column); found && value != constants.UnavailableValue {
			record.Data[column] = value
			r.recovered++
			continue
		}
		r.unresolved++
	}
	r.readBuffer.release(olakeID)
	r.writeCache.remember(olakeID, record.Data, r.unavailableColumns)
}

// trimWriteCache drops this thread's oldest cached rows once its cache is over its share of the budget. This
// thread is one of writeCacheThreads, so the share never divides by zero.
func (r *unavailableValuesResolver) trimWriteCache() {
	before := len(r.writeCache.rows)
	if dropped := r.writeCache.trim(maxWriteCacheBytes / writeCacheThreads.Load()); dropped > 0 {
		logger.Debugf("Thread[%s]: dropped %d of %d cached row(s), %d bytes left", r.threadID, dropped, before, r.writeCache.bytes)
	}
}

// Close gives back this thread's share of the write cache budget and logs what it recovered. It frees the cache
// now, because this writer's commit and other streams' commits run after it.
func (r *unavailableValuesResolver) Close() {
	if len(r.unavailableColumns) > 0 {
		writeCacheThreads.Add(-1)
	}
	r.unavailableColumns, r.writeCache, r.readBuffer = nil, nil, nil

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
