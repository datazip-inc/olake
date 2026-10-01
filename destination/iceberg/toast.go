package iceberg

import (
	"context"
	"fmt"
	"io"
	"math"
	"sort"
	"sync/atomic"

	"github.com/goccy/go-json"
	"google.golang.org/grpc"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination/iceberg/proto"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
)

// toastResolver fills the columns a change could not carry — Postgres omits unchanged
// out-of-line (TOASTed) columns from an UPDATE unless the table's replica identity is
// FULL — with the value already stored in the row's previous version.
//
// Where that previous version lives is exactly what the stream index answers, so the
// resolver only exists for positional-delete upsert threads. Two sources are used, in
// this order:
//
//	carry: values this thread wrote earlier in the same sync, kept in memory because
//	       rows in a data file that is still open cannot be read back.
//	table: one batched ReadRows call that reads the columns out of the committed (or
//	       closed) data files, grouped by file so the Java side can skip whole pages.
//
// A batch in which no record carries an unavailable column costs one length check per
// record, so streams that never produce the marker are unaffected.
type toastResolver struct {
	writer Writer
	// reader reads stored values out of data files (Java side).
	reader   proto.ToastReadServiceClient
	threadID string
	// resolveColumn maps a source column name to its destination column name.
	resolveColumn func(string) string
	// normalized is false when the whole row is stored as JSON in one column.
	normalized bool

	tracked    map[string]struct{}  // columns seen marked in this thread
	carry      map[string]*carryRow // olake id -> values this thread wrote
	carryBytes int64

	fromMemory int64
	fromTable  int64
	unresolved int64
}

// carryEntryOverhead approximates the memory cost of one carried row.
const carryEntryOverhead = int64(96)

// carryUsage is the carry memory held by all threads of this process. The budget is shared
// so that N streams cannot use N times constants.MaxToastCarryBytes.
var carryUsage atomic.Int64

// carryRow is what this thread last wrote for one row. deleted means nothing may be
// carried forward from it.
type carryRow struct {
	deleted bool
	values  map[string]any
}

// pendingValue is the result of one read, shared by every record waiting on it.
type pendingValue struct {
	value    any
	resolved bool
}

// readKey identifies one read. jsonKey is set when the whole row is stored as JSON.
type readKey struct {
	filePath string
	position int64
	column   string
	jsonKey  string
}

// rowKey identifies one row of the destination table.
type rowKey struct {
	filePath string
	position int64
}

// waiter is a record column to fill once its read returns.
type waiter struct {
	record  *types.RawRecord
	olakeID string
	column  string
	read    *pendingValue
}

func newToastResolver(threadID string, stream types.StreamInterface, writer Writer, reader proto.ToastReadServiceClient) *toastResolver {
	return &toastResolver{
		writer:        writer,
		reader:        reader,
		threadID:      threadID,
		resolveColumn: stream.ResolveColumnName,
		normalized:    stream.NormalizationEnabled(),
		tracked:       make(map[string]struct{}),
		carry:         make(map[string]*carryRow),
	}
}

// Resolve fills every unavailable value in the batch it can recover. The rest keep
// constants.UnavailableValue, as before this feature.
func (r *toastResolver) Resolve(ctx context.Context, records []types.RawRecord) error {
	if !r.track(records) {
		return nil
	}

	reads := make(map[readKey]*pendingValue)
	var waiting []waiter

	// Records are handled in order: each one only sees what earlier changes wrote, which
	// keeps several changes to one row in the same batch correct.
	for idx := range records {
		record := &records[idx]
		olakeID := record.OlakeColumns[constants.OlakeID].(string)

		if record.OlakeColumns[constants.OpType].(string) == "d" {
			r.tombstone(olakeID)
			continue
		}

		for _, column := range record.UnavailableColumns {
			if _, selected := record.Data[column]; !selected {
				continue // column not synced
			}

			if row := r.carry[olakeID]; row != nil {
				if row.deleted {
					// deleted earlier in this sync: old values must not come back
					r.unresolved++
					continue
				}
				if carried, inCarry := row.values[column]; inCarry {
					// an earlier record in this batch already queued the read: share it
					if pending, isPending := carried.(*pendingValue); isPending {
						if !pending.resolved {
							waiting = append(waiting, waiter{record: record, olakeID: olakeID, column: column, read: pending})
							continue
						}
						carried = pending.value
					}
					record.Data[column] = carried
					r.fromMemory++
					continue
				}
			}

			location, found, err := r.writer.Lookup(olakeID)
			if err != nil {
				return fmt.Errorf("failed to look up row[%s] in index: %w", olakeID, err)
			}
			if !found {
				// no previous version: a filtered row, or the UPDATE changed the primary key
				r.unresolved++
				continue
			}

			read := r.queueRead(location, column, reads)
			r.remember(olakeID, column, read)
			waiting = append(waiting, waiter{record: record, olakeID: olakeID, column: column, read: read})
		}

		// Remember the values this record does carry, for the next change to the row.
		for column := range r.tracked {
			if value, present := record.Data[column]; present && !isUnavailable(value) {
				r.remember(olakeID, column, value)
			}
		}
	}

	if len(reads) > 0 {
		if err := r.readValues(ctx, reads); err != nil {
			return err
		}

		for _, slot := range waiting {
			if !slot.read.resolved || isUnavailable(slot.read.value) {
				// forget the failed read so the next change to the row tries again
				r.forget(slot.olakeID, slot.column)
				r.unresolved++
				continue
			}
			slot.record.Data[slot.column] = slot.read.value
			r.remember(slot.olakeID, slot.column, slot.read.value)
			r.fromTable++
		}
	}

	r.trim()

	return nil
}

// track adds this batch's marked columns to r.tracked and reports whether there is work.
// Once a column is tracked, every later batch is processed so carry stays up to date,
// even batches with no marker.
func (r *toastResolver) track(records []types.RawRecord) bool {
	for idx := range records {
		for _, column := range records[idx].UnavailableColumns {
			// lookup before insert: cheaper, and a column is new only once per thread
			if _, known := r.tracked[column]; !known {
				r.tracked[column] = struct{}{}
			}
		}
	}

	return len(r.tracked) > 0
}

// queueRead returns the read for this column of the row at location, adding it to the
// batch's reads the first time it is asked for.
func (r *toastResolver) queueRead(location types.RowLocation, column string, reads map[readKey]*pendingValue) *pendingValue {
	destColumn, jsonKey := r.projection(column)
	// without normalization every column comes from the same JSON column; jsonKey tells them apart
	identity := readKey{filePath: location.FilePath, position: location.Position, column: destColumn, jsonKey: jsonKey}
	if read, queued := reads[identity]; queued {
		return read
	}

	read := &pendingValue{}
	reads[identity] = read

	return read
}

// projection returns the destination column holding a source column, and the JSON key
// to pick out of it when normalization is off.
func (r *toastResolver) projection(column string) (destColumn, key string) {
	if r.normalized {
		return r.resolveColumn(column), ""
	}
	return constants.StringifiedData, column
}

// readValues reads all queued values in one streamed call, after closing any data file
// this thread still has open among the ones to read.
func (r *toastResolver) readValues(ctx context.Context, reads map[readKey]*pendingValue) error {
	columnIndex := make(map[string]int)
	var columns []string
	positions := make(map[string]map[int64]struct{})
	byRow := make(map[rowKey][]readKey)

	for identity := range reads {
		if _, known := columnIndex[identity.column]; !known {
			columnIndex[identity.column] = len(columns)
			columns = append(columns, identity.column)
		}
		if positions[identity.filePath] == nil {
			positions[identity.filePath] = make(map[int64]struct{})
		}
		positions[identity.filePath][identity.position] = struct{}{}
		row := rowKey{filePath: identity.filePath, position: identity.position}
		byRow[row] = append(byRow[row], identity)
	}

	paths := make([]string, 0, len(positions))
	for path := range positions {
		paths = append(paths, path)
	}
	sort.Strings(paths)

	// a file still being written has no footer and cannot be read
	if err := r.writer.EnsureReadable(ctx, paths); err != nil {
		return err
	}

	request := &proto.ReadRowsRequest{ThreadId: r.threadID, Columns: columns}
	for _, path := range paths {
		ordered := make([]int64, 0, len(positions[path]))
		for position := range positions[path] {
			ordered = append(ordered, position)
		}
		sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
		request.Files = append(request.Files, &proto.ReadRowsRequest_FileRows{FilePath: path, Positions: ordered})
	}

	// The limit applies per streamed message; the 4 MB default is too small for one large
	// value. Java keeps each message to 64 rows or 16 MB.
	stream, err := r.reader.ReadRows(ctx, request, grpc.MaxCallRecvMsgSize(math.MaxInt32))
	if err != nil {
		return fmt.Errorf("failed to read unavailable column values: %w", err)
	}

	for {
		batch, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("failed to read unavailable column values: %w", err)
		}

		for _, row := range batch.GetRows() {
			for _, identity := range byRow[rowKey{filePath: row.GetFilePath(), position: row.GetPosition()}] {
				index := columnIndex[identity.column]
				if index >= len(row.GetValues()) {
					continue // reply does not match the request
				}
				value, err := decodeValue(row.GetValues()[index], identity.jsonKey)
				if err != nil {
					// keep the placeholder: a retry would read the same bytes again
					logger.Debugf("Thread[%s]: cannot decode %s of %s row %d: %s", r.threadID, identity.column, row.GetFilePath(), row.GetPosition(), err)
					continue
				}
				reads[identity].value = value
				reads[identity].resolved = true
			}
		}
	}
}

// remember stores the newest value this thread wrote for a row's column.
func (r *toastResolver) remember(olakeID, column string, value any) {
	row := r.carry[olakeID]
	if row == nil || row.deleted {
		if row == nil {
			// a tombstone already counted this entry
			r.account(int64(len(olakeID)) + carryEntryOverhead)
		}
		row = &carryRow{values: make(map[string]any, len(r.tracked))}
		r.carry[olakeID] = row
	}

	r.account(valueSize(value) - valueSize(row.values[column]))
	row.values[column] = value
}

// tombstone marks a row deleted and releases the values it carried.
func (r *toastResolver) tombstone(olakeID string) {
	if row := r.carry[olakeID]; row != nil {
		for _, value := range row.values {
			r.account(-valueSize(value))
		}
	} else {
		r.account(int64(len(olakeID)) + carryEntryOverhead)
	}

	r.carry[olakeID] = &carryRow{deleted: true}
}

// forget removes one carried column whose read returned nothing.
func (r *toastResolver) forget(olakeID, column string) {
	row := r.carry[olakeID]
	if row == nil {
		return
	}

	r.account(-valueSize(row.values[column]))
	delete(row.values, column)
}

// account adds delta carried bytes to this thread and to the process total.
func (r *toastResolver) account(delta int64) {
	r.carryBytes += delta
	carryUsage.Add(delta)
}

// trim drops this thread's carry when the process is over budget. Nothing is lost: the
// index still locates every dropped row, so the next change reads it back.
func (r *toastResolver) trim() {
	if carryUsage.Load() <= constants.MaxToastCarryBytes {
		return
	}

	logger.Debugf("Thread[%s]: dropping %d carried unavailable value(s) holding %d bytes", r.threadID, len(r.carry), r.carryBytes)
	r.drop()
}

// drop releases everything this thread carries.
func (r *toastResolver) drop() {
	carryUsage.Add(-r.carryBytes)
	r.carryBytes = 0
	r.carry = make(map[string]*carryRow)
}

// Close releases this thread's carry and logs what it recovered.
func (r *toastResolver) Close() {
	r.drop()

	if r.fromMemory+r.fromTable+r.unresolved == 0 {
		return
	}

	logger.Debugf("Thread[%s]: recovered %d unavailable column value(s) from this sync and %d from the destination table, %d could not be recovered",
		r.threadID, r.fromMemory, r.fromTable, r.unresolved)
}

func valueSize(value any) int64 {
	switch typed := value.(type) {
	case nil:
		return 0
	case string:
		return int64(len(typed))
	case json.RawMessage:
		return int64(len(typed))
	case *pendingValue:
		return 0
	default:
		return 16
	}
}

func isUnavailable(value any) bool {
	text, ok := value.(string)
	return ok && text == constants.UnavailableValue
}

// decodeValue converts one stored value for the record; unset means NULL. With jsonKey,
// the value is the whole row as JSON and that key's raw JSON is returned.
func decodeValue(value *proto.ColumnValue, jsonKey string) (any, error) {
	var decoded any
	switch stored := value.GetValue().(type) {
	case *proto.ColumnValue_StringValue:
		decoded = stored.StringValue
	case *proto.ColumnValue_LongValue:
		decoded = stored.LongValue
	case *proto.ColumnValue_DoubleValue:
		decoded = stored.DoubleValue
	case *proto.ColumnValue_BoolValue:
		decoded = stored.BoolValue
	case *proto.ColumnValue_BytesValue:
		decoded = string(stored.BytesValue)
	default:
		//nolint:nilnil // an unset value means the stored column is NULL, which is a value
		return nil, nil
	}

	if jsonKey == "" {
		return decoded, nil
	}

	encoded, ok := decoded.(string)
	if !ok {
		return nil, fmt.Errorf("expected json text in column %s, got %T", constants.StringifiedData, decoded)
	}

	var row map[string]json.RawMessage
	if err := json.Unmarshal([]byte(encoded), &row); err != nil {
		return nil, fmt.Errorf("failed to parse stored row: %s", err)
	}

	raw, present := row[jsonKey]
	if !present {
		// key not in the stored row: keep the placeholder rather than write NULL
		return constants.UnavailableValue, nil
	}

	return raw, nil
}
