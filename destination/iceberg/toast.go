package iceberg

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"slices"
	"sync/atomic"

	"github.com/goccy/go-json"
	"google.golang.org/grpc"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination/iceberg/proto"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
)

// toastResolver fills columns Postgres left out of an UPDATE (unchanged TOAST) with the
// value from the row's previous version: from carry if this sync wrote the row, else read
// from the destination at the location the row index (pos or dv) gives.
type toastResolver struct {
	writer Writer
	// reader reads stored values out of data files (Java side).
	reader   proto.TableIndexServiceClient
	threadID string
	stream   types.StreamInterface

	tracked    map[string]struct{}  // columns seen marked in this thread
	carry      map[string]*carryRow // olake id -> values this thread wrote
	carryBytes int64
	batch      int64 // batches resolved, to tell recently written rows from old ones

	recovered  int64 // values filled into records
	fromTable  int64 // values read from the destination
	unresolved int64 // values left as the placeholder
}

const (
	// maxCarryBytes caps the memory, across all streams, used to keep values written in this
	// sync. Past it they are dropped and read back from the destination if needed.
	maxCarryBytes = int64(128) * 1024 * 1024 // 128 MB
	// carryEntryOverhead approximates the memory cost of one carried row.
	carryEntryOverhead = int64(96)
)

// carryUsage is the carry memory held by all threads of this process. The budget is shared
// so that N streams cannot use N times maxCarryBytes.
var carryUsage atomic.Int64

// carryRow is what this thread last wrote for one row. deleted means nothing may be
// carried forward from it.
type carryRow struct {
	deleted bool
	values  map[string]any
	batch   int64 // last batch that wrote the row
}

// size is the carry memory the row is accounted for.
func (row *carryRow) size(olakeID string) int64 {
	size := int64(len(olakeID)) + carryEntryOverhead
	for _, value := range row.values {
		size += valueSize(value)
	}
	return size
}

// pendingRead is one stored row to read: the row it is the previous version of, and the
// source columns wanted from it.
type pendingRead struct {
	olakeID string
	columns []string
}

func newToastResolver(threadID string, stream types.StreamInterface, writer Writer, reader proto.TableIndexServiceClient) *toastResolver {
	return &toastResolver{
		writer:   writer,
		reader:   reader,
		threadID: threadID,
		stream:   stream,
		tracked:  make(map[string]struct{}),
		carry:    make(map[string]*carryRow),
	}
}

// Resolve fills every unavailable value in the batch it can recover. The rest keep
// constants.UnavailableValue, as before this feature.
func (r *toastResolver) Resolve(ctx context.Context, records []types.RawRecord) error {
	if !r.track(records) {
		return nil
	}
	r.batch++

	if err := r.readMissing(ctx, records); err != nil {
		return err
	}

	// Records are applied in order on top of carry, which now holds each row's state from
	// before the batch, so every record sees exactly what the changes before it wrote.
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
				if value, carried := row.values[column]; carried {
					record.Data[column] = value
					r.recovered++
					continue
				}
			}
			r.unresolved++
		}

		// Remember the values this record does carry, for the next change to the row.
		for column := range r.tracked {
			if value, present := record.Data[column]; present && !isUnavailable(value) {
				r.remember(olakeID, column, value)
			}
		}
	}

	r.trim()

	return nil
}

// readMissing reads from the destination the values carry cannot answer and adds them to
// carry. Only a row's first record in the batch can need a read: every later record of the
// row is answered by what the earlier ones wrote.
func (r *toastResolver) readMissing(ctx context.Context, records []types.RawRecord) error {
	seen := make(map[string]struct{})
	reads := make(map[types.RowLocation]*pendingRead)

	for idx := range records {
		record := &records[idx]
		olakeID := record.OlakeColumns[constants.OlakeID].(string)
		if _, done := seen[olakeID]; done {
			continue
		}
		seen[olakeID] = struct{}{}

		row := r.carry[olakeID]
		if row != nil && row.deleted {
			continue // deleted earlier in this sync: old values must not come back
		}

		var missing []string
		for _, column := range record.UnavailableColumns {
			if _, selected := record.Data[column]; !selected {
				continue
			}
			if row != nil {
				if _, carried := row.values[column]; carried {
					continue
				}
			}
			missing = append(missing, column)
		}
		if len(missing) == 0 {
			continue
		}

		location, found, err := r.writer.Lookup(olakeID)
		if err != nil {
			return fmt.Errorf("failed to look up row[%s] in index: %w", olakeID, err)
		}
		if !found {
			continue // no previous version: a filtered row, or the UPDATE changed the primary key
		}
		reads[location] = &pendingRead{olakeID: olakeID, columns: missing}
	}

	if len(reads) == 0 {
		return nil
	}

	return r.read(ctx, reads)
}

// track adds this batch's marked columns to r.tracked and reports whether there is work.
// Once a column is tracked, every later batch is processed so carry stays up to date,
// even batches with no marker.
func (r *toastResolver) track(records []types.RawRecord) bool {
	for idx := range records {
		for _, column := range records[idx].UnavailableColumns {
			r.tracked[column] = struct{}{}
		}
	}

	return len(r.tracked) > 0
}

// readColumns is what to read of one stored row: _op_type, to spot a soft-deleted row (an
// UPDATE can move a row onto a key deleted in an earlier sync), then the destination columns
// holding the wanted source columns. Without normalization that is the one column holding
// the whole row as JSON.
func (r *toastResolver) readColumns(columns []string) []string {
	if !r.stream.NormalizationEnabled() {
		return []string{constants.OpType, constants.StringifiedData}
	}

	read := make([]string, 0, len(columns)+1)
	read = append(read, constants.OpType)
	for _, column := range columns {
		read = append(read, r.stream.ResolveColumnName(column))
	}
	return read
}

// read reads the pending rows in one streamed call and adds their values to carry. Values
// that cannot be read are left out, so the record keeps the placeholder.
func (r *toastResolver) read(ctx context.Context, reads map[types.RowLocation]*pendingRead) error {
	rows := make(map[string][]*proto.ReadRowsRequest_Row) // data file -> rows to read in it
	for location, pending := range reads {
		rows[location.FilePath] = append(rows[location.FilePath], &proto.ReadRowsRequest_Row{
			Position: location.Position,
			Columns:  r.readColumns(pending.columns),
		})
	}

	paths := slices.Sorted(maps.Keys(rows))
	// a file still being written has no footer and cannot be read: the arrow writer uploads
	// its own here, Java closes the legacy writer's before serving ReadRows
	if err := r.writer.EnsureReadable(ctx, paths); err != nil {
		return err
	}

	request := &proto.ReadRowsRequest{ThreadId: r.threadID}
	for _, path := range paths {
		// Java reads a file front to back, so it takes the rows in position order
		slices.SortFunc(rows[path], func(a, b *proto.ReadRowsRequest_Row) int {
			return cmp.Compare(a.GetPosition(), b.GetPosition())
		})
		request.Files = append(request.Files, &proto.ReadRowsRequest_FileRows{FilePath: path, Rows: rows[path]})
	}

	// The limit applies per streamed message; the 4 MB default is too small for one large
	// value. Java keeps each message to 64 rows or 16 MB.
	stream, err := r.reader.ReadRows(ctx, request, grpc.MaxCallRecvMsgSize(math.MaxInt32))
	if err != nil {
		return fmt.Errorf("failed to read unavailable column values: %w", err)
	}

	for {
		batch, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("failed to read unavailable column values: %w", err)
		}

		for _, row := range batch.GetRows() {
			pending := reads[types.RowLocation{FilePath: row.GetFilePath(), Position: row.GetPosition()}]
			if opType, _ := fieldValue(row.GetValues()[constants.OpType]).(string); opType == "d" {
				continue
			}

			values, err := r.rowValues(row.GetValues(), pending.columns)
			if err != nil {
				return fmt.Errorf("failed to read row %d of %s: %w", row.GetPosition(), row.GetFilePath(), err)
			}
			for column, value := range values {
				if isUnavailable(value) {
					continue // the stored version has the placeholder too
				}
				r.remember(pending.olakeID, column, value)
				r.fromTable++
			}
		}
	}
}

// rowValues picks the wanted source columns out of one stored row, keyed by destination
// column. A column the data file does not carry is left out, so the record keeps the
// placeholder. Without normalization the row is one JSON document, parsed once, and each
// column's raw JSON is returned.
func (r *toastResolver) rowValues(stored map[string]*proto.IcebergPayload_IceRecord_FieldValue, columns []string) (map[string]any, error) {
	values := make(map[string]any, len(columns))
	if r.stream.NormalizationEnabled() {
		for _, column := range columns {
			if value, present := stored[r.stream.ResolveColumnName(column)]; present {
				values[column] = fieldValue(value)
			}
		}
		return values, nil
	}

	encoded, _ := fieldValue(stored[constants.StringifiedData]).(string)
	var row map[string]json.RawMessage
	if err := json.Unmarshal([]byte(encoded), &row); err != nil {
		return nil, fmt.Errorf("failed to parse stored row: %s", err)
	}
	for _, column := range columns {
		// a key missing from the stored row keeps the placeholder rather than become NULL
		if raw, present := row[column]; present {
			values[column] = raw
		}
	}

	return values, nil
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
	row.batch = r.batch
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

	r.carry[olakeID] = &carryRow{deleted: true, batch: r.batch}
}

// account adds delta carried bytes to this thread and to the process total.
func (r *toastResolver) account(delta int64) {
	r.carryBytes += delta
	carryUsage.Add(delta)
}

// trim brings the process back to half the budget once it is over, dropping this thread's
// least recently written rows first. Nothing is lost: the index still locates every dropped
// row, so its next change reads it back. Old rows go first because they sit in data files
// already closed, which costs just a read; a recent row may sit in a file still open, which
// the read would have to close early. Halving keeps trims rare.
func (r *toastResolver) trim() {
	if carryUsage.Load() <= maxCarryBytes {
		return
	}

	olakeIDs := slices.SortedFunc(maps.Keys(r.carry), func(a, b string) int {
		return cmp.Compare(r.carry[a].batch, r.carry[b].batch)
	})
	dropped := 0
	for _, olakeID := range olakeIDs {
		if carryUsage.Load() <= maxCarryBytes/2 {
			break
		}
		r.account(-r.carry[olakeID].size(olakeID))
		delete(r.carry, olakeID)
		dropped++
	}

	logger.Debugf("Thread[%s]: dropped %d of %d carried row(s), %d bytes left", r.threadID, dropped, dropped+len(r.carry), r.carryBytes)
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

	if r.recovered+r.unresolved == 0 {
		return
	}

	logger.Infof("Thread[%s]: recovered %d unavailable column value(s), read %d from the destination table, %d could not be recovered",
		r.threadID, r.recovered, r.fromTable, r.unresolved)
}

func valueSize(value any) int64 {
	switch typed := value.(type) {
	case nil:
		return 0
	case string:
		return int64(len(typed))
	case json.RawMessage:
		return int64(len(typed))
	case []any: // Postgres arrays
		size := int64(0)
		for _, element := range typed {
			size += valueSize(element)
		}
		return size
	default:
		return 16
	}
}

func isUnavailable(value any) bool {
	text, ok := value.(string)
	return ok && text == constants.UnavailableValue
}

// fieldValue converts one stored value for the record; unset means NULL. The columns read
// are text (Postgres text, json, arrays and the like, _op_type, the stored row) or numeric
// stored as double.
func fieldValue(value *proto.IcebergPayload_IceRecord_FieldValue) any {
	switch stored := value.GetValue().(type) {
	case *proto.IcebergPayload_IceRecord_FieldValue_StringValue:
		return stored.StringValue
	case *proto.IcebergPayload_IceRecord_FieldValue_DoubleValue:
		return stored.DoubleValue
	default:
		return nil
	}
}
