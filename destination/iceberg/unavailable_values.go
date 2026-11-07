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

// unavailableValuesResolver fills columns the source could not send (Postgres unchanged TOAST)
// with the value from the row's previous version: from carry if this sync wrote the row,
// else read from the destination at the location the row index (pos or dv) gives.
type unavailableValuesResolver struct {
	// openFiles uploads data files the writer still has open on the Go side (arrow), so
	// their rows can be read; nil for the legacy writer, whose files Java closes itself
	// when ReadRows asks for them.
	openFiles openFileWriter
	// index locates a row's newest version: this thread's uncommitted writes first, then
	// the committed index.
	index *types.StreamIndexThread
	// reader reads stored values out of data files (Java side).
	reader   proto.TableIndexServiceClient
	threadID string
	stream   types.StreamInterface

	tracked    map[string]struct{}  // synced columns seen marked in this thread
	carry      map[string]*carryRow // olake id -> values this thread wrote
	carryBytes int64
	writes     int64 // counts carry writes, to tell recently written rows from old ones

	// fetched holds the values read from the destination for rows whose record is not filled
	// yet, the placeholder where the stored row has none. Its bytes count against Resolve's limit.
	fetched      map[string]map[string]any // olake id -> column -> value
	fetchedBytes int64

	recovered  int64 // values filled into records
	fromTable  int64 // values read from the destination
	unresolved int64 // values left as the placeholder

	readRows  int64 // rows the last read got in full, and all the bytes it read, to size the next read
	readBytes int64
}

const (
	// maxCarryBytes is split equally among the threads carrying values written in this sync; past its
	// share a thread drops its oldest rows, read back from the destination if needed. A thread that
	// goes idle keeps its old share until its next batch, so the total can pass this for a while.
	maxCarryBytes = int64(128) * 1024 * 1024 // 128 MB
	// carryEntryOverhead and carryValueOverhead approximate the memory a carried row costs besides
	// its id and value bytes: measured at about 400 bytes for a row of up to 8 values (Go 1.26).
	carryEntryOverhead = int64(384)
	carryValueOverhead = int64(24)
	// firstReadRows caps a thread's first read, before it knows how big its rows are. Java loads the
	// pages of all asked rows in a row group at once, so a smaller first read keeps its memory low.
	firstReadRows = 64
)

// unavailableJSON is the placeholder as the stored JSON row (normalization off) holds it.
const unavailableJSON = `"` + constants.UnavailableValue + `"`

// carriers counts the threads carrying values. Each keeps to an equal share of maxCarryBytes, so N
// streams do not use N times the budget; an idle thread trims to a new share on its next batch.
var carriers atomic.Int64

// carryRow is what this thread last wrote for one row. deleted means nothing may be
// carried forward from it.
type carryRow struct {
	deleted bool
	values  map[string]any
	written int64 // order the row was last written in
}

// size is the memory the row counts for in carry: its id, a fixed cost per row for its carry
// entry, carryRow and values map (carryEntryOverhead), its values' bytes (dataSize) and a fixed
// cost per value (carryValueOverhead). remember counts the same parts as it stores values.
func (row *carryRow) size(olakeID string) int64 {
	return int64(len(olakeID)) + carryEntryOverhead + dataSize(row.values) + int64(len(row.values))*carryValueOverhead
}

// pendingRead is one stored row to read: the row it is the previous version of, where that
// version is stored, and the source columns wanted from it.
type pendingRead struct {
	olakeID  string
	location types.RowLocation
	columns  []string
}

// openFileWriter is a writer that holds data files open on the Go side.
type openFileWriter interface {
	// EnsureReadable closes any file in paths the writer still has open, without
	// committing it, so its rows can be read (an open file has no footer).
	EnsureReadable(ctx context.Context, paths []string) error
}

func newUnavailableValuesResolver(threadID string, stream types.StreamInterface, writer Writer, index *types.StreamIndexThread, reader proto.TableIndexServiceClient) *unavailableValuesResolver {
	openFiles, _ := writer.(openFileWriter)
	return &unavailableValuesResolver{
		openFiles: openFiles,
		index:     index,
		reader:    reader,
		threadID:  threadID,
		stream:    stream,
		tracked:   make(map[string]struct{}),
		carry:     make(map[string]*carryRow),
		fetched:   make(map[string]map[string]any),
	}
}

// Resolve fills the unavailable values of a prefix of records and returns its length (at least 1).
// It reads once, from the first record missing a value, and ends the prefix at a row that read did
// not get or before the prefix passes limit bytes. Only a lone first record may pass the limit.
func (r *unavailableValuesResolver) Resolve(ctx context.Context, records []types.RawRecord, limit int64) (int, error) {
	r.track(records)
	// no synced column was ever marked in this thread: nothing to fill, nothing to carry
	if len(r.tracked) == 0 {
		return len(records), nil
	}
	defer r.trim()

	size, read := int64(0), false
	for idx := range records {
		record := &records[idx]
		missing := len(r.missing(record)) > 0
		if missing && !read {
			if err := r.readAhead(ctx, records[idx:], limit-size-r.fetchedBytes); err != nil {
				return 0, err
			}
			read, missing = true, len(r.missing(record)) > 0
		}
		// the first record is read alone, whatever its size, so each call moves on
		if missing && idx == 0 {
			if err := r.readAhead(ctx, records[:1], math.MaxInt64); err != nil {
				return 0, err
			}
		}

		recordSize := r.filledSize(record)
		if idx > 0 && (missing || size+recordSize > limit) {
			return idx, nil // the next call goes on from this record
		}
		r.fill(record)
		size += recordSize
	}

	return len(records), nil
}

// previous is the row's value of column before the record: what this thread last wrote for
// the row, else what was read for it. A row deleted earlier in this sync gives the placeholder,
// so its old values never come back.
func (r *unavailableValuesResolver) previous(olakeID, column string) (any, bool) {
	if row := r.carry[olakeID]; row != nil {
		if row.deleted {
			return constants.UnavailableValue, true
		}
		if value, found := row.values[column]; found {
			return value, true
		}
	}

	value, found := r.fetched[olakeID][column]
	return value, found
}

// filledSize is the size of the record's values once fill has filled them in.
func (r *unavailableValuesResolver) filledSize(record *types.RawRecord) int64 {
	olakeID := record.OlakeColumns[constants.OlakeID].(string)
	size := dataSize(record.Data)
	for _, column := range record.UnavailableColumns {
		if value, found := r.previous(olakeID, column); found && value != constants.UnavailableValue {
			size += valueSize(value) - valueSize(record.Data[column])
		}
	}
	return size
}

// fill fills the record's unavailable columns with the row's previous values, then remembers
// what the record writes for the next change to the row.
func (r *unavailableValuesResolver) fill(record *types.RawRecord) {
	olakeID := record.OlakeColumns[constants.OlakeID].(string)
	if record.OlakeColumns[constants.OpType].(string) == "d" {
		r.tombstone(olakeID)
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
	// what was read for the row is in the record now, and goes to carry below
	r.release(olakeID)

	// The placeholder is remembered too: the destination has nothing better for the row, so its
	// next change must not read it again.
	for column := range r.tracked {
		if value, present := record.Data[column]; present {
			r.remember(olakeID, column, value)
		}
	}
}

// missing lists the record's unavailable columns that neither carry nor a read answers yet.
func (r *unavailableValuesResolver) missing(record *types.RawRecord) []string {
	olakeID := record.OlakeColumns[constants.OlakeID].(string)
	var missing []string
	for _, column := range record.UnavailableColumns {
		if _, found := r.previous(olakeID, column); !found {
			missing = append(missing, column)
		}
	}
	return missing
}

// readAhead reads the values the next records are missing, as many rows as the last read
// suggests fit in budget with the records' own values. Only a row's first record can need a
// read: every later record of the row is answered by what the earlier ones wrote.
func (r *unavailableValuesResolver) readAhead(ctx context.Context, records []types.RawRecord, budget int64) error {
	if budget <= 0 {
		return nil // what was read ahead already fills the limit
	}

	rowBytes := int64(0) // what a read row brought on average; unknown before the first read
	if r.readRows > 0 {
		rowBytes = r.readBytes / r.readRows
	}

	size := int64(0)
	seen := make(map[string]struct{})
	var reads []*pendingRead
	for idx := range records {
		if len(reads) > 0 && (size >= budget || (r.readRows == 0 && len(reads) >= firstReadRows)) {
			break
		}
		record := &records[idx]
		size += dataSize(record.Data)
		olakeID := record.OlakeColumns[constants.OlakeID].(string)
		if _, done := seen[olakeID]; done {
			continue
		}
		seen[olakeID] = struct{}{}

		missing := r.missing(record)
		if len(missing) == 0 {
			continue
		}

		location, found, err := r.index.Lookup(olakeID)
		if err != nil {
			return fmt.Errorf("failed to look up row[%s] in index: %w", olakeID, err)
		}
		if !found {
			// no previous version: a filtered row, or the UPDATE changed the primary key
			for _, column := range missing {
				r.fetch(olakeID, column, constants.UnavailableValue)
			}
			continue
		}
		reads = append(reads, &pendingRead{olakeID: olakeID, location: location, columns: missing})
		size += rowBytes
	}

	if len(reads) == 0 {
		return nil
	}

	return r.read(ctx, reads, budget)
}

// track drops the marked columns the stream does not sync and adds the rest to the columns
// this thread carries.
func (r *unavailableValuesResolver) track(records []types.RawRecord) {
	for idx := range records {
		record := &records[idx]
		record.UnavailableColumns = slices.DeleteFunc(record.UnavailableColumns, func(column string) bool {
			_, selected := record.Data[column]
			return !selected
		})
		for _, column := range record.UnavailableColumns {
			if len(r.tracked) == 0 {
				carriers.Add(1)
			}
			r.tracked[column] = struct{}{}
		}
	}
}

// readColumns lists what to read of one stored row: _op_type, which Java checks to send a
// soft-deleted row's columns as NULL, then the columns holding the wanted values; without
// normalization, the one column holding the whole row as JSON.
func (r *unavailableValuesResolver) readColumns(columns []string) []string {
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

// read reads the pending rows in one streamed call into fetched. Java ends the stream after the
// row that takes the bytes it sent past budget; a row not read in full by then is read by a later call.
func (r *unavailableValuesResolver) read(ctx context.Context, reads []*pendingRead, budget int64) error {
	wanted := make(map[types.RowLocation]*pendingRead, len(reads))
	rows := make(map[string][]*proto.ReadRowsRequest_Row) // data file -> rows to read in it
	var paths []string                                    // in the order the records need them, which Java starts reading them in
	for _, pending := range reads {
		wanted[pending.location] = pending
		path := pending.location.FilePath
		if _, queued := rows[path]; !queued {
			paths = append(paths, path)
		}
		rows[path] = append(rows[path], &proto.ReadRowsRequest_Row{
			Position: pending.location.Position,
			Columns:  r.readColumns(pending.columns),
		})
	}

	// a file still being written has no footer and cannot be read: the arrow writer uploads
	// its own here, Java closes the legacy writer's before serving ReadRows
	if r.openFiles != nil {
		if err := r.openFiles.EnsureReadable(ctx, paths); err != nil {
			return err
		}
	}

	request := &proto.ReadRowsRequest{ThreadId: r.threadID, MaxBytes: budget}
	for _, path := range paths {
		// Java reads a file front to back, so it takes the rows in position order
		slices.SortFunc(rows[path], func(a, b *proto.ReadRowsRequest_Row) int {
			return cmp.Compare(a.GetPosition(), b.GetPosition())
		})
		request.Files = append(request.Files, &proto.ReadRowsRequest_FileRows{FilePath: path, Rows: rows[path]})
	}

	// canceled on return, which stops Java's reads when this side fails before the stream ends
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// The limit applies per streamed message; the 4 MB default is too small for one large
	// value. Java keeps each message to 64 rows or 16 MB.
	stream, err := r.reader.ReadRows(ctx, request, grpc.MaxCallRecvMsgSize(math.MaxInt32))
	if err != nil {
		return fmt.Errorf("failed to read unavailable column values: %w", err)
	}

	start, done := r.fetchedBytes, int64(0) // done: rows read in full by this call
	defer func() {
		if done > 0 { // the next window is sized from this read alone
			r.readRows, r.readBytes = done, r.fetchedBytes-start
		}
	}()
	for {
		batch, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil // every row asked has arrived, or as many as budget allowed
		}
		if err != nil {
			return fmt.Errorf("failed to read unavailable column values: %w", err)
		}

		for _, row := range batch.GetRows() {
			pending := wanted[types.RowLocation{FilePath: row.GetFilePath(), Position: row.GetPosition()}]
			if err := r.fetchRow(pending, row.GetValues()); err != nil {
				return fmt.Errorf("failed to read row %d of %s: %w", row.GetPosition(), row.GetFilePath(), err)
			}
			// a row over 16 MB comes in parts, each with some of its columns
			if r.fetchedAll(pending) {
				done++
			}
		}
	}
}

// fetchedAll reports whether every column the read asked of the row has arrived.
func (r *unavailableValuesResolver) fetchedAll(pending *pendingRead) bool {
	for _, column := range pending.columns {
		if _, found := r.fetched[pending.olakeID][column]; !found {
			return false
		}
	}
	return true
}

// fetchRow keeps the wanted values of one stored row, or one part of a split row, until fill uses
// them. NULL stays the placeholder: Postgres never leaves out a NULL, so a stored NULL means the
// value was never written. Java sends NULL for a soft-deleted row and for a column the file lacks.
func (r *unavailableValuesResolver) fetchRow(pending *pendingRead, stored map[string]*proto.IcebergPayload_IceRecord_FieldValue) error {
	if r.stream.NormalizationEnabled() {
		for _, column := range pending.columns {
			field, present := stored[r.stream.ResolveColumnName(column)]
			if !present {
				continue // in another part of the row
			}
			value := fieldValue(field)
			if value == nil {
				value = constants.UnavailableValue
			}
			r.fetch(pending.olakeID, column, value)
		}
		return nil
	}

	// without normalization the row is one JSON document; a key missing from it predates the column
	field, present := stored[constants.StringifiedData]
	if !present {
		return nil // in another part of the row
	}
	var row map[string]json.RawMessage
	if encoded, _ := fieldValue(field).(string); encoded != "" {
		if err := json.Unmarshal([]byte(encoded), &row); err != nil {
			return fmt.Errorf("failed to parse stored row: %s", err)
		}
	}
	for _, column := range pending.columns {
		var value any = constants.UnavailableValue
		if raw, found := row[column]; found && string(raw) != "null" && string(raw) != unavailableJSON {
			value = raw
		}
		r.fetch(pending.olakeID, column, value)
	}
	return nil
}

// fetch keeps a value read for a row until fill writes it into the row's record. A value can come
// twice when two source columns share one destination column (Foo and foo), so it replaces the old.
func (r *unavailableValuesResolver) fetch(olakeID, column string, value any) {
	if value != constants.UnavailableValue {
		r.fromTable++
	}
	values := r.fetched[olakeID]
	if values == nil {
		values = make(map[string]any, len(r.tracked))
		r.fetched[olakeID] = values
	}
	r.fetchedBytes += valueSize(value) - valueSize(values[column])
	values[column] = value
}

// release drops what was read for a row once fill has written it into the row's record.
func (r *unavailableValuesResolver) release(olakeID string) {
	r.fetchedBytes -= dataSize(r.fetched[olakeID])
	delete(r.fetched, olakeID)
}

// remember stores the newest value this thread wrote for a row's column.
func (r *unavailableValuesResolver) remember(olakeID, column string, value any) {
	row := r.carry[olakeID]
	if row == nil {
		r.carryBytes += int64(len(olakeID)) + carryEntryOverhead // a deleted row's entry is counted already
	}
	if row == nil || row.deleted {
		row = &carryRow{values: make(map[string]any, len(r.tracked))}
		r.carry[olakeID] = row
	}

	previous, found := row.values[column]
	r.carryBytes += valueSize(value) - valueSize(previous)
	if !found {
		r.carryBytes += carryValueOverhead
	}
	row.values[column] = value
	r.writes++
	row.written = r.writes
}

// tombstone marks a row deleted and releases the values it carried.
func (r *unavailableValuesResolver) tombstone(olakeID string) {
	if row := r.carry[olakeID]; row != nil {
		r.carryBytes -= row.size(olakeID)
	}
	r.writes++
	row := &carryRow{deleted: true, written: r.writes}
	r.carry[olakeID] = row
	r.carryBytes += row.size(olakeID)
}

// trim drops this thread's oldest carried rows once it is over its share, down to half of it.
// A dropped row is read back on its next change; old rows go first as they sit in closed files,
// while a recent one may sit in a file still open. Halving keeps trims rare.
func (r *unavailableValuesResolver) trim() {
	share := maxCarryBytes / carriers.Load()
	if r.carryBytes <= share {
		return
	}

	olakeIDs := slices.SortedFunc(maps.Keys(r.carry), func(a, b string) int {
		return cmp.Compare(r.carry[a].written, r.carry[b].written)
	})
	for _, olakeID := range olakeIDs {
		if r.carryBytes <= share/2 {
			break
		}
		r.carryBytes -= r.carry[olakeID].size(olakeID)
		delete(r.carry, olakeID)
	}

	logger.Debugf("Thread[%s]: dropped %d of %d carried row(s), %d bytes left", r.threadID, len(olakeIDs)-len(r.carry), len(olakeIDs), r.carryBytes)
}

// Close gives back this thread's share of the carry budget and logs what it recovered. It frees
// the carried values now, because this writer's commit and other streams' commits run after it.
func (r *unavailableValuesResolver) Close() {
	if len(r.tracked) > 0 {
		carriers.Add(-1)
	}
	r.tracked, r.carry, r.fetched = nil, nil, nil

	if r.recovered+r.unresolved == 0 {
		return
	}

	logger.Infof("Thread[%s]: recovered %d unavailable column value(s), read %d from the destination table, %d could not be recovered",
		r.threadID, r.recovered, r.fromTable, r.unresolved)
}

func dataSize(data map[string]any) int64 {
	size := int64(0)
	for _, value := range data {
		size += valueSize(value)
	}
	return size
}

// valueSize is roughly the bytes a value holds: text by its length, numbers, booleans and
// times as 16. NULL and a missing value count as 0, which remember relies on.
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

// fieldValue converts one stored value for the record; unset means NULL. The columns read are
// text (Postgres text, json, arrays and the like, _op_type, the stored row), which the reader
// sends as bytes when it is not UTF-8, or numeric stored as double.
func fieldValue(value *proto.IcebergPayload_IceRecord_FieldValue) any {
	switch stored := value.GetValue().(type) {
	case *proto.IcebergPayload_IceRecord_FieldValue_StringValue:
		return stored.StringValue
	case *proto.IcebergPayload_IceRecord_FieldValue_BytesValue:
		return string(stored.BytesValue)
	case *proto.IcebergPayload_IceRecord_FieldValue_DoubleValue:
		return stored.DoubleValue
	default:
		return nil
	}
}
