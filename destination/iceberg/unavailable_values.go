package iceberg

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"

	"github.com/goccy/go-json"
	"google.golang.org/grpc"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination/iceberg/proto"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
)

// unavailableValuesResolver fills columns the source could not send (Postgres unchanged TOAST)
// with the row's previous value. known holds the last value this thread wrote or read for a row;
// a row it lacks is read from the destination at the location the stream index (pos or dv) gives.
type unavailableValuesResolver struct {
	// writer closes a data file it still has open before rows are read from it (an open file has
	// no footer); the legacy writer's are closed by Java when ReadRows asks for them.
	writer Writer
	// index locates a row's newest version: this thread's uncommitted writes first, then the
	// committed index.
	index *types.StreamIndexThread
	// reader reads stored values out of data files (Java side).
	reader   proto.TableIndexServiceClient
	threadID string
	stream   types.StreamInterface

	// tracked lists the synced columns ever marked unavailable in this thread; only these are
	// kept in known.
	tracked map[string]struct{}
	// known is a row's last value of each tracked column, written by this thread or read from the
	// destination; nil for a row deleted in this sync. Past maxKnownBytes it is cleared, and those
	// rows are read back from the destination on their next change.
	known      map[string]map[string]any
	knownBytes int64

	recovered  int64 // values filled into records
	fromTable  int64 // values read from the destination
	unresolved int64 // values left as the placeholder
}

const (
	// maxKnownBytes caps the values a thread keeps in known.
	maxKnownBytes = int64(64) * 1024 * 1024 // 64 MB
	// knownRowOverhead approximates what a row in known costs besides its id and values: its map
	// entry and values map, about 400 bytes for a row of up to 8 values (Go 1.26).
	knownRowOverhead = int64(400)
	// readRows caps the rows one ReadRows call asks for. Java loads the pages of all the rows
	// asked in a row group at once, so this bounds its memory before max_bytes can stop it.
	readRows = 64
)

// unavailableJSON is the placeholder as the stored JSON row (normalization off) holds it.
const unavailableJSON = `"` + constants.UnavailableValue + `"`

// pendingRead is one stored row to read: the row it is the previous version of, where that
// version is stored, and the source columns wanted from it.
type pendingRead struct {
	olakeID  string
	location types.RowLocation
	columns  []string
}

func newUnavailableValuesResolver(threadID string, stream types.StreamInterface, writer Writer, index *types.StreamIndexThread, reader proto.TableIndexServiceClient) *unavailableValuesResolver {
	return &unavailableValuesResolver{
		writer:   writer,
		index:    index,
		reader:   reader,
		threadID: threadID,
		stream:   stream,
		tracked:  make(map[string]struct{}),
		known:    make(map[string]map[string]any),
	}
}

// Resolve fills the unavailable values of a prefix of records and returns its length (at least 1).
// The prefix ends at a record whose row a read could not get within limit bytes, or before its
// values pass limit; only a lone first record may pass it.
func (r *unavailableValuesResolver) Resolve(ctx context.Context, records []types.RawRecord, limit int64) (int, error) {
	r.track(records)
	// no synced column was ever marked in this thread: nothing to fill, nothing to keep
	if len(r.tracked) == 0 {
		return len(records), nil
	}
	defer r.trim()

	size := int64(0)
	for idx := range records {
		record := &records[idx]
		if len(r.missing(record)) > 0 {
			// reads the rows of the next records too, so most records find their values ready
			if err := r.read(ctx, records[idx:], limit-size); err != nil {
				return 0, err
			}
		}
		if len(r.missing(record)) > 0 {
			if idx > 0 {
				return idx, nil // the next call goes on from this record
			}
			// the first record must move on: read its row alone, whatever its size
			if err := r.read(ctx, records[:1], math.MaxInt64); err != nil {
				return 0, err
			}
			// a row the read still did not return (not in its file) keeps the placeholder
			for _, column := range r.missing(record) {
				r.keep(record.OlakeColumns[constants.OlakeID].(string), column, constants.UnavailableValue)
			}
		}

		recordSize := r.filledSize(record)
		if idx > 0 && size+recordSize > limit {
			return idx, nil
		}
		r.fill(record)
		size += recordSize
	}

	return len(records), nil
}

// previous is the row's value of column before the record. A row deleted earlier in this sync
// gives the placeholder, so its old values never come back.
func (r *unavailableValuesResolver) previous(olakeID, column string) (any, bool) {
	row, found := r.known[olakeID]
	if found && row == nil {
		return constants.UnavailableValue, true
	}
	value, found := row[column]
	return value, found
}

// missing lists the record's unavailable columns known has no value for yet.
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
		r.remember(olakeID, nil)
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
	// The placeholder is remembered too: the destination has nothing better for the row, so its
	// next change must not read it again.
	r.remember(olakeID, record.Data)
}

// remember keeps the tracked columns a record writes for its row; nil data marks the row deleted.
func (r *unavailableValuesResolver) remember(olakeID string, data map[string]any) {
	r.update(olakeID, func(row map[string]any) map[string]any {
		if data == nil {
			return nil
		}
		for column := range r.tracked {
			if value, present := data[column]; present {
				row[column] = value
			}
		}
		return row
	})
}

// keep stores a value read for a row's column. A value can come twice when two source columns
// share one destination column (Foo and foo), so it replaces the old.
func (r *unavailableValuesResolver) keep(olakeID, column string, value any) {
	r.update(olakeID, func(row map[string]any) map[string]any {
		row[column] = value
		return row
	})
}

// update applies change to the row in known, keeping knownBytes in step. change gets the row's
// values (a new map for a row known lacks or has as deleted) and returns them, nil to mark the
// row deleted.
func (r *unavailableValuesResolver) update(olakeID string, change func(map[string]any) map[string]any) {
	row, found := r.known[olakeID]
	if found {
		r.knownBytes -= rowSize(olakeID, row)
	}
	if row == nil {
		row = make(map[string]any, len(r.tracked))
	}
	row = change(row)
	r.known[olakeID] = row
	r.knownBytes += rowSize(olakeID, row)
}

// trim clears known once it passes maxKnownBytes. Its rows are read back from the destination
// on their next change; a row still in an open data file closes that file early, once per file.
func (r *unavailableValuesResolver) trim() {
	if r.knownBytes <= maxKnownBytes {
		return
	}

	logger.Debugf("Thread[%s]: cleared %d known row(s), %d bytes", r.threadID, len(r.known), r.knownBytes)
	clear(r.known)
	r.knownBytes = 0
}

// read reads into known, in one call of at most readRows rows, the values the records are missing. Only a
// row's first record can need a read: every later record of the row is answered by what the
// earlier ones wrote. Java ends the stream after the row that takes the bytes it sent past
// budget; the rows it did not get to are read by a later call.
func (r *unavailableValuesResolver) read(ctx context.Context, records []types.RawRecord, budget int64) error {
	seen := make(map[string]struct{})
	var reads []*pendingRead
	for idx := range records {
		if len(reads) == readRows {
			break
		}
		record := &records[idx]
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
				r.keep(olakeID, column, constants.UnavailableValue)
			}
			continue
		}
		reads = append(reads, &pendingRead{olakeID: olakeID, location: location, columns: missing})
	}

	if len(reads) == 0 {
		return nil
	}

	return r.readRows(ctx, reads, budget)
}

// readRows streams the pending rows from Java into known.
func (r *unavailableValuesResolver) readRows(ctx context.Context, reads []*pendingRead, budget int64) error {
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

	if err := r.writer.EnsureReadable(ctx, paths); err != nil {
		return err
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
			if err := r.storeRow(pending, row.GetValues()); err != nil {
				return fmt.Errorf("failed to read row %d of %s: %w", row.GetPosition(), row.GetFilePath(), err)
			}
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

// storeRow keeps the wanted values of one stored row, or of one part of a row Java split. NULL
// stays the placeholder: Postgres never leaves out a NULL, so a stored NULL means the value was
// never written. Java sends NULL for a soft-deleted row and for a column the file lacks.
func (r *unavailableValuesResolver) storeRow(pending *pendingRead, stored map[string]*proto.IcebergPayload_IceRecord_FieldValue) error {
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
			r.store(pending.olakeID, column, value)
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
		r.store(pending.olakeID, column, value)
	}
	return nil
}

// store keeps a value read from the destination.
func (r *unavailableValuesResolver) store(olakeID, column string, value any) {
	if value != constants.UnavailableValue {
		r.fromTable++
	}
	r.keep(olakeID, column, value)
}

// track drops the marked columns the stream does not sync and adds the rest to the columns this
// thread keeps in known.
func (r *unavailableValuesResolver) track(records []types.RawRecord) {
	for idx := range records {
		record := &records[idx]
		record.UnavailableColumns = slices.DeleteFunc(record.UnavailableColumns, func(column string) bool {
			_, exist := record.Data[column]
			return !exist
		})
		for _, column := range record.UnavailableColumns {
			r.tracked[column] = struct{}{}
		}
	}
}

// Close releases known and logs what this thread recovered. It frees the values now, because
// this writer's commit and other streams' commits run after it.
func (r *unavailableValuesResolver) Close() {
	r.known, r.knownBytes = nil, 0

	if r.recovered+r.unresolved == 0 {
		return
	}

	logger.Infof("Thread[%s]: recovered %d unavailable column value(s), read %d from the destination table, %d could not be recovered",
		r.threadID, r.recovered, r.fromTable, r.unresolved)
}

// rowSize is the memory a row in known counts for: its id, a fixed cost for its entry and map,
// and its values' bytes.
func rowSize(olakeID string, row map[string]any) int64 {
	return int64(len(olakeID)) + knownRowOverhead + dataSize(row)
}

func dataSize(data map[string]any) int64 {
	size := int64(0)
	for _, value := range data {
		size += valueSize(value)
	}
	return size
}

// valueSize is roughly the bytes a value holds: text by its length, numbers, booleans and
// times as 16. NULL and a missing value count as 0.
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
