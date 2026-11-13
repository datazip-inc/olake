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
)

// firstReadRows caps a thread's first read, before it knows how big its rows are. Java loads the pages of all
// asked rows in a row group at once, so a smaller first read keeps its memory low.
const firstReadRows = 64

// unavailableJSON is the placeholder as the stored JSON row (normalization off) holds it.
const unavailableJSON = `"` + constants.UnavailableValue + `"`

// openFileWriter is a writer that holds data files open on the Go side.
type openFileWriter interface {
	// EnsureReadable closes any file in paths the writer still has open, without committing it, so its rows
	// can be read (an open file has no footer).
	EnsureReadable(ctx context.Context, paths []string) error
}

// pendingRead is one stored row to read: the row it is the previous version of, where that version is
// stored, and the source columns wanted from it.
type pendingRead struct {
	olakeID  string
	location types.RowLocation
	columns  []string
}

// readSize is how many rows a read got in full and how many bytes they brought.
type readSize struct {
	rows  int64
	bytes int64
}

// averageRowBytes is what one row brought on average, 0 before any read got a row in full.
func (s readSize) averageRowBytes() int64 {
	if s.rows == 0 {
		return 0
	}
	return s.bytes / s.rows
}

// readAhead reads the previous versions of the rows the records are missing values of, as many as fit in
// budget. Only a row's first record can be missing values: later ones get what the earlier ones wrote.
func (r *unavailableValuesResolver) readAhead(ctx context.Context, records []types.RawRecord, budget int64) error {
	if budget <= 0 {
		return nil // the read buffer already fills the budget
	}
	rows, err := r.rowsToRead(records, budget)
	if err != nil || len(rows) == 0 {
		return err
	}
	return r.read(ctx, rows, budget)
}

// rowsToRead lists where the previous versions of the records' rows are stored, until the read is full. A row
// with no previous version gets the placeholder: a filtered row, or an UPDATE that changed the primary key.
func (r *unavailableValuesResolver) rowsToRead(records []types.RawRecord, budget int64) ([]*pendingRead, error) {
	var rows []*pendingRead
	size := int64(0) // the records' own bytes, plus what their rows to read are expected to bring
	seen := make(map[string]struct{})
	for idx := range records {
		if len(rows) > 0 && (size >= budget || (r.lastRead.rows == 0 && len(rows) >= firstReadRows)) {
			break
		}
		record := &records[idx]
		size += dataSize(record.Data)
		olakeID := olakeIDOf(record)
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
			return nil, fmt.Errorf("failed to look up row[%s] in index: %w", olakeID, err)
		}
		if !found {
			for _, column := range missing {
				r.fetch(olakeID, column, constants.UnavailableValue)
			}
			continue
		}
		rows = append(rows, &pendingRead{olakeID: olakeID, location: location, columns: missing})
		size += r.lastRead.averageRowBytes()
	}
	return rows, nil
}

// read reads the pending rows in one streamed call into the read buffer. Java ends the stream after the row that
// takes the bytes it sent past budget; a row not read in full by then is read by a later call.
func (r *unavailableValuesResolver) read(ctx context.Context, rows []*pendingRead, budget int64) error {
	request, wanted := r.readRequest(rows, budget)
	if r.writer != nil {
		paths := make([]string, 0, len(request.Files))
		for _, file := range request.Files {
			paths = append(paths, file.GetFilePath())
		}
		if err := r.writer.EnsureReadable(ctx, paths); err != nil {
			return err
		}
	}

	ctx, cancel := context.WithCancel(ctx) // canceled on return, which stops Java's reads if this side fails first
	defer cancel()
	// The limit applies per streamed message; the 4 MB default is too small for one large value. Java keeps each
	// message to 64 rows or 16 MB.
	stream, err := r.reader.ReadRows(ctx, request, grpc.MaxCallRecvMsgSize(math.MaxInt32))
	if err != nil {
		return fmt.Errorf("failed to read unavailable column values: %w", err)
	}

	start, done := r.readBuffer.bytes, int64(0) // done: rows read in full by this call
	defer func() {
		if done > 0 { // the next read is sized from this one alone
			r.lastRead = readSize{rows: done, bytes: r.readBuffer.bytes - start}
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
			if r.readBuffer.hasAll(pending.olakeID, pending.columns) { // a row over 16 MB comes in parts
				done++
			}
		}
	}
}

// readRequest asks for the rows file by file, the files in the order the records need them (Java starts reading
// them in that order) and each file's rows in position order. wanted maps a returned row to its pending read.
func (r *unavailableValuesResolver) readRequest(rows []*pendingRead, budget int64) (*proto.ReadRowsRequest, map[types.RowLocation]*pendingRead) {
	wanted := make(map[types.RowLocation]*pendingRead, len(rows))
	byFile := make(map[string][]*proto.ReadRowsRequest_Row)
	var paths []string
	for _, pending := range rows {
		wanted[pending.location] = pending
		path := pending.location.FilePath
		if _, queued := byFile[path]; !queued {
			paths = append(paths, path)
		}
		byFile[path] = append(byFile[path], &proto.ReadRowsRequest_Row{
			Position: pending.location.Position,
			Columns:  r.readColumns(pending.columns),
		})
	}

	request := &proto.ReadRowsRequest{ThreadId: r.threadID, MaxBytes: budget}
	for _, path := range paths {
		slices.SortFunc(byFile[path], func(a, b *proto.ReadRowsRequest_Row) int {
			return cmp.Compare(a.GetPosition(), b.GetPosition())
		})
		request.Files = append(request.Files, &proto.ReadRowsRequest_FileRows{FilePath: path, Rows: byFile[path]})
	}
	return request, wanted
}

// readColumns lists what to read of one stored row: _op_type, which Java checks to send a soft-deleted row's
// columns as NULL, then the columns holding the wanted values; without normalization, the one JSON column.
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

// fetchRow puts the wanted values of one stored row, or one part of a split row, into the read buffer. NULL
// stays the placeholder: Postgres never leaves out a NULL, so a stored NULL means the value was never written.
// Java sends NULL for a soft-deleted row and for a column the file lacks.
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

	field, present := stored[constants.StringifiedData] // without normalization the row is one JSON document
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
		var value any = constants.UnavailableValue // also for a key missing from the stored row, which predates the column
		if raw, found := row[column]; found && string(raw) != "null" && string(raw) != unavailableJSON {
			value = raw
		}
		r.fetch(pending.olakeID, column, value)
	}
	return nil
}

// fetch puts a value read for a row into the read buffer, where fill finds it.
func (r *unavailableValuesResolver) fetch(olakeID, column string, value any) {
	if value != constants.UnavailableValue {
		r.fromTable++
	}
	r.readBuffer.put(olakeID, column, value)
}

// fieldValue converts one stored value for the record; unset means NULL. The columns read are text (Postgres
// text, json, arrays and the like, _op_type, the stored row), sent as bytes when not UTF-8, or numeric as double.
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
