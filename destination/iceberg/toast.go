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

// toastResolver fills columns Postgres left out of an UPDATE (unchanged TOAST) with the
// value from the row's previous version: from carry if this sync wrote the row, else read
// from the destination at the location the row index (pos or dv) gives.
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

	recovered  int64 // values filled into records
	fromTable  int64 // values read from the destination
	unresolved int64 // values left as the placeholder
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

// readKey identifies one source column of one stored row.
type readKey struct {
	location types.RowLocation
	column   string
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
	wanted := make(map[readKey]string) // read -> olake id of the row it belongs to

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
		for _, column := range missing {
			wanted[readKey{location: location, column: column}] = olakeID
		}
	}

	if len(wanted) == 0 {
		return nil
	}

	values, err := r.readValues(ctx, wanted)
	if err != nil {
		return err
	}
	for key, value := range values {
		if isUnavailable(value) {
			continue // the stored version has the placeholder too
		}
		r.remember(wanted[key], key.column, value)
		r.fromTable++
	}

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

// projection returns the destination column holding a source column, and the JSON key
// to pick out of it when normalization is off.
func (r *toastResolver) projection(column string) (destColumn, key string) {
	if r.normalized {
		return r.resolveColumn(column), ""
	}
	return constants.StringifiedData, column
}

// readValues reads the wanted values in one streamed call, after closing any data file
// this thread still has open among the ones to read. Values that cannot be read are left out.
func (r *toastResolver) readValues(ctx context.Context, wanted map[readKey]string) (map[readKey]any, error) {
	columnIndex := make(map[string]int)
	var columns []string
	positions := make(map[string]map[int64]struct{})
	byRow := make(map[types.RowLocation][]readKey)

	for key := range wanted {
		destColumn, _ := r.projection(key.column)
		if _, known := columnIndex[destColumn]; !known {
			columnIndex[destColumn] = len(columns)
			columns = append(columns, destColumn)
		}
		if positions[key.location.FilePath] == nil {
			positions[key.location.FilePath] = make(map[int64]struct{})
		}
		positions[key.location.FilePath][key.location.Position] = struct{}{}
		byRow[key.location] = append(byRow[key.location], key)
	}

	paths := make([]string, 0, len(positions))
	for path := range positions {
		paths = append(paths, path)
	}
	sort.Strings(paths)

	// a file still being written has no footer and cannot be read
	if err := r.writer.EnsureReadable(ctx, paths); err != nil {
		return nil, err
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
		return nil, fmt.Errorf("failed to read unavailable column values: %w", err)
	}

	values := make(map[readKey]any, len(wanted))
	for {
		batch, err := stream.Recv()
		if err == io.EOF {
			return values, nil
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read unavailable column values: %w", err)
		}

		for _, row := range batch.GetRows() {
			for _, key := range byRow[types.RowLocation{FilePath: row.GetFilePath(), Position: row.GetPosition()}] {
				destColumn, jsonKey := r.projection(key.column)
				index := columnIndex[destColumn]
				if index >= len(row.GetValues()) {
					continue // reply does not match the request
				}
				value, err := decodeValue(row.GetValues()[index], jsonKey)
				if err != nil {
					// keep the placeholder: a retry would read the same bytes again
					logger.Debugf("Thread[%s]: cannot decode %s of %s row %d: %s", r.threadID, destColumn, row.GetFilePath(), row.GetPosition(), err)
					continue
				}
				values[key] = value
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

	if r.recovered+r.unresolved == 0 {
		return
	}

	logger.Debugf("Thread[%s]: recovered %d unavailable column value(s), read %d from the destination table, %d could not be recovered",
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
