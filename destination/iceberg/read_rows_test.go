package iceberg

import (
	"context"
	"errors"
	"io"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	arrowwriter "github.com/datazip-inc/olake/destination/iceberg/arrow-writer"
	legacywriter "github.com/datazip-inc/olake/destination/iceberg/legacy-writer"
	"github.com/datazip-inc/olake/destination/iceberg/proto"
	"github.com/datazip-inc/olake/types"
)

// testIndex is the stream's committed row index. The embedded interface supplies the index calls
// ReadRows never makes.
type testIndex struct {
	types.StreamIndex
	locations map[string]types.RowLocation
	err       error
}

func (i *testIndex) Lookup(olakeID string) (types.RowLocation, bool, error) {
	if i.err != nil {
		return types.RowLocation{}, false, i.err
	}
	location, found := i.locations[olakeID]
	return location, found, nil
}

// testOpenFileWriter stands in for the arrow writer: it records the files it is asked to make readable.
// The embedded interface supplies the writer calls ReadRows never makes.
type testOpenFileWriter struct {
	Writer
	flushes [][]string
	err     error
}

func (w *testOpenFileWriter) EnsureReadable(_ context.Context, paths []string) error {
	w.flushes = append(w.flushes, paths)
	return w.err
}

// testCell addresses one column of one stored row.
type testCell struct {
	filePath string
	position int64
	column   string
}

// testTable is what the data files hold. A nil value is a stored NULL; a cell missing from the map is a
// column the data file does not carry, which the reader sends as NULL too.
type testTable map[testCell]any

// testTableIndexClient answers ReadRows out of a testTable, standing in for the Java side, and keeps every
// request it received. The embedded interface supplies the calls ReadRows never makes.
type testTableIndexClient struct {
	proto.TableIndexServiceClient
	table    testTable
	readErr  error
	recvErr  error
	requests []*proto.ReadRowsRequest
	ctx      context.Context   // of the last call
	opts     []grpc.CallOption // of the last call
	// inParts sends each row the way the Java reader sends a large one: a part per column
	inParts bool
}

func (c *testTableIndexClient) ReadRows(ctx context.Context, request *proto.ReadRowsRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[proto.ReadRowsBatch], error) {
	c.requests = append(c.requests, request)
	c.ctx, c.opts = ctx, opts
	if c.readErr != nil {
		return nil, c.readErr
	}

	var rows []*proto.ReadRowsBatch_Row
	for _, file := range request.GetFiles() {
		for _, requested := range file.GetRows() {
			row := &proto.ReadRowsBatch_Row{
				FilePath: file.GetFilePath(),
				Position: requested.GetPosition(),
				Values:   map[string]*proto.IcebergPayload_IceRecord_FieldValue{},
			}
			for _, column := range requested.GetColumns() {
				value := testFieldValue(c.table[testCell{file.GetFilePath(), requested.GetPosition(), column}])
				if c.inParts {
					rows = append(rows, &proto.ReadRowsBatch_Row{
						FilePath: row.GetFilePath(),
						Position: row.GetPosition(),
						Values:   map[string]*proto.IcebergPayload_IceRecord_FieldValue{column: value},
					})
					continue
				}
				row.Values[column] = value
			}
			if !c.inParts {
				rows = append(rows, row)
			}
		}
	}
	return &testStream{batches: []*proto.ReadRowsBatch{{Rows: rows}}, err: c.recvErr}, nil
}

// testFieldValue mirrors what the Java reader sends: an unset value for a stored NULL.
func testFieldValue(value any) *proto.IcebergPayload_IceRecord_FieldValue {
	if text, ok := value.(string); ok {
		return &proto.IcebergPayload_IceRecord_FieldValue{Value: &proto.IcebergPayload_IceRecord_FieldValue_StringValue{StringValue: text}}
	}
	return &proto.IcebergPayload_IceRecord_FieldValue{}
}

// testStream hands back canned batches, then err (io.EOF when unset). The embedded interface
// supplies the rest of the streaming client, which ReadRows never calls.
type testStream struct {
	grpc.ClientStream
	batches []*proto.ReadRowsBatch
	err     error
	next    int
}

func (s *testStream) Recv() (*proto.ReadRowsBatch, error) {
	if s.next >= len(s.batches) {
		if s.err != nil {
			return nil, s.err
		}
		return nil, io.EOF
	}

	batch := s.batches[s.next]
	s.next++

	return batch, nil
}

// testIceberg is a writer thread's Iceberg with the parts ReadRows uses.
func testIceberg(index *testIndex, writer Writer, client *testTableIndexClient) *Iceberg {
	return &Iceberg{
		options:     &destination.Options{ThreadID: "test-thread"},
		writer:      writer,
		server:      &serverInstance{tableIndexClient: client},
		indexThread: types.NewStreamIndexThread(index),
	}
}

// testEmitted is what one emit call carried.
type testEmitted struct {
	row    int
	values map[string]any
}

// testCollect is an emit that keeps what it is given.
func testCollect(emitted *[]testEmitted) func(int, map[string]any) error {
	return func(row int, values map[string]any) error {
		*emitted = append(*emitted, testEmitted{row: row, values: values})
		return nil
	}
}

func TestReadRows(t *testing.T) {
	// three rows in two files, asked out of position order
	locations := map[string]types.RowLocation{
		"1": {FilePath: "b.parquet", Position: 5},
		"2": {FilePath: "a.parquet", Position: 9},
		"3": {FilePath: "a.parquet", Position: 2},
	}
	table := testTable{
		{"b.parquet", 5, constants.OpType}: "u", {"b.parquet", 5, "payload"}: "p1",
		{"a.parquet", 9, constants.OpType}: "u", {"a.parquet", 9, "payload"}: "p2", {"a.parquet", 9, "doc"}: "d2",
		{"a.parquet", 2, constants.OpType}: "d",
	}
	rows := []destination.RowRead{
		{OlakeID: "1", Columns: []string{constants.OpType, "payload"}},
		{OlakeID: "2", Columns: []string{constants.OpType, "payload", "doc"}},
		{OlakeID: "3", Columns: []string{constants.OpType, "doc"}},
	}

	testCases := []struct {
		name            string
		index           *testIndex
		writer          Writer
		inParts         bool
		rows            []destination.RowRead
		expectedFiles   []*proto.ReadRowsRequest_FileRows // nil: no read is made
		expectedFlushes [][]string
		expectedEmitted []testEmitted
	}{
		// each answer is mapped back to its row by file and position
		{
			name:   "rows are asked in the order given and mapped back by file and position",
			index:  &testIndex{locations: locations},
			writer: struct{ Writer }{},
			rows:   rows,
			expectedFiles: []*proto.ReadRowsRequest_FileRows{
				{FilePath: "b.parquet", Rows: []*proto.ReadRowsRequest_Row{{Position: 5, Columns: []string{constants.OpType, "payload"}}}},
				{FilePath: "a.parquet", Rows: []*proto.ReadRowsRequest_Row{
					{Position: 9, Columns: []string{constants.OpType, "payload", "doc"}},
					{Position: 2, Columns: []string{constants.OpType, "doc"}},
				}},
			},
			expectedEmitted: []testEmitted{
				{row: 0, values: map[string]any{constants.OpType: "u", "payload": "p1"}},
				{row: 1, values: map[string]any{constants.OpType: "u", "payload": "p2", "doc": "d2"}},
				// a column the file lacks is NULL
				{row: 2, values: map[string]any{constants.OpType: "d", "doc": nil}},
			},
		},
		// the file is named to the writer once per run too; it ignores a file it has already closed
		{
			name:   "a file the order comes back to is listed again",
			index:  &testIndex{locations: locations},
			writer: &testOpenFileWriter{},
			rows:   []destination.RowRead{rows[1], rows[0], rows[2]},
			expectedFiles: []*proto.ReadRowsRequest_FileRows{
				{FilePath: "a.parquet", Rows: []*proto.ReadRowsRequest_Row{{Position: 9, Columns: []string{constants.OpType, "payload", "doc"}}}},
				{FilePath: "b.parquet", Rows: []*proto.ReadRowsRequest_Row{{Position: 5, Columns: []string{constants.OpType, "payload"}}}},
				{FilePath: "a.parquet", Rows: []*proto.ReadRowsRequest_Row{{Position: 2, Columns: []string{constants.OpType, "doc"}}}},
			},
			expectedFlushes: [][]string{{"a.parquet", "b.parquet", "a.parquet"}},
			expectedEmitted: []testEmitted{
				{row: 0, values: map[string]any{constants.OpType: "u", "payload": "p2", "doc": "d2"}},
				{row: 1, values: map[string]any{constants.OpType: "u", "payload": "p1"}},
				{row: 2, values: map[string]any{constants.OpType: "d", "doc": nil}},
			},
		},
		// a row too large for one message comes in parts, each emitted as it arrives
		{
			name:    "a row in parts is emitted part by part",
			index:   &testIndex{locations: locations},
			writer:  struct{ Writer }{},
			inParts: true,
			rows:    rows[:1],
			expectedFiles: []*proto.ReadRowsRequest_FileRows{
				{FilePath: "b.parquet", Rows: []*proto.ReadRowsRequest_Row{{Position: 5, Columns: []string{constants.OpType, "payload"}}}},
			},
			expectedEmitted: []testEmitted{
				{row: 0, values: map[string]any{constants.OpType: "u"}},
				{row: 0, values: map[string]any{"payload": "p1"}},
			},
		},
		// a filtered row, or an UPDATE that changed the primary key: answered at once, without a read
		{
			name:   "a row the index does not know has no previous version",
			index:  &testIndex{locations: locations},
			writer: &testOpenFileWriter{},
			rows:   []destination.RowRead{{OlakeID: "42", Columns: []string{constants.OpType, "payload"}}, rows[0]},
			expectedFiles: []*proto.ReadRowsRequest_FileRows{
				{FilePath: "b.parquet", Rows: []*proto.ReadRowsRequest_Row{{Position: 5, Columns: []string{constants.OpType, "payload"}}}},
			},
			expectedFlushes: [][]string{{"b.parquet"}},
			expectedEmitted: []testEmitted{
				{row: 0, values: nil},
				{row: 1, values: map[string]any{constants.OpType: "u", "payload": "p1"}},
			},
		},
		{
			name:            "no read when no row is known",
			index:           &testIndex{locations: locations},
			writer:          &testOpenFileWriter{},
			rows:            []destination.RowRead{{OlakeID: "42", Columns: []string{constants.OpType, "payload"}}},
			expectedEmitted: []testEmitted{{row: 0, values: nil}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			client := &testTableIndexClient{table: table, inParts: tc.inParts}
			iceberg := testIceberg(tc.index, tc.writer, client)
			var emitted []testEmitted

			err := iceberg.ReadRows(context.Background(), tc.rows, testCollect(&emitted))

			require.NoError(t, err)
			assert.Equal(t, tc.expectedEmitted, emitted)
			if withOpenFiles, ok := tc.writer.(*testOpenFileWriter); ok {
				assert.Equal(t, tc.expectedFlushes, withOpenFiles.flushes, "files made readable")
			}
			if tc.expectedFiles == nil {
				assert.Empty(t, client.requests)
				return
			}
			require.Len(t, client.requests, 1)
			// one message may carry a value far past the 4 MB default, and Java's reader stops once the call is over
			assert.Equal(t, []grpc.CallOption{grpc.MaxCallRecvMsgSize(math.MaxInt32)}, client.opts)
			assert.ErrorIs(t, client.ctx.Err(), context.Canceled, "the read's context is canceled once it returns")
			request := client.requests[0]
			assert.Equal(t, "test-thread", request.GetThreadId())
			require.Len(t, request.GetFiles(), len(tc.expectedFiles))
			for idx, file := range tc.expectedFiles {
				assert.Equal(t, file.GetFilePath(), request.GetFiles()[idx].GetFilePath())
				require.Len(t, request.GetFiles()[idx].GetRows(), len(file.GetRows()), file.GetFilePath())
				for rowIdx, row := range file.GetRows() {
					assert.Equal(t, row.GetPosition(), request.GetFiles()[idx].GetRows()[rowIdx].GetPosition())
					assert.Equal(t, row.GetColumns(), request.GetFiles()[idx].GetRows()[rowIdx].GetColumns())
				}
			}
		})
	}
}

func TestReadRowsErrors(t *testing.T) {
	failure := errors.New("boom")
	locations := map[string]types.RowLocation{"1": {FilePath: "a.parquet", Position: 0}}

	// every failure is wrapped with %w, so the destination error classifier can still see it
	testCases := []struct {
		name    string
		index   *testIndex
		writer  Writer
		client  *testTableIndexClient
		emit    func(int, map[string]any) error
		message string
		reads   int // read calls made before the failure
	}{
		{
			name:    "index lookup fails",
			index:   &testIndex{err: failure},
			writer:  struct{ Writer }{},
			client:  &testTableIndexClient{},
			message: "failed to look up row[1] in index",
		},
		// the arrow writer's open files are closed before the read, so the read never sees a file without a footer
		{
			name:    "open file cannot be closed",
			index:   &testIndex{locations: locations},
			writer:  &testOpenFileWriter{err: failure},
			client:  &testTableIndexClient{},
			message: "boom",
		},
		{
			name:    "read call fails",
			index:   &testIndex{locations: locations},
			writer:  struct{ Writer }{},
			client:  &testTableIndexClient{readErr: failure},
			message: "failed to read unavailable column values",
			reads:   1,
		},
		{
			name:    "read stream breaks",
			index:   &testIndex{locations: locations},
			writer:  struct{ Writer }{},
			client:  &testTableIndexClient{table: testTable{}, recvErr: failure},
			message: "failed to read unavailable column values",
			reads:   1,
		},
		{
			name:    "the caller rejects a row",
			index:   &testIndex{locations: locations},
			writer:  struct{ Writer }{},
			client:  &testTableIndexClient{table: testTable{}},
			emit:    func(int, map[string]any) error { return failure },
			message: "boom",
			reads:   1,
		},
		// a row the index does not know is answered before anything is read
		{
			name:    "the caller rejects a row the index does not know",
			index:   &testIndex{locations: map[string]types.RowLocation{}},
			writer:  struct{ Writer }{},
			client:  &testTableIndexClient{},
			emit:    func(int, map[string]any) error { return failure },
			message: "boom",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			iceberg := testIceberg(tc.index, tc.writer, tc.client)
			emit := tc.emit
			if emit == nil {
				emit = func(int, map[string]any) error { return nil }
			}

			err := iceberg.ReadRows(context.Background(), []destination.RowRead{{OlakeID: "1", Columns: []string{"payload"}}}, emit)

			require.Error(t, err)
			assert.ErrorIs(t, err, failure)
			assert.Contains(t, err.Error(), tc.message)
			assert.Len(t, tc.client.requests, tc.reads, "reads made before the failure")
		})
	}
}

// The arrow writer holds data files open on the Go side, so ReadRows must ask it to close them; the legacy
// writer's open files are Java's, closed inside the read.
func TestOpenFileWriters(t *testing.T) {
	_, arrowHoldsFiles := any(&arrowwriter.ArrowWriter{}).(openFileWriter)
	_, legacyHoldsFiles := any(&legacywriter.LegacyWriter{}).(openFileWriter)

	assert.True(t, arrowHoldsFiles)
	assert.False(t, legacyHoldsFiles)
}

func TestFieldValue(t *testing.T) {
	testCases := []struct {
		name     string
		value    *proto.IcebergPayload_IceRecord_FieldValue
		expected any
	}{
		// an unset value is how the reader says the stored column is NULL
		{name: "unset value is NULL", value: &proto.IcebergPayload_IceRecord_FieldValue{}, expected: nil},
		{name: "string", value: &proto.IcebergPayload_IceRecord_FieldValue{Value: &proto.IcebergPayload_IceRecord_FieldValue_StringValue{StringValue: "hello"}}, expected: "hello"},
		// text that is not UTF-8 (the arrow writer stores a Postgres bytea as it is) comes as bytes
		{name: "bytes are text that is not UTF-8", value: &proto.IcebergPayload_IceRecord_FieldValue{Value: &proto.IcebergPayload_IceRecord_FieldValue_BytesValue{BytesValue: []byte{0xff, 'a'}}}, expected: "\xffa"},
		{name: "double", value: &proto.IcebergPayload_IceRecord_FieldValue{Value: &proto.IcebergPayload_IceRecord_FieldValue_DoubleValue{DoubleValue: 1.5}}, expected: 1.5},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, fieldValue(tc.value))
		})
	}
}
