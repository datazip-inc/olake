package iceberg

import (
	"context"
	"errors"
	"io"
	"maps"
	"testing"

	"github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination/iceberg/proto"
	"github.com/datazip-inc/olake/types"
)

// placeholder is what the source writes for a column it could not send.
const placeholder = constants.UnavailableValue

// testWriter stands in for the Iceberg writers: it answers where a row lives, records the
// files the resolver asks to make readable, and fails on demand.
type testWriter struct {
	locations map[string]types.RowLocation
	lookupErr error
	ensureErr error
	lookups   int
	flushes   [][]string
}

func (w *testWriter) Write(context.Context, []types.RawRecord) error        { return nil }
func (w *testWriter) EvolveSchema(context.Context, map[string]string) error { return nil }
func (w *testWriter) Close(context.Context, any) error                      { return nil }
func (w *testWriter) Abort()                                                {}

func (w *testWriter) Lookup(olakeID string) (types.RowLocation, bool, error) {
	w.lookups++
	if w.lookupErr != nil {
		return types.RowLocation{}, false, w.lookupErr
	}
	location, found := w.locations[olakeID]
	return location, found, nil
}

func (w *testWriter) EnsureReadable(_ context.Context, paths []string) error {
	w.flushes = append(w.flushes, paths)
	return w.ensureErr
}

// testCell addresses one column of one stored row.
type testCell struct {
	filePath string
	position int64
	column   string
}

// testTable is what the destination holds. A nil value is a stored NULL; a cell missing
// from the map is a column the data file does not carry.
type testTable map[testCell]any

// testReader answers ReadRows out of a testTable, standing in for the Java side, and keeps
// every request it received.
type testReader struct {
	table    testTable
	readErr  error
	recvErr  error
	requests []*proto.ReadRowsRequest
}

func (r *testReader) FlushOpenFiles(context.Context, *proto.FlushOpenFilesRequest, ...grpc.CallOption) (*proto.FlushOpenFilesResponse, error) {
	return &proto.FlushOpenFilesResponse{}, nil
}

func (r *testReader) ReadRows(_ context.Context, request *proto.ReadRowsRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[proto.ReadRowsBatch], error) {
	r.requests = append(r.requests, request)
	if r.readErr != nil {
		return nil, r.readErr
	}

	batch := &proto.ReadRowsBatch{}
	for _, file := range request.GetFiles() {
		for _, position := range file.GetPositions() {
			row := &proto.ReadRowsBatch_Row{FilePath: file.GetFilePath(), Position: position}
			for _, column := range request.GetColumns() {
				value, present := r.table[testCell{filePath: file.GetFilePath(), position: position, column: column}]
				row.Values = append(row.Values, testColumnValue(value, present))
			}
			batch.Rows = append(batch.Rows, row)
		}
	}

	return &testStream{batches: []*proto.ReadRowsBatch{batch}, err: r.recvErr}, nil
}

// testColumnValue mirrors what the Java reader sends: an unset value for a stored NULL, and
// the placeholder for a column the file does not carry at all.
func testColumnValue(value any, present bool) *proto.ColumnValue {
	if !present {
		return &proto.ColumnValue{Value: &proto.ColumnValue_StringValue{StringValue: placeholder}}
	}

	switch typed := value.(type) {
	case string:
		return &proto.ColumnValue{Value: &proto.ColumnValue_StringValue{StringValue: typed}}
	case int64:
		return &proto.ColumnValue{Value: &proto.ColumnValue_LongValue{LongValue: typed}}
	default:
		return &proto.ColumnValue{}
	}
}

// testStream hands back canned batches, then err (io.EOF when unset). The embedded interface
// supplies the rest of the streaming client, which the resolver never calls.
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

func testResolver(t *testing.T, normalized bool, writer *testWriter, reader *testReader) *toastResolver {
	t.Helper()

	resolver := &toastResolver{
		writer:        writer,
		reader:        reader,
		threadID:      "test-thread",
		resolveColumn: func(column string) string { return column },
		normalized:    normalized,
		tracked:       make(map[string]struct{}),
		carry:         make(map[string]*carryRow),
	}
	t.Cleanup(resolver.drop)

	return resolver
}

func testRecord(olakeID, opType string, data map[string]any, unavailable ...string) types.RawRecord {
	return types.RawRecord{
		Data: data,
		OlakeColumns: map[string]any{
			constants.OlakeID: olakeID,
			constants.OpType:  opType,
		},
		UnavailableColumns: unavailable,
	}
}

// testOverBudget makes the process look like it already holds the whole carry budget, so
// the next Resolve has to drop what it carries.
func testOverBudget(t *testing.T) {
	t.Helper()

	carryUsage.Add(constants.MaxToastCarryBytes)
	t.Cleanup(func() { carryUsage.Add(-constants.MaxToastCarryBytes) })
}

// resolveStep is one Resolve call, i.e. one batch. before changes what the destination holds
// first, the way a write between two batches would. expected lists, per record, the columns
// to check afterwards; a nil entry skips that record.
type resolveStep struct {
	before   func(writer *testWriter, table testTable)
	records  []types.RawRecord
	expected []map[string]any
}

func TestResolve(t *testing.T) {
	committed := map[string]types.RowLocation{"1": {FilePath: "a.parquet", Position: 0}}

	testCases := []struct {
		name               string
		rawData            bool // normalization off: the whole source row is stored as JSON
		locations          map[string]types.RowLocation
		table              testTable
		steps              []resolveStep
		expectedLookups    int
		expectedFromMemory int64
		expectedFromTable  int64
		expectedUnresolved int64
	}{
		// the basic case: the previous version is committed, so the value is read back
		{
			name:      "reads the value of a committed row",
			locations: committed,
			table:     testTable{{"a.parquet", 0, "payload"}: "big value"},
			steps: []resolveStep{{
				records:  []types.RawRecord{testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload")},
				expected: []map[string]any{{"payload": "big value"}},
			}},
			expectedLookups:   1,
			expectedFromTable: 1,
		},
		// the inserted row is not in the index yet; only the batch itself knows the value
		{
			name: "insert then update in one batch reuses the inserted value",
			steps: []resolveStep{{
				records: []types.RawRecord{
					testRecord("1", "i", map[string]any{"id": 1, "payload": "inserted"}),
					testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload"),
				},
				expected: []map[string]any{nil, {"payload": "inserted"}},
			}},
			expectedFromMemory: 1,
		},
		// the index still points at the old version, so a lookup would return a stale value
		{
			name:      "a value changed earlier in the batch wins over the committed one",
			locations: committed,
			table:     testTable{{"a.parquet", 0, "payload"}: "old value"},
			steps: []resolveStep{{
				records: []types.RawRecord{
					testRecord("1", "u", map[string]any{"id": 1, "payload": "new value"}),
					testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload"),
				},
				expected: []map[string]any{nil, {"payload": "new value"}},
			}},
			expectedFromMemory: 1,
		},
		// the second record waits on the first one's read instead of issuing its own
		{
			name:      "two marked updates of one row share one read",
			locations: committed,
			table:     testTable{{"a.parquet", 0, "payload"}: "stored"},
			steps: []resolveStep{{
				records: []types.RawRecord{
					testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload"),
					testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload"),
				},
				expected: []map[string]any{{"payload": "stored"}, {"payload": "stored"}},
			}},
			expectedLookups:   1,
			expectedFromTable: 2,
		},
		// batches without a marker still update the carry; skipping them would hand the
		// third batch the value read in the first one
		{
			name:      "a change in a later batch without markers replaces the carried value",
			locations: committed,
			table:     testTable{{"a.parquet", 0, "payload"}: "P1"},
			steps: []resolveStep{
				{
					records:  []types.RawRecord{testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload")},
					expected: []map[string]any{{"payload": "P1"}},
				},
				{records: []types.RawRecord{testRecord("1", "u", map[string]any{"id": 1, "payload": "changed"})}},
				{
					records:  []types.RawRecord{testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload")},
					expected: []map[string]any{{"payload": "changed"}},
				},
			},
			expectedLookups:    1,
			expectedFromTable:  1,
			expectedFromMemory: 1,
		},
		// same as above for a delete followed by a re-insert
		{
			name:      "a delete and re-insert in a batch without markers replace the carried value",
			locations: committed,
			table:     testTable{{"a.parquet", 0, "payload"}: "P1"},
			steps: []resolveStep{
				{records: []types.RawRecord{testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload")}},
				{records: []types.RawRecord{
					testRecord("1", "d", map[string]any{"id": 1}),
					testRecord("1", "c", map[string]any{"id": 1, "payload": "C"}),
				}},
				{
					records:  []types.RawRecord{testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload")},
					expected: []map[string]any{{"payload": "C"}},
				},
			},
			expectedLookups:    1,
			expectedFromTable:  1,
			expectedFromMemory: 1,
		},
		// a deleted row's columns are gone; its old values must not come back
		{
			name: "a delete carries nothing forward",
			steps: []resolveStep{{
				records: []types.RawRecord{
					testRecord("1", "i", map[string]any{"id": 1, "payload": "inserted"}),
					testRecord("1", "d", map[string]any{"id": 1}),
					testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload"),
				},
				expected: []map[string]any{nil, nil, {"payload": placeholder}},
			}},
			expectedUnresolved: 1,
		},
		{
			name: "a re-insert after a delete in the same batch is used",
			steps: []resolveStep{{
				records: []types.RawRecord{
					testRecord("1", "d", map[string]any{"id": 1}),
					testRecord("1", "i", map[string]any{"id": 1, "payload": "re-inserted"}),
					testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload"),
				},
				expected: []map[string]any{nil, nil, {"payload": "re-inserted"}},
			}},
			expectedFromMemory: 1,
		},
		// a filtered row, or an update that changed the primary key
		{
			name: "a row with no destination version keeps the placeholder",
			steps: []resolveStep{{
				records:  []types.RawRecord{testRecord("42", "u", map[string]any{"id": 42, "payload": placeholder}, "payload")},
				expected: []map[string]any{{"payload": placeholder}},
			}},
			expectedLookups:    1,
			expectedUnresolved: 1,
		},
		// the source marks the column, but the stream's selection dropped it from Data
		{
			name:      "a column left out of the stream selection is skipped",
			locations: committed,
			steps: []resolveStep{{
				records: []types.RawRecord{testRecord("1", "u", map[string]any{"id": 1}, "payload")},
			}},
		},
		// the previous version was written before this feature; there is nothing better to write
		{
			name:      "a stored placeholder is not counted as recovered",
			locations: committed,
			table:     testTable{{"a.parquet", 0, "payload"}: placeholder},
			steps: []resolveStep{{
				records:  []types.RawRecord{testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload")},
				expected: []map[string]any{{"payload": placeholder}},
			}},
			expectedLookups:    1,
			expectedUnresolved: 1,
		},
		// the first read finds a file that predates the column; the failure must not be
		// remembered, so the next change reads again
		{
			name:      "a read that found nothing is retried by the next change",
			locations: committed,
			steps: []resolveStep{
				{
					records:  []types.RawRecord{testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload")},
					expected: []map[string]any{{"payload": placeholder}},
				},
				{
					before: func(_ *testWriter, table testTable) {
						table[testCell{"a.parquet", 0, "payload"}] = "recovered later"
					},
					records:  []types.RawRecord{testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload")},
					expected: []map[string]any{{"payload": "recovered later"}},
				},
			},
			expectedLookups:    2,
			expectedFromTable:  1,
			expectedUnresolved: 1,
		},
		// both columns come out of the same stored JSON row; each must get its own key
		{
			name:      "without normalization each column is picked out of the stored row",
			rawData:   true,
			locations: committed,
			table:     testTable{{"a.parquet", 0, constants.StringifiedData}: `{"id":1,"payload":"stored value","doc":{"k":1}}`},
			steps: []resolveStep{{
				records: []types.RawRecord{
					testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder, "doc": placeholder}, "payload", "doc"),
				},
				expected: []map[string]any{{
					"payload": json.RawMessage(`"stored value"`),
					"doc":     json.RawMessage(`{"k":1}`),
				}},
			}},
			expectedLookups:   2,
			expectedFromTable: 2,
		},
		// streams that never produce a marker must cost nothing
		{
			name:      "a batch without markers does nothing",
			locations: committed,
			steps: []resolveStep{{
				records:  []types.RawRecord{testRecord("1", "u", map[string]any{"id": 1, "payload": "value"})},
				expected: []map[string]any{{"payload": "value"}},
			}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			writer := &testWriter{locations: map[string]types.RowLocation{}}
			maps.Copy(writer.locations, tc.locations)
			table := testTable{}
			maps.Copy(table, tc.table)
			resolver := testResolver(t, !tc.rawData, writer, &testReader{table: table})

			for stepIdx, step := range tc.steps {
				if step.before != nil {
					step.before(writer, table)
				}
				require.NoError(t, resolver.Resolve(context.Background(), step.records))

				for recordIdx, columns := range step.expected {
					for column, expected := range columns {
						assert.Equal(t, expected, step.records[recordIdx].Data[column], "batch %d, record %d, column %s", stepIdx, recordIdx, column)
					}
				}
			}

			assert.Equal(t, tc.expectedLookups, writer.lookups, "lookups")
			assert.Equal(t, tc.expectedFromMemory, resolver.fromMemory, "from memory")
			assert.Equal(t, tc.expectedFromTable, resolver.fromTable, "from table")
			assert.Equal(t, tc.expectedUnresolved, resolver.unresolved, "unresolved")
		})
	}
}

func TestResolveReadRequest(t *testing.T) {
	testCases := []struct {
		name            string
		locations       map[string]types.RowLocation
		records         []types.RawRecord
		expectedColumns []string
		expectedFiles   []*proto.ReadRowsRequest_FileRows
		expectedFlushes [][]string
	}{
		// one call per batch: files sorted, positions ascending, each column once
		{
			name: "reads are grouped by file in one call",
			locations: map[string]types.RowLocation{
				"1": {FilePath: "b.parquet", Position: 5},
				"2": {FilePath: "a.parquet", Position: 9},
				"3": {FilePath: "a.parquet", Position: 2},
			},
			records: []types.RawRecord{
				testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload"),
				testRecord("2", "u", map[string]any{"id": 2, "payload": placeholder, "doc": placeholder}, "payload", "doc"),
				testRecord("3", "u", map[string]any{"id": 3, "doc": placeholder}, "doc"),
			},
			expectedColumns: []string{"payload", "doc"},
			expectedFiles: []*proto.ReadRowsRequest_FileRows{
				{FilePath: "a.parquet", Positions: []int64{2, 9}},
				{FilePath: "b.parquet", Positions: []int64{5}},
			},
			expectedFlushes: [][]string{{"a.parquet", "b.parquet"}},
		},
		// a value found in the batch itself needs no read, and no file has to be closed
		{
			name: "no request when every value comes from the batch",
			records: []types.RawRecord{
				testRecord("1", "i", map[string]any{"id": 1, "payload": "inserted"}),
				testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload"),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			writer := &testWriter{locations: tc.locations}
			reader := &testReader{table: testTable{}}
			resolver := testResolver(t, true, writer, reader)

			require.NoError(t, resolver.Resolve(context.Background(), tc.records))

			assert.Equal(t, tc.expectedFlushes, writer.flushes, "files made readable")
			if tc.expectedFiles == nil {
				assert.Empty(t, reader.requests)
				return
			}
			require.Len(t, reader.requests, 1)
			request := reader.requests[0]
			assert.Equal(t, "test-thread", request.GetThreadId())
			// column order follows map iteration, so only the set is fixed
			assert.ElementsMatch(t, tc.expectedColumns, request.GetColumns())
			require.Len(t, request.GetFiles(), len(tc.expectedFiles))
			for idx, expected := range tc.expectedFiles {
				assert.Equal(t, expected.GetFilePath(), request.GetFiles()[idx].GetFilePath())
				assert.Equal(t, expected.GetPositions(), request.GetFiles()[idx].GetPositions())
			}
		})
	}
}

func TestResolveErrors(t *testing.T) {
	failure := errors.New("boom")

	// every failure is wrapped with %w, so the destination error classifier can still see it
	testCases := []struct {
		name    string
		writer  *testWriter
		reader  *testReader
		message string
	}{
		{
			name:    "index lookup fails",
			writer:  &testWriter{lookupErr: failure},
			reader:  &testReader{},
			message: "failed to look up row[1] in index",
		},
		{
			name:    "open file cannot be closed",
			writer:  &testWriter{ensureErr: failure},
			reader:  &testReader{},
			message: "boom",
		},
		{
			name:    "read call fails",
			writer:  &testWriter{},
			reader:  &testReader{readErr: failure},
			message: "failed to read unavailable column values",
		},
		{
			name:    "read stream breaks",
			writer:  &testWriter{},
			reader:  &testReader{table: testTable{}, recvErr: failure},
			message: "failed to read unavailable column values",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.writer.locations == nil {
				tc.writer.locations = map[string]types.RowLocation{"1": {FilePath: "a.parquet", Position: 0}}
			}
			resolver := testResolver(t, true, tc.writer, tc.reader)
			records := []types.RawRecord{testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload")}

			err := resolver.Resolve(context.Background(), records)

			require.Error(t, err)
			assert.ErrorIs(t, err, failure)
			assert.Contains(t, err.Error(), tc.message)
			assert.Equal(t, placeholder, records[0].Data["payload"], "a failed batch writes nothing")
		})
	}
}

func TestResolveCarryBudget(t *testing.T) {
	testCases := []struct {
		name                string
		overBudget          bool
		records             []types.RawRecord
		expectedCarriedRows int
	}{
		// the values are kept for the next change to the row
		{
			name: "under budget the written values are carried",
			records: []types.RawRecord{
				testRecord("1", "i", map[string]any{"id": 1, "payload": "inserted"}),
				testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload"),
			},
			expectedCarriedRows: 1,
		},
		// dropping costs nothing: the rows are read back only if a later change asks for them
		{
			name:       "over budget the carried values are dropped",
			overBudget: true,
			records: []types.RawRecord{
				testRecord("1", "i", map[string]any{"id": 1, "payload": "inserted"}),
				testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload"),
			},
		},
		// a delete-heavy stream must not grow the carry without bound
		{
			name:       "tombstones count against the budget",
			overBudget: true,
			records: []types.RawRecord{
				testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload"),
				testRecord("2", "d", map[string]any{"id": 2}),
				testRecord("3", "d", map[string]any{"id": 3}),
			},
		},
		{
			name:    "a stream that never marks a column carries nothing",
			records: []types.RawRecord{testRecord("1", "u", map[string]any{"id": 1, "payload": "value"})},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			writer := &testWriter{locations: map[string]types.RowLocation{}}
			resolver := testResolver(t, true, writer, &testReader{table: testTable{}})
			if tc.overBudget {
				testOverBudget(t)
			}

			require.NoError(t, resolver.Resolve(context.Background(), tc.records))

			assert.Len(t, resolver.carry, tc.expectedCarriedRows)
			if tc.expectedCarriedRows == 0 {
				assert.Zero(t, resolver.carryBytes)
			}
			assert.Empty(t, writer.flushes, "no file is closed to drop or keep carried values")
		})
	}
}

func TestToastResolverCloseReleasesBudget(t *testing.T) {
	before := carryUsage.Load()
	resolver := testResolver(t, true, &testWriter{locations: map[string]types.RowLocation{}}, &testReader{table: testTable{}})

	require.NoError(t, resolver.Resolve(context.Background(), []types.RawRecord{
		testRecord("1", "i", map[string]any{"id": 1, "payload": "inserted"}),
		testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload"),
	}))
	require.Greater(t, carryUsage.Load(), before, "carrying a value takes from the shared budget")

	resolver.Close()

	assert.Equal(t, before, carryUsage.Load(), "the thread gives back everything it held")
	assert.Empty(t, resolver.carry)
}

func TestDecodeValue(t *testing.T) {
	text := func(value string) *proto.ColumnValue {
		return &proto.ColumnValue{Value: &proto.ColumnValue_StringValue{StringValue: value}}
	}

	testCases := []struct {
		name        string
		value       *proto.ColumnValue
		jsonKey     string
		expected    any
		expectedErr bool
	}{
		// an unset value is how the reader says the stored column is NULL
		{name: "unset value is NULL", value: &proto.ColumnValue{}, expected: nil},
		{name: "string", value: text("hello"), expected: "hello"},
		{name: "long", value: &proto.ColumnValue{Value: &proto.ColumnValue_LongValue{LongValue: 7}}, expected: int64(7)},
		{name: "double", value: &proto.ColumnValue{Value: &proto.ColumnValue_DoubleValue{DoubleValue: 1.5}}, expected: 1.5},
		{name: "bool", value: &proto.ColumnValue{Value: &proto.ColumnValue_BoolValue{BoolValue: true}}, expected: true},
		{name: "bytes come back as text", value: &proto.ColumnValue{Value: &proto.ColumnValue_BytesValue{BytesValue: []byte("raw")}}, expected: "raw"},
		// normalization off: the key's raw JSON is spliced back unchanged
		{name: "json key holding a string", value: text(`{"payload":"v"}`), jsonKey: "payload", expected: json.RawMessage(`"v"`)},
		{name: "json key holding an object", value: text(`{"doc":{"k":1}}`), jsonKey: "doc", expected: json.RawMessage(`{"k":1}`)},
		// the stored row predates the column: keep the placeholder rather than invent a NULL
		{name: "json key missing from the stored row", value: text(`{"id":1}`), jsonKey: "payload", expected: placeholder},
		{name: "json key on a non-text value", value: &proto.ColumnValue{Value: &proto.ColumnValue_LongValue{LongValue: 7}}, jsonKey: "payload", expectedErr: true},
		{name: "json key on invalid json", value: text(`{not json`), jsonKey: "payload", expectedErr: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			value, err := decodeValue(tc.value, tc.jsonKey)
			if tc.expectedErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expected, value)
		})
	}
}
