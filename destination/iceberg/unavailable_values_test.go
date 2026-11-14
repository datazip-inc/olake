package iceberg

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	protobuf "google.golang.org/protobuf/proto"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination/iceberg/proto"
	"github.com/datazip-inc/olake/types"
)

// placeholder is what the source writes for a column it could not send.
const placeholder = constants.UnavailableValue

// testWriter stands in for the arrow writer and its stream index: it answers where a row
// lives, records the files the resolver asks to make readable, and fails on demand.
type testWriter struct {
	locations map[string]types.RowLocation
	lookupErr error
	ensureErr error
	lookups   int
	flushes   [][]string
}

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

// testIndex is the committed stream index, answered by the testWriter. The embedded
// interface supplies the index calls the resolver never makes.
type testIndex struct {
	types.StreamIndex
	writer *testWriter
}

func (i testIndex) Lookup(key string) (types.RowLocation, bool, error) {
	return i.writer.Lookup(key)
}

// testCell addresses one column of one stored row.
type testCell struct {
	filePath string
	position int64
	column   string
}

// testTable is what the destination holds. A nil value is a stored NULL; a cell missing
// from the map is a column the data file does not carry, which the reader sends as NULL.
type testTable map[testCell]any

// testTableIndexClient answers ReadRows out of a testTable, standing in for the Java side, and keeps
// every request it received. The embedded interface supplies the table index calls the
// resolver never makes.
type testTableIndexClient struct {
	proto.TableIndexServiceClient
	table    testTable
	readErr  error
	recvErr  error
	requests []*proto.ReadRowsRequest
	// inParts sends each row the way the Java reader sends a large one: a part per column
	inParts bool
	// reverse sends the rows last first, as Java's concurrent file reads may
	reverse bool
}

func (r *testTableIndexClient) ReadRows(_ context.Context, request *proto.ReadRowsRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[proto.ReadRowsBatch], error) {
	r.requests = append(r.requests, request)
	if r.readErr != nil {
		return nil, r.readErr
	}

	var rows []*proto.ReadRowsBatch_Row
	for _, file := range request.GetFiles() {
		for _, requested := range file.GetRows() {
			position := requested.GetPosition()
			row := &proto.ReadRowsBatch_Row{
				FilePath: file.GetFilePath(),
				Position: position,
				Values:   map[string]*proto.IcebergPayload_IceRecord_FieldValue{},
			}
			deleted := r.table[testCell{filePath: file.GetFilePath(), position: position, column: constants.OpType}] == "d"
			for _, column := range requested.GetColumns() {
				// as the Java reader: NULL for a column the file lacks and for a soft-deleted row
				var value any
				if !deleted || column == constants.OpType {
					value = r.table[testCell{filePath: file.GetFilePath(), position: position, column: column}]
				}
				row.Values[column] = testFieldValue(value)
			}
			if r.inParts {
				rows = append(rows, testRowParts(row, requested.GetColumns())...)
				continue
			}
			rows = append(rows, row)
		}
	}

	if r.reverse {
		slices.Reverse(rows)
	}
	// as the Java reader, the stream ends after the row (or part) that takes the bytes sent past max_bytes
	sent := int64(0)
	for idx, row := range rows {
		if sent += int64(protobuf.Size(row)); sent > request.GetMaxBytes() {
			rows = rows[:idx+1]
			break
		}
	}
	return &testStream{batches: []*proto.ReadRowsBatch{{Rows: rows}}, err: r.recvErr}, nil
}

// testRowParts splits a row into a part per column, in the order asked, as the Java reader
// splits a row whose every column is large: _op_type, asked first, goes alone.
func testRowParts(row *proto.ReadRowsBatch_Row, columns []string) []*proto.ReadRowsBatch_Row {
	var parts []*proto.ReadRowsBatch_Row
	for _, column := range columns {
		parts = append(parts, &proto.ReadRowsBatch_Row{
			FilePath: row.GetFilePath(),
			Position: row.GetPosition(),
			Values:   map[string]*proto.IcebergPayload_IceRecord_FieldValue{column: row.GetValues()[column]},
		})
	}

	return parts
}

// testFieldValue mirrors what the Java reader sends: an unset value for a stored NULL.
func testFieldValue(value any) *proto.IcebergPayload_IceRecord_FieldValue {
	if text, ok := value.(string); ok {
		return &proto.IcebergPayload_IceRecord_FieldValue{Value: &proto.IcebergPayload_IceRecord_FieldValue_StringValue{StringValue: text}}
	}
	return &proto.IcebergPayload_IceRecord_FieldValue{}
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

// testConfiguredStream is a stream that keeps source column names, so destination columns
// in the tests read the same as the source ones.
func testConfiguredStream(normalized bool) *types.ConfiguredStream {
	return &types.ConfiguredStream{StreamMetadata: types.StreamMetadata{Normalization: new(normalized), UseSourceColumnNames: true}}
}

func testResolver(t *testing.T, normalized bool, writer *testWriter, tableIndexClient *testTableIndexClient) *unavailableValuesResolver {
	t.Helper()

	resolver := &unavailableValuesResolver{
		ensureReadable:     writer.EnsureReadable,
		index:              types.NewStreamIndexThread(testIndex{writer: writer}),
		tableIndexClient:   tableIndexClient,
		threadID:           "test-thread",
		stream:             testConfiguredStream(normalized),
		unavailableColumns: make(map[string]struct{}),
		cachedValues:       newCachedValues(),
		tableValues:        newTableValues(),
	}
	t.Cleanup(resolver.Close)

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

// testOverBudget makes so many threads share the cache budget that this thread's share holds
// nothing, so the next RecoverUnavailableValues has to drop what it caches.
func testOverBudget(t *testing.T) {
	t.Helper()

	cachingThreads.Add(maxCachedBytes)
	t.Cleanup(func() { cachingThreads.Add(-maxCachedBytes) })
}

// testRecoverAll recovers the values of records as one prefix, as a batch under the byte limit is.
func testRecoverAll(resolver *unavailableValuesResolver, records []types.RawRecord) error {
	_, err := resolver.RecoverUnavailableValues(context.Background(), records, math.MaxInt64)
	return err
}

// testRecoverBatch recovers the values of records a prefix at a time, as the writer does; a call that fills nothing fails.
func testRecoverBatch(resolver *unavailableValuesResolver, records []types.RawRecord, limit int64) error {
	for start := 0; start < len(records); {
		n, err := resolver.RecoverUnavailableValues(context.Background(), records[start:], limit)
		if err != nil {
			return err
		}
		if n < 1 {
			return fmt.Errorf("recovering the records from %d on filled none", start)
		}
		start += n
	}
	return nil
}

// testCopyRecords copies records, so each thread fills its own.
func testCopyRecords(records []types.RawRecord) []types.RawRecord {
	copied := make([]types.RawRecord, 0, len(records))
	for _, record := range records {
		copied = append(copied, types.RawRecord{
			Data:               maps.Clone(record.Data),
			OlakeColumns:       maps.Clone(record.OlakeColumns),
			UnavailableColumns: slices.Clone(record.UnavailableColumns),
		})
	}
	return copied
}

// recoverStep is one RecoverUnavailableValues call, i.e. one batch. expected lists, per record,
// the columns to check afterwards; a nil entry skips that record.
type recoverStep struct {
	records  []types.RawRecord
	expected []map[string]any
}

func TestRecoverUnavailableValues(t *testing.T) {
	committed := map[string]types.RowLocation{"1": {FilePath: "a.parquet", Position: 0}}
	big := strings.Repeat("x", int(maxCachedBytes/2))

	testCases := []struct {
		name               string
		rawData            bool  // normalization off: the whole source row is stored as JSON
		limit              int64 // bytes per prefix; 0 recovers each batch as one prefix
		overBudget         bool  // the cache budget holds nothing, so every batch drops what it caches
		threads            int   // threads recovering each batch at once, each its own copy; 0 is one
		locations          map[string]types.RowLocation
		table              testTable
		steps              []recoverStep
		expectedLookups    int
		expectedRecovered  int64
		expectedFromTable  int64
		expectedUnresolved int64
	}{
		// the basic case: the previous version is committed, so the value is read back
		{
			name:      "reads the value of a committed row",
			locations: committed,
			table:     testTable{{"a.parquet", 0, "payload"}: "big value"},
			steps: []recoverStep{{
				records:  []types.RawRecord{testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload")},
				expected: []map[string]any{{"payload": "big value"}},
			}},
			expectedLookups:   1,
			expectedFromTable: 1,
			expectedRecovered: 1,
		},
		// the inserted row is not in the index yet; only the batch itself knows the value
		{
			name: "insert then update in one batch reuses the inserted value",
			steps: []recoverStep{{
				records: []types.RawRecord{
					testRecord("1", "i", map[string]any{"id": 1, "payload": "inserted"}),
					testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload"),
				},
				expected: []map[string]any{nil, {"payload": "inserted"}},
			}},
			expectedRecovered: 1,
		},
		// the index still points at the old version, so a lookup would return a stale value
		{
			name:      "a value changed earlier in the batch wins over the committed one",
			locations: committed,
			table:     testTable{{"a.parquet", 0, "payload"}: "old value"},
			steps: []recoverStep{{
				records: []types.RawRecord{
					testRecord("1", "u", map[string]any{"id": 1, "payload": "new value"}),
					testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload"),
				},
				expected: []map[string]any{nil, {"payload": "new value"}},
			}},
			expectedRecovered: 1,
		},
		// a value written after a queued read is newer; the read's answer must not replace it in the cached values
		{
			name:      "a read answer does not overwrite a newer value from the same batch",
			locations: committed,
			table:     testTable{{"a.parquet", 0, "payload"}: "old value"},
			steps: []recoverStep{
				{
					records: []types.RawRecord{
						testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload"),
						testRecord("1", "u", map[string]any{"id": 1, "payload": "new value"}),
					},
					expected: []map[string]any{{"payload": "old value"}, {"payload": "new value"}},
				},
				{
					records:  []types.RawRecord{testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload")},
					expected: []map[string]any{{"payload": "new value"}},
				},
			},
			expectedLookups:   1,
			expectedRecovered: 2,
			expectedFromTable: 1,
		},
		// a row deleted after a queued read stays deleted; the read's answer must not revive it
		{
			name:      "a read answer does not revive a row deleted later in the batch",
			locations: committed,
			table:     testTable{{"a.parquet", 0, "payload"}: "old value"},
			steps: []recoverStep{
				{
					records: []types.RawRecord{
						testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload"),
						testRecord("1", "d", map[string]any{"id": 1}),
					},
					expected: []map[string]any{{"payload": "old value"}, nil},
				},
				{
					records:  []types.RawRecord{testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload")},
					expected: []map[string]any{{"payload": placeholder}},
				},
			},
			expectedLookups:    1,
			expectedFromTable:  1,
			expectedUnresolved: 1,
			expectedRecovered:  1,
		},
		// the second record waits on the first one's read instead of issuing its own
		{
			name:      "two marked updates of one row share one read",
			locations: committed,
			table:     testTable{{"a.parquet", 0, "payload"}: "stored"},
			steps: []recoverStep{{
				records: []types.RawRecord{
					testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload"),
					testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload"),
				},
				expected: []map[string]any{{"payload": "stored"}, {"payload": "stored"}},
			}},
			expectedLookups:   1,
			expectedFromTable: 1,
			expectedRecovered: 2,
		},
		// the first change sends doc and leaves out payload, the second leaves out both: doc comes from the first
		{
			name:      "a later change of a row takes a column it leaves out from the change before",
			locations: committed,
			table:     testTable{{"a.parquet", 0, "payload"}: "stored payload", {"a.parquet", 0, "doc"}: "stored doc"},
			steps: []recoverStep{{
				records: []types.RawRecord{
					testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder, "doc": "new doc"}, "payload"),
					testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder, "doc": placeholder}, "payload", "doc"),
				},
				expected: []map[string]any{{"payload": "stored payload"}, {"payload": "stored payload", "doc": "new doc"}},
			}},
			expectedLookups:   1,
			expectedFromTable: 1,
			expectedRecovered: 3,
		},
		// a 1-byte limit is smaller than any record, so every call fills its first record alone and moves on
		{
			name:      "records larger than the limit are filled one per call",
			limit:     1,
			locations: map[string]types.RowLocation{"1": {FilePath: "a.parquet", Position: 0}, "2": {FilePath: "a.parquet", Position: 1}},
			table:     testTable{{"a.parquet", 0, "payload"}: "stored one", {"a.parquet", 1, "payload"}: "stored two"},
			steps: []recoverStep{{
				records: []types.RawRecord{
					testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload"),
					testRecord("2", "u", map[string]any{"id": 2, "payload": placeholder}, "payload"),
				},
				expected: []map[string]any{{"payload": "stored one"}, {"payload": "stored two"}},
			}},
			expectedLookups:   2,
			expectedFromTable: 2,
			expectedRecovered: 2,
		},
		// batches without a marker still update the cached values; skipping them would hand the
		// third batch the value read in the first one
		{
			name:      "a change in a later batch without markers replaces the cached value",
			locations: committed,
			table:     testTable{{"a.parquet", 0, "payload"}: "P1"},
			steps: []recoverStep{
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
			expectedLookups:   1,
			expectedFromTable: 1,
			expectedRecovered: 2,
		},
		// same as above for a delete followed by a re-insert
		{
			name:      "a delete and re-insert in a batch without markers replace the cached value",
			locations: committed,
			table:     testTable{{"a.parquet", 0, "payload"}: "P1"},
			steps: []recoverStep{
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
			expectedLookups:   1,
			expectedFromTable: 1,
			expectedRecovered: 2,
		},
		// a deleted row's columns are gone; its old values must not come back
		{
			name: "values from before a delete are not recovered",
			steps: []recoverStep{{
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
			steps: []recoverStep{{
				records: []types.RawRecord{
					testRecord("1", "d", map[string]any{"id": 1}),
					testRecord("1", "i", map[string]any{"id": 1, "payload": "re-inserted"}),
					testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload"),
				},
				expected: []map[string]any{nil, nil, {"payload": "re-inserted"}},
			}},
			expectedRecovered: 1,
		},
		// a filtered row, or an update that changed the primary key: the placeholder is cached, so it is looked up once
		{
			name: "a row with no destination version keeps the placeholder, looked up once",
			steps: []recoverStep{
				{
					records:  []types.RawRecord{testRecord("42", "u", map[string]any{"id": 42, "payload": placeholder}, "payload")},
					expected: []map[string]any{{"payload": placeholder}},
				},
				{
					records:  []types.RawRecord{testRecord("42", "u", map[string]any{"id": 42, "payload": placeholder}, "payload")},
					expected: []map[string]any{{"payload": placeholder}},
				},
			},
			expectedLookups:    1,
			expectedUnresolved: 2,
		},
		// nothing stays cached, so the row's next change reads its value from the destination again
		{
			name:       "a value no longer cached is read again from the destination",
			overBudget: true,
			locations:  committed,
			table:      testTable{{"a.parquet", 0, "payload"}: "stored"},
			steps: []recoverStep{
				{
					records:  []types.RawRecord{testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload")},
					expected: []map[string]any{{"payload": "stored"}},
				},
				{
					records:  []types.RawRecord{testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload")},
					expected: []map[string]any{{"payload": "stored"}},
				},
			},
			expectedLookups:   2,
			expectedFromTable: 2,
			expectedRecovered: 2,
		},
		// a 64 MB row fits a lone thread's share but not a quarter of the budget, so each thread's next batch drops it
		{
			name:    "threads caching at once share the cache budget",
			threads: 4,
			steps: []recoverStep{
				{
					records: []types.RawRecord{
						testRecord("big", "c", map[string]any{"payload": big}),
						testRecord("0", "u", map[string]any{"id": 0, "payload": placeholder}, "payload"),
					},
					expected: []map[string]any{nil, {"payload": placeholder}},
				},
				{
					records:  []types.RawRecord{testRecord("0", "u", map[string]any{"id": 0, "payload": placeholder}, "payload")},
					expected: []map[string]any{{"payload": placeholder}},
				},
			},
			expectedLookups:    1,
			expectedUnresolved: 2,
		},
		// the source marks the column, but the stream's selection dropped it from Data
		{
			name:      "a column left out of the stream selection is skipped",
			locations: committed,
			table:     testTable{{"a.parquet", 0, "payload"}: "stored"},
			steps: []recoverStep{{
				records:  []types.RawRecord{testRecord("1", "u", map[string]any{"id": 1}, "payload")},
				expected: []map[string]any{{"payload": nil}},
			}},
		},
		// the previous version was written before this feature; there is nothing better to write
		{
			name:      "a stored placeholder is not counted as recovered",
			locations: committed,
			table:     testTable{{"a.parquet", 0, "payload"}: placeholder},
			steps: []recoverStep{{
				records:  []types.RawRecord{testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload")},
				expected: []map[string]any{{"payload": placeholder}},
			}},
			expectedLookups:    1,
			expectedUnresolved: 1,
		},
		// an UPDATE moved a row onto a key deleted in an earlier sync: the index points at
		// the soft-delete row, whose columns are NULL, not the row's values
		{
			name:      "a soft-deleted stored row is not read as the row's values",
			locations: committed,
			table: testTable{
				{"a.parquet", 0, constants.OpType}: "d",
				{"a.parquet", 0, "payload"}:        nil,
			},
			steps: []recoverStep{{
				records:  []types.RawRecord{testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload")},
				expected: []map[string]any{{"payload": placeholder}},
			}},
			expectedLookups:    1,
			expectedUnresolved: 1,
		},
		// the stored row was written while the column was deselected, so it holds NULL; a
		// value the source could not send is never NULL, so the placeholder is kept
		{
			name:      "a stored NULL is not read as the row's value",
			locations: committed,
			table:     testTable{{"a.parquet", 0, "payload"}: nil},
			steps: []recoverStep{{
				records:  []types.RawRecord{testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload")},
				expected: []map[string]any{{"payload": placeholder}},
			}},
			expectedLookups:    1,
			expectedUnresolved: 1,
		},
		// the first read finds a file that predates the column; the row written next holds the
		// placeholder too, so a later change must not read again for a value no version has
		{
			name:      "a read that found nothing is not repeated by the next change",
			locations: committed,
			steps: []recoverStep{
				{
					records:  []types.RawRecord{testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload")},
					expected: []map[string]any{{"payload": placeholder}},
				},
				{
					records:  []types.RawRecord{testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload")},
					expected: []map[string]any{{"payload": placeholder}},
				},
			},
			expectedLookups:    1,
			expectedUnresolved: 2,
		},
		// both columns come out of the same stored JSON row; each must get its own key
		{
			name:      "without normalization each column is picked out of the stored row",
			rawData:   true,
			locations: committed,
			table:     testTable{{"a.parquet", 0, constants.StringifiedData}: `{"id":1,"payload":"stored value","doc":{"k":1}}`},
			steps: []recoverStep{{
				records: []types.RawRecord{
					testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder, "doc": placeholder}, "payload", "doc"),
				},
				expected: []map[string]any{{
					"payload": json.RawMessage(`"stored value"`),
					"doc":     json.RawMessage(`{"k":1}`),
				}},
			}},
			expectedLookups:   1,
			expectedFromTable: 2,
			expectedRecovered: 2,
		},
		// a JSON null, a stored placeholder and a missing key all mean no value was stored; the text "null" is one
		{
			name:      "without normalization only a stored value is recovered, not a JSON null, placeholder or missing key",
			rawData:   true,
			locations: committed,
			table:     testTable{{"a.parquet", 0, constants.StringifiedData}: `{"id":1,"nulled":null,"text":"null","old":"` + placeholder + `"}`},
			steps: []recoverStep{{
				records: []types.RawRecord{
					testRecord("1", "u", map[string]any{"id": 1, "nulled": placeholder, "text": placeholder, "old": placeholder, "added": placeholder},
						"nulled", "text", "old", "added"),
				},
				expected: []map[string]any{{
					"nulled": placeholder,
					"text":   json.RawMessage(`"null"`),
					"old":    placeholder,
					"added":  placeholder,
				}},
			}},
			expectedLookups:    1,
			expectedFromTable:  1,
			expectedRecovered:  1,
			expectedUnresolved: 3,
		},
		// streams that never produce a marker must cost nothing
		{
			name:      "a batch without markers does nothing",
			locations: committed,
			steps: []recoverStep{{
				records:  []types.RawRecord{testRecord("1", "u", map[string]any{"id": 1, "payload": "value"})},
				expected: []map[string]any{{"payload": "value"}},
			}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.overBudget {
				testOverBudget(t)
			}
			limit := tc.limit
			if limit == 0 {
				limit = math.MaxInt64
			}
			writers := make([]*testWriter, max(tc.threads, 1))
			resolvers := make([]*unavailableValuesResolver, len(writers))
			for idx := range writers {
				writers[idx] = &testWriter{locations: maps.Clone(tc.locations)}
				resolvers[idx] = testResolver(t, !tc.rawData, writers[idx], &testTableIndexClient{table: tc.table})
			}

			for stepIdx, step := range tc.steps {
				batches := make([][]types.RawRecord, len(resolvers))
				errs := make([]error, len(resolvers))
				var wg sync.WaitGroup
				for idx, resolver := range resolvers {
					batches[idx] = testCopyRecords(step.records)
					wg.Go(func() { errs[idx] = testRecoverBatch(resolver, batches[idx], limit) })
				}
				wg.Wait()

				for idx, batch := range batches {
					require.NoError(t, errs[idx])
					for recordIdx, columns := range step.expected {
						for column, expected := range columns {
							assert.Equal(t, expected, batch[recordIdx].Data[column], "batch %d, record %d, column %s", stepIdx, recordIdx, column)
						}
					}
				}
			}

			cached := int64(0)
			for idx, resolver := range resolvers {
				assert.Equal(t, tc.expectedLookups, writers[idx].lookups, "lookups")
				assert.Equal(t, tc.expectedRecovered, resolver.recovered, "recovered")
				assert.Equal(t, tc.expectedFromTable, resolver.fromTable, "from table")
				assert.Equal(t, tc.expectedUnresolved, resolver.unresolved, "unresolved")
				assert.Zero(t, resolver.tableValues.bytes, "every value read is filled into its record")
				cached += resolver.cachedValues.bytes
			}
			assert.LessOrEqual(t, cached, maxCachedBytes, "the threads' caches stay within the budget together")
		})
	}
}

// A row too large for one message comes back in parts; each part must be merged into the row,
// and a deleted row must stay unrecovered whichever part arrives first.
func TestRecoverUnavailableValuesRowInParts(t *testing.T) {
	testCases := []struct {
		name               string
		opType             string
		expected           map[string]any
		expectedRecovered  int64
		expectedUnresolved int64
	}{
		{
			name:              "every part of a live row is recovered",
			opType:            "u",
			expected:          map[string]any{"payload": "big value", "doc": "big doc"},
			expectedRecovered: 2,
		},
		{
			name:               "no part of a deleted row is recovered",
			opType:             "d",
			expected:           map[string]any{"payload": placeholder, "doc": placeholder},
			expectedUnresolved: 2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			writer := &testWriter{locations: map[string]types.RowLocation{"1": {FilePath: "a.parquet", Position: 0}}}
			table := testTable{
				{"a.parquet", 0, constants.OpType}: tc.opType,
				{"a.parquet", 0, "payload"}:        "big value",
				{"a.parquet", 0, "doc"}:            "big doc",
			}
			resolver := testResolver(t, true, writer, &testTableIndexClient{table: table, inParts: true})

			records := []types.RawRecord{testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder, "doc": placeholder}, "payload", "doc")}
			require.NoError(t, testRecoverAll(resolver, records))

			for column, expected := range tc.expected {
				assert.Equal(t, expected, records[0].Data[column], column)
			}
			assert.Equal(t, tc.expectedRecovered, resolver.recovered, "recovered")
			assert.Equal(t, tc.expectedUnresolved, resolver.unresolved, "unresolved")
		})
	}
}

// Two source columns that share one destination column (Foo and foo) both read it, from each
// part it comes in, and leave no read bytes counted once the record is filled.
func TestRecoverUnavailableValuesColumnsSharingDestinationName(t *testing.T) {
	writer := &testWriter{locations: map[string]types.RowLocation{"1": {FilePath: "a.parquet", Position: 0}}}
	table := testTable{
		{"a.parquet", 0, constants.OpType}: "u",
		{"a.parquet", 0, "foo"}:            "big value",
	}
	resolver := testResolver(t, true, writer, &testTableIndexClient{table: table, inParts: true})
	resolver.stream = &types.ConfiguredStream{StreamMetadata: types.StreamMetadata{Normalization: new(true)}}

	records := []types.RawRecord{testRecord("1", "u", map[string]any{"id": 1, "Foo": placeholder, "foo": placeholder}, "Foo", "foo")}
	require.NoError(t, testRecoverAll(resolver, records))

	assert.Equal(t, "big value", records[0].Data["Foo"])
	assert.Equal(t, "big value", records[0].Data["foo"])
	assert.Zero(t, resolver.tableValues.bytes, "table values bytes")
}

// A batch whose recovered values pass the limit is recovered a prefix at a time: a prefix passes
// the limit only as one record, what is read ahead stays within the limit too, and every value
// is recovered once, whatever order the rows arrive in.
func TestRecoverUnavailableValuesPrefixStaysWithinLimit(t *testing.T) {
	const limit = 300

	writer := &testWriter{locations: map[string]types.RowLocation{}}
	table := testTable{}
	var records []types.RawRecord
	expected := map[string]string{}
	for idx := range 40 {
		olakeID := strconv.Itoa(idx)
		location := types.RowLocation{FilePath: fmt.Sprintf("%d.parquet", idx%4), Position: int64(idx)}
		value := strings.Repeat("x", 10+(idx*37)%250)
		writer.locations[olakeID] = location
		table[testCell{location.FilePath, location.Position, "payload"}] = value
		expected[olakeID] = value
		records = append(records, testRecord(olakeID, "u", map[string]any{"id": idx, "payload": placeholder}, "payload"))
	}
	tableIndexClient := &testTableIndexClient{table: table, reverse: true}
	resolver := testResolver(t, true, writer, tableIndexClient)
	// the cached values are trimmed to nothing after every prefix; the table values must survive that
	testOverBudget(t)

	for start := 0; start < len(records); {
		n, err := resolver.RecoverUnavailableValues(context.Background(), records[start:], limit)
		require.NoError(t, err)
		require.Positive(t, n)

		size := int64(0)
		for _, record := range records[start : start+n] {
			assert.Equal(t, expected[record.OlakeColumns[constants.OlakeID].(string)], record.Data["payload"])
			for _, value := range record.Data {
				size += valueSize(value)
			}
		}
		if n > 1 {
			assert.LessOrEqual(t, size, int64(limit), "prefix at %d", start)
		}
		// a read stops after the row that passes the limit, of at most 259 bytes here
		assert.LessOrEqual(t, resolver.tableValues.bytes, int64(limit+259), "read ahead at %d", start)
		start += n
	}

	assert.Greater(t, len(tableIndexClient.requests), 1, "the batch is read a window at a time")
	for _, request := range tableIndexClient.requests {
		// a window is read within what is left of the limit; a lone first record whatever its size
		assert.True(t, request.GetMaxBytes() <= limit || request.GetMaxBytes() == math.MaxInt64, "max_bytes %d", request.GetMaxBytes())
	}
	assert.Equal(t, int64(len(records)), resolver.recovered)
	assert.Equal(t, int64(len(records)), resolver.fromTable, "no value is read twice")
	assert.Zero(t, resolver.tableValues.bytes)
}

// A read leaves out of its budget what the records before it filled from the cached values, so the prefix and
// the values read ahead for later records stay within the limit together, past it by at most one row.
func TestRecoverUnavailableValuesReadLeavesRoomForCachedRecords(t *testing.T) {
	const limit, valueBytes = 600, 100
	value := strings.Repeat("v", valueBytes)

	writer := &testWriter{locations: map[string]types.RowLocation{}}
	table := testTable{}
	cached := []types.RawRecord{testRecord("marker", "u", map[string]any{"id": 0, "payload": placeholder}, "payload")}
	var records []types.RawRecord
	for idx := range 5 {
		cached = append(cached, testRecord("c"+strconv.Itoa(idx), "c", map[string]any{"id": idx, "payload": value}))
		records = append(records, testRecord("c"+strconv.Itoa(idx), "u", map[string]any{"id": idx, "payload": placeholder}, "payload"))
	}
	for idx := range 5 {
		olakeID := "r" + strconv.Itoa(idx)
		writer.locations[olakeID] = types.RowLocation{FilePath: "a.parquet", Position: int64(idx)}
		table[testCell{"a.parquet", int64(idx), "payload"}] = value
		records = append(records, testRecord(olakeID, "u", map[string]any{"id": idx, "payload": placeholder}, "payload"))
	}
	tableIndexClient := &testTableIndexClient{table: table}
	resolver := testResolver(t, true, writer, tableIndexClient)
	require.NoError(t, testRecoverAll(resolver, cached))

	n, err := resolver.RecoverUnavailableValues(context.Background(), records, limit)
	require.NoError(t, err)

	prefix := int64(0)
	for _, record := range records[:n] {
		prefix += dataSize(record.Data)
	}
	assert.Equal(t, 5, n, "the cached records fill the prefix")
	require.Len(t, tableIndexClient.requests, 1)
	assert.Equal(t, limit-prefix, tableIndexClient.requests[0].GetMaxBytes(), "the read gets only what the cached records left")
	assert.LessOrEqual(t, prefix+resolver.tableValues.bytes, int64(limit+valueBytes))
}

// A read stopped by the limit in the middle of a row split in parts keeps the parts it got,
// and the row's next read asks only for the columns still missing.
func TestRecoverUnavailableValuesRowCutInParts(t *testing.T) {
	writer := &testWriter{locations: map[string]types.RowLocation{"1": {FilePath: "a.parquet", Position: 0}}}
	table := testTable{{"a.parquet", 0, "payload"}: "big value", {"a.parquet", 0, "doc"}: "big doc"}
	tableIndexClient := &testTableIndexClient{table: table, inParts: true}
	resolver := testResolver(t, true, writer, tableIndexClient)
	records := []types.RawRecord{testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder, "doc": placeholder}, "payload", "doc")}

	// the read is cut after the payload part: the _op_type part is 25 serialized bytes, the payload part 35
	const limit = 32
	n, err := resolver.RecoverUnavailableValues(context.Background(), records, limit)

	require.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Equal(t, "big value", records[0].Data["payload"])
	assert.Equal(t, "big doc", records[0].Data["doc"])
	require.Len(t, tableIndexClient.requests, 2)
	assert.Equal(t, int64(limit), tableIndexClient.requests[0].GetMaxBytes(), "the read ahead stops Java at the limit")
	assert.Equal(t, int64(math.MaxInt64), tableIndexClient.requests[1].GetMaxBytes(), "the lone first record is read whole")
	assert.Len(t, tableIndexClient.requests[1].GetFiles()[0].GetRows()[0].GetColumns(), 2, "_op_type and the one column still missing")
}

// The read starts at the first record missing a value and answers it, so the records after it
// stay in the same prefix.
func TestRecoverUnavailableValuesReadMidBatchKeepsOnePrefix(t *testing.T) {
	writer := &testWriter{locations: map[string]types.RowLocation{"2": {FilePath: "a.parquet", Position: 0}}}
	resolver := testResolver(t, true, writer, &testTableIndexClient{table: testTable{{"a.parquet", 0, "payload"}: "stored value"}})
	records := []types.RawRecord{
		testRecord("1", "c", map[string]any{"id": 1, "payload": "inserted"}),
		testRecord("2", "u", map[string]any{"id": 2, "payload": placeholder}, "payload"),
		testRecord("3", "c", map[string]any{"id": 3, "payload": "inserted"}),
	}

	n, err := resolver.RecoverUnavailableValues(context.Background(), records, math.MaxInt64)

	require.NoError(t, err)
	assert.Equal(t, len(records), n)
	assert.Equal(t, "stored value", records[1].Data["payload"])
}

// Without normalization a large row's _op_type can come alone, before the part holding the
// stored row; that first part must not be taken as the whole row.
func TestRecoverUnavailableValuesRawRowCutAfterOpType(t *testing.T) {
	writer := &testWriter{locations: map[string]types.RowLocation{"1": {FilePath: "a.parquet", Position: 0}}}
	table := testTable{
		{"a.parquet", 0, constants.OpType}:          "u",
		{"a.parquet", 0, constants.StringifiedData}: `{"id":1,"payload":"stored value"}`,
	}
	tableIndexClient := &testTableIndexClient{table: table, inParts: true}
	resolver := testResolver(t, false, writer, tableIndexClient)
	records := []types.RawRecord{testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload")}

	n, err := resolver.RecoverUnavailableValues(context.Background(), records, 10)

	require.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Equal(t, json.RawMessage(`"stored value"`), records[0].Data["payload"])
	assert.Equal(t, int64(1), resolver.recovered)
	assert.Zero(t, resolver.tableValues.bytes)
}

func TestRecoverUnavailableValuesReadRequest(t *testing.T) {
	locations := map[string]types.RowLocation{
		"1": {FilePath: "b.parquet", Position: 5},
		"2": {FilePath: "a.parquet", Position: 9},
		"3": {FilePath: "a.parquet", Position: 2},
	}
	// a function, as RecoverUnavailableValues fills the records it is given
	marked := func() []types.RawRecord {
		return []types.RawRecord{
			testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload"),
			testRecord("2", "u", map[string]any{"id": 2, "payload": placeholder, "doc": placeholder}, "payload", "doc"),
			testRecord("3", "u", map[string]any{"id": 3, "doc": placeholder}, "doc"),
		}
	}

	testCases := []struct {
		name            string
		rawData         bool // normalization off: the whole source row is stored as JSON
		locations       map[string]types.RowLocation
		table           testTable
		records         []types.RawRecord
		expectedRows    map[string]map[int64][]string // file -> position -> columns read
		expectedFlushes [][]string
	}{
		// one call for these rows, grouped by file, each row asking only for its own columns
		{
			name:      "each row reads only the columns it is missing",
			locations: locations,
			records:   marked(),
			expectedRows: map[string]map[int64][]string{
				"a.parquet": {9: {constants.OpType, "payload", "doc"}, 2: {constants.OpType, "doc"}},
				"b.parquet": {5: {constants.OpType, "payload"}},
			},
			expectedFlushes: [][]string{{"b.parquet", "a.parquet"}},
		},
		// every source column lives in the one JSON column, read once per row
		{
			name:      "without normalization each row reads the stored row once",
			rawData:   true,
			locations: locations,
			table: testTable{
				{"a.parquet", 2, constants.StringifiedData}: `{"id":3}`,
				{"a.parquet", 9, constants.StringifiedData}: `{"id":2}`,
				{"b.parquet", 5, constants.StringifiedData}: `{"id":1}`,
			},
			records: marked(),
			expectedRows: map[string]map[int64][]string{
				"a.parquet": {9: {constants.OpType, constants.StringifiedData}, 2: {constants.OpType, constants.StringifiedData}},
				"b.parquet": {5: {constants.OpType, constants.StringifiedData}},
			},
			expectedFlushes: [][]string{{"b.parquet", "a.parquet"}},
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
			tableIndexClient := &testTableIndexClient{table: tc.table}
			resolver := testResolver(t, !tc.rawData, writer, tableIndexClient)

			require.NoError(t, testRecoverAll(resolver, tc.records))

			assert.Equal(t, tc.expectedFlushes, writer.flushes, "files made readable")
			if tc.expectedRows == nil {
				assert.Empty(t, tableIndexClient.requests)
				return
			}
			require.Len(t, tableIndexClient.requests, 1)
			request := tableIndexClient.requests[0]
			assert.Equal(t, "test-thread", request.GetThreadId())

			// files are in the order the records need them, and rows within a file in position order
			actual := make(map[string]map[int64][]string)
			var paths []string
			for _, file := range request.GetFiles() {
				paths = append(paths, file.GetFilePath())
				actual[file.GetFilePath()] = make(map[int64][]string)
				var positions []int64
				for _, row := range file.GetRows() {
					positions = append(positions, row.GetPosition())
					actual[file.GetFilePath()][row.GetPosition()] = row.GetColumns()
				}
				assert.True(t, slices.IsSorted(positions), "rows of %s in position order", file.GetFilePath())
			}
			assert.Equal(t, []string{"b.parquet", "a.parquet"}, paths)
			assert.Equal(t, tc.expectedRows, actual)
		})
	}
}

func TestRecoverUnavailableValuesOpenFiles(t *testing.T) {
	testCases := []struct {
		name            string
		writer          func(files *testWriter) Writer
		expectedFlushes [][]string
	}{
		// the arrow writer holds data files open on the Go side, so it is asked to close them
		{
			name: "arrow writer closes the files to read",
			writer: func(files *testWriter) Writer {
				return struct {
					Writer
					*testWriter
				}{testWriter: files}
			},
			expectedFlushes: [][]string{{"a.parquet"}},
		},
		// the legacy writer's files are closed by Java when ReadRows asks for them
		{
			name:   "legacy writer leaves open files to Java",
			writer: func(*testWriter) Writer { return struct{ Writer }{} },
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			files := &testWriter{locations: map[string]types.RowLocation{"1": {FilePath: "a.parquet", Position: 0}}}
			resolver := newUnavailableValuesResolver("test-thread", testConfiguredStream(true), tc.writer(files),
				types.NewStreamIndexThread(testIndex{writer: files}), &testTableIndexClient{table: testTable{{"a.parquet", 0, "payload"}: "stored"}})
			t.Cleanup(resolver.Close)
			records := []types.RawRecord{testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload")}

			require.NoError(t, testRecoverAll(resolver, records))

			assert.Equal(t, "stored", records[0].Data["payload"])
			assert.Equal(t, tc.expectedFlushes, files.flushes, "files made readable")
		})
	}
}

func TestRecoverUnavailableValuesErrors(t *testing.T) {
	failure := errors.New("boom")

	// every failure is wrapped with %w, so the destination error classifier can still see it
	testCases := []struct {
		name             string
		writer           *testWriter
		tableIndexClient *testTableIndexClient
		message          string
	}{
		{
			name:             "index lookup fails",
			writer:           &testWriter{lookupErr: failure},
			tableIndexClient: &testTableIndexClient{},
			message:          "failed to look up row[1] in index",
		},
		{
			name:             "open file cannot be closed",
			writer:           &testWriter{ensureErr: failure},
			tableIndexClient: &testTableIndexClient{},
			message:          "boom",
		},
		{
			name:             "read call fails",
			writer:           &testWriter{},
			tableIndexClient: &testTableIndexClient{readErr: failure},
			message:          "failed to read unavailable column values",
		},
		{
			name:             "read stream breaks",
			writer:           &testWriter{},
			tableIndexClient: &testTableIndexClient{table: testTable{}, recvErr: failure},
			message:          "failed to read unavailable column values",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.writer.locations = map[string]types.RowLocation{"1": {FilePath: "a.parquet", Position: 0}}
			resolver := testResolver(t, true, tc.writer, tc.tableIndexClient)
			records := []types.RawRecord{testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload")}

			err := testRecoverAll(resolver, records)

			require.Error(t, err)
			assert.ErrorIs(t, err, failure)
			assert.Contains(t, err.Error(), tc.message)
			assert.Equal(t, placeholder, records[0].Data["payload"], "a failed batch writes nothing")
		})
	}
}

func TestRecoverUnavailableValuesWriteCacheBudget(t *testing.T) {
	testCases := []struct {
		name               string
		overBudget         bool
		records            []types.RawRecord
		expectedCachedRows int
	}{
		// the values are kept for the next change to the row
		{
			name: "under budget the written values are cached",
			records: []types.RawRecord{
				testRecord("1", "i", map[string]any{"id": 1, "payload": "inserted"}),
				testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload"),
			},
			expectedCachedRows: 1,
		},
		// dropping costs nothing: the rows are read back only if a later change asks for them
		{
			name:       "over budget the cached values are dropped",
			overBudget: true,
			records: []types.RawRecord{
				testRecord("1", "i", map[string]any{"id": 1, "payload": "inserted"}),
				testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload"),
			},
		},
		// a delete-heavy stream must not grow the cached values without bound
		{
			name:       "deleted rows count against the budget",
			overBudget: true,
			records: []types.RawRecord{
				testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload"),
				testRecord("2", "d", map[string]any{"id": 2}),
				testRecord("3", "d", map[string]any{"id": 3}),
			},
		},
		{
			name:    "a stream that never marks a column caches nothing",
			records: []types.RawRecord{testRecord("1", "u", map[string]any{"id": 1, "payload": "value"})},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			writer := &testWriter{locations: map[string]types.RowLocation{}}
			resolver := testResolver(t, true, writer, &testTableIndexClient{table: testTable{}})
			if tc.overBudget {
				testOverBudget(t)
			}

			require.NoError(t, testRecoverAll(resolver, tc.records))

			assert.Len(t, resolver.cachedValues.rows, tc.expectedCachedRows)
			if tc.expectedCachedRows == 0 {
				assert.Zero(t, resolver.cachedValues.bytes)
			}
			assert.Empty(t, writer.flushes, "no file is closed to drop or keep cached values")
		})
	}
}

func TestUnavailableValuesResolverCloseReleasesBudget(t *testing.T) {
	before := cachingThreads.Load()
	resolver := testResolver(t, true, &testWriter{locations: map[string]types.RowLocation{}}, &testTableIndexClient{table: testTable{}})

	require.NoError(t, testRecoverAll(resolver, []types.RawRecord{
		testRecord("1", "i", map[string]any{"id": 1, "payload": "inserted"}),
		testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload"),
	}))
	require.Equal(t, before+1, cachingThreads.Load(), "a thread that caches takes a share of the budget")

	resolver.Close()

	assert.Equal(t, before, cachingThreads.Load(), "the thread gives its share back")
	assert.Nil(t, resolver.cachedValues, "the cached values are freed")
}
