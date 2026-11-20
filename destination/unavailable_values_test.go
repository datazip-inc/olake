package destination

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
)

// placeholder is what the source writes for a column it could not send.
const placeholder = constants.UnavailableValue

// testTable is what the destination holds, by olake id and column. A nil value is a stored NULL; a column
// missing from a row is one the data file does not carry, which the destination sends as NULL too; a row
// missing from the table has no previous version.
type testTable map[string]map[string]any

// testParts is a row as the destination sends it: whole, or a part per column.
type testParts struct {
	row   int
	parts []map[string]any
}

// testRowReader answers ReadRows out of a testTable, standing in for the Iceberg side, and keeps
// the rows of every call it received. It counts the bytes it sends as the column names and values of each row.
type testRowReader struct {
	table testTable
	err   error
	reads [][]RowRead
	// inParts sends each row the way the Java reader sends a large one: a part per column
	inParts bool
	// maxBytes ends the read, as the Java reader does, after the row that takes the bytes sent past it; 0 is no limit
	maxBytes int64
}

func (f *testRowReader) ReadRows(_ context.Context, rows []RowRead, emit func(int, map[string]any) error) error {
	f.reads = append(f.reads, rows)
	if f.err != nil {
		return f.err
	}

	var found []testParts
	for idx, row := range rows {
		stored, ok := f.table[row.OlakeID]
		if !ok {
			if err := emit(idx, nil); err != nil { // no previous version: answered before anything is read
				return err
			}
			continue
		}
		deleted := stored[constants.OpType] == "d"
		values := make(map[string]any, len(row.Columns))
		for _, column := range row.Columns {
			if !deleted || column == constants.OpType { // as the Java reader: a soft-deleted row's columns are NULL
				values[column] = stored[column]
			} else {
				values[column] = nil
			}
		}
		parts := []map[string]any{values}
		if f.inParts {
			parts = nil
			for _, column := range row.Columns {
				parts = append(parts, map[string]any{column: values[column]})
			}
		}
		found = append(found, testParts{row: idx, parts: parts})
	}
	// as the Java reader, the read ends after the row, whole, that takes the bytes sent past maxBytes
	sent := int64(0)
	for _, row := range found {
		for _, part := range row.parts {
			if err := emit(row.row, part); err != nil {
				return err
			}
			for column, value := range part {
				sent += int64(len(column)) + valueSize(value)
			}
		}
		if f.maxBytes > 0 && sent > f.maxBytes {
			return nil
		}
	}
	return nil
}

// asked is how many rows were asked of the destination, over every call.
func (f *testRowReader) asked() int {
	asked := 0
	for _, read := range f.reads {
		asked += len(read)
	}
	return asked
}

// testConfiguredStream is a stream that keeps source column names, so destination columns
// in the tests read the same as the source ones.
func testConfiguredStream(normalized bool) *types.ConfiguredStream {
	return &types.ConfiguredStream{
		Stream:         &types.Stream{Name: "test", Namespace: "public"},
		StreamMetadata: types.StreamMetadata{Normalization: new(normalized), UseSourceColumnNames: true},
	}
}

func testValueFetcher(t *testing.T, normalized bool, reader *testRowReader) *destinationValuesFetcher {
	t.Helper()

	writer := struct {
		Writer
		*testRowReader
	}{testRowReader: reader}
	fetcher := newDestinationValuesFetcher(testConfiguredStream(normalized), writer, struct{ types.StreamIndex }{})
	t.Cleanup(fetcher.Close)

	return fetcher
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

// testRecoverAll recovers the values of records in one call, as a batch the destination answers whole is.
func testRecoverAll(fetcher *destinationValuesFetcher, records []types.RawRecord) error {
	_, err := fetcher.RecoverUnavailableValues(context.Background(), records)
	return err
}

// testRecoverBatch recovers the values of records a prefix at a time, as the writer does; a call that fills nothing fails.
func testRecoverBatch(fetcher *destinationValuesFetcher, records []types.RawRecord) error {
	for start := 0; start < len(records); {
		n, err := fetcher.RecoverUnavailableValues(context.Background(), records[start:])
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

// recoverStep is one RecoverUnavailableValues call, i.e. one batch. expected lists, per record,
// the columns to check afterwards; a nil entry skips that record.
type recoverStep struct {
	records  []types.RawRecord
	expected []map[string]any
}

func TestRecoverUnavailableValues(t *testing.T) {
	big := strings.Repeat("x", int(maxCachedBytes/2))

	testCases := []struct {
		name                string
		rawData             bool  // normalization off: the whole source row is stored as JSON
		limit               int64 // bytes the destination sends per read before it stops; 0 is no limit
		overBudget          bool  // the cache budget holds nothing, so every batch drops what it caches
		threads             int   // threads recovering each batch at once, each its own copy; 0 is one
		table               testTable
		steps               []recoverStep
		expectedAsked       int // rows asked of the destination
		expectedRecovered   int64
		expectedFromTable   int64
		expectedUnrecovered int64
	}{
		// the basic case: the previous version is committed, so the value is read back
		{
			name:  "reads the value of a committed row",
			table: testTable{"1": {"payload": "big value"}},
			steps: []recoverStep{{
				records:  []types.RawRecord{testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload")},
				expected: []map[string]any{{"payload": "big value"}},
			}},
			expectedAsked:     1,
			expectedFromTable: 1,
			expectedRecovered: 1,
		},
		// the inserted row is not in the destination yet; only the batch itself knows the value
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
		// the destination still holds the old version, so a read would return a stale value
		{
			name:  "a value changed earlier in the batch wins over the committed one",
			table: testTable{"1": {"payload": "old value"}},
			steps: []recoverStep{{
				records: []types.RawRecord{
					testRecord("1", "u", map[string]any{"id": 1, "payload": "new value"}),
					testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload"),
				},
				expected: []map[string]any{nil, {"payload": "new value"}},
			}},
			expectedRecovered: 1,
		},
		// a value written after a queued read is newer; the read's answer must not replace it in the cache
		{
			name:  "a read answer does not overwrite a newer value from the same batch",
			table: testTable{"1": {"payload": "old value"}},
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
			expectedAsked:     1,
			expectedRecovered: 2,
			expectedFromTable: 1,
		},
		// a row deleted after a queued read stays deleted; the read's answer must not revive it
		{
			name:  "a read answer does not revive a row deleted later in the batch",
			table: testTable{"1": {"payload": "old value"}},
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
			expectedAsked:       1,
			expectedFromTable:   1,
			expectedUnrecovered: 1,
			expectedRecovered:   1,
		},
		// the second record waits on the first one's read instead of issuing its own
		{
			name:  "two marked updates of one row share one read",
			table: testTable{"1": {"payload": "stored"}},
			steps: []recoverStep{{
				records: []types.RawRecord{
					testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload"),
					testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload"),
				},
				expected: []map[string]any{{"payload": "stored"}, {"payload": "stored"}},
			}},
			expectedAsked:     1,
			expectedFromTable: 1,
			expectedRecovered: 2,
		},
		// the first change sends doc and leaves out payload, the second leaves out both: doc comes from the first
		{
			name:  "a later change of a row takes a column it leaves out from the change before",
			table: testTable{"1": {"payload": "stored payload", "doc": "stored doc"}},
			steps: []recoverStep{{
				records: []types.RawRecord{
					testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder, "doc": "new doc"}, "payload"),
					testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder, "doc": placeholder}, "payload", "doc"),
				},
				expected: []map[string]any{{"payload": "stored payload"}, {"payload": "stored payload", "doc": "new doc"}},
			}},
			expectedAsked:     1,
			expectedFromTable: 1,
			expectedRecovered: 3,
		},
		// a 1-byte limit stops the destination after its first row, so every call fills its first record and moves on;
		// the first call asks for both rows, the destination answers the first, the second call asks again
		{
			name:  "records the read did not answer are filled by the next call",
			limit: 1,
			table: testTable{"1": {"payload": "stored one"}, "2": {"payload": "stored two"}},
			steps: []recoverStep{{
				records: []types.RawRecord{
					testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload"),
					testRecord("2", "u", map[string]any{"id": 2, "payload": placeholder}, "payload"),
				},
				expected: []map[string]any{{"payload": "stored one"}, {"payload": "stored two"}},
			}},
			expectedAsked:     3,
			expectedFromTable: 2,
			expectedRecovered: 2,
		},
		// the value this thread wrote is cached, so the row's next change needs no read
		{
			name:  "a later batch takes the value from the cache",
			table: testTable{"1": {"payload": "stored"}},
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
			expectedAsked:     1,
			expectedFromTable: 1,
			expectedRecovered: 2,
		},
		// nothing stays cached, so the row's next change reads its value from the destination again
		{
			name:       "a value no longer cached is read again from the destination",
			overBudget: true,
			table:      testTable{"1": {"payload": "stored"}},
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
			expectedAsked:     2,
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
			expectedAsked:       1,
			expectedUnrecovered: 2,
		},
		// batches without a marker still update the cache; skipping them would hand the
		// third batch the value read in the first one
		{
			name:  "a change in a later batch without markers replaces the cached value",
			table: testTable{"1": {"payload": "P1"}},
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
			expectedAsked:     1,
			expectedFromTable: 1,
			expectedRecovered: 2,
		},
		// same as above for a delete followed by a re-insert
		{
			name:  "a delete and re-insert in a batch without markers replace the cached value",
			table: testTable{"1": {"payload": "P1"}},
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
			expectedAsked:     1,
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
			expectedUnrecovered: 1,
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
		// a filtered row, or an update that changed the primary key: the placeholder is cached, so it is asked for once
		{
			name: "a row with no destination version keeps the placeholder, asked for once",
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
			expectedAsked:       1,
			expectedUnrecovered: 2,
		},
		// the previous version was written before this feature; there is nothing better to write
		{
			name:  "a stored placeholder is not counted as recovered",
			table: testTable{"1": {"payload": placeholder}},
			steps: []recoverStep{{
				records:  []types.RawRecord{testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload")},
				expected: []map[string]any{{"payload": placeholder}},
			}},
			expectedAsked:       1,
			expectedUnrecovered: 1,
		},
		// an UPDATE moved a row onto a key deleted in an earlier sync: the destination's latest version
		// is the soft-delete row, whose columns come back NULL, not the row's values
		{
			name:  "a soft-deleted stored row is not read as the row's values",
			table: testTable{"1": {constants.OpType: "d", "payload": nil}},
			steps: []recoverStep{{
				records:  []types.RawRecord{testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload")},
				expected: []map[string]any{{"payload": placeholder}},
			}},
			expectedAsked:       1,
			expectedUnrecovered: 1,
		},
		// the stored row was written while the column was deselected, so it holds NULL; a
		// value the source could not send is never NULL, so the placeholder is kept
		{
			name:  "a stored NULL is not read as the row's value",
			table: testTable{"1": {"payload": nil}},
			steps: []recoverStep{{
				records:  []types.RawRecord{testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload")},
				expected: []map[string]any{{"payload": placeholder}},
			}},
			expectedAsked:       1,
			expectedUnrecovered: 1,
		},
		// the first read finds a file that predates the column; the row written next holds the
		// placeholder too, so a later change must not read again for a value no version has
		{
			name:  "a read that found nothing is not repeated by the next change",
			table: testTable{"1": {}},
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
			expectedAsked:       1,
			expectedUnrecovered: 2,
		},
		// both columns come out of the same stored JSON row; each must get its own key
		{
			name:    "without normalization each column is picked out of the stored row",
			rawData: true,
			table:   testTable{"1": {constants.StringifiedData: `{"id":1,"payload":"stored value","doc":{"k":1}}`}},
			steps: []recoverStep{{
				records: []types.RawRecord{
					testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder, "doc": placeholder}, "payload", "doc"),
				},
				expected: []map[string]any{{
					"payload": json.RawMessage(`"stored value"`),
					"doc":     json.RawMessage(`{"k":1}`),
				}},
			}},
			expectedAsked:     1,
			expectedFromTable: 2,
			expectedRecovered: 2,
		},
		// a JSON null, a stored placeholder and a missing key all mean no value was stored; the text "null" is one
		{
			name:    "without normalization only a stored value is recovered, not a JSON null, placeholder or missing key",
			rawData: true,
			table:   testTable{"1": {constants.StringifiedData: `{"id":1,"nulled":null,"text":"null","old":"` + placeholder + `"}`}},
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
			expectedAsked:       1,
			expectedFromTable:   1,
			expectedRecovered:   1,
			expectedUnrecovered: 3,
		},
		// the row is read once; after its delete later in the batch, the old value must not come back
		{
			name:  "an update after a delete of the row in the same batch keeps the placeholder",
			table: testTable{"1": {"payload": "old"}},
			steps: []recoverStep{{
				records: []types.RawRecord{
					testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload"),
					testRecord("1", "d", map[string]any{"id": 1}),
					testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload"),
				},
				expected: []map[string]any{{"payload": "old"}, nil, {"payload": placeholder}},
			}},
			expectedAsked:       1,
			expectedFromTable:   1,
			expectedRecovered:   1,
			expectedUnrecovered: 1,
		},
		// streams that never produce a marker must cost nothing
		{
			name:  "a batch without markers does nothing",
			table: testTable{"1": {"payload": "stored"}},
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
			readers := make([]*testRowReader, max(tc.threads, 1))
			threads := make([]*destinationValuesFetcher, len(readers))
			for idx := range readers {
				readers[idx] = &testRowReader{table: tc.table, maxBytes: tc.limit}
				threads[idx] = testValueFetcher(t, !tc.rawData, readers[idx])
			}

			for stepIdx, step := range tc.steps {
				batches := make([][]types.RawRecord, len(threads))
				errs := make([]error, len(threads))
				var wg sync.WaitGroup
				for idx, fetcher := range threads {
					batches[idx] = testCopyRecords(step.records)
					wg.Go(func() { errs[idx] = testRecoverBatch(fetcher, batches[idx]) })
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
			for idx, fetcher := range threads {
				assert.Equal(t, tc.expectedAsked, readers[idx].asked(), "rows asked of the destination")
				assert.Equal(t, tc.expectedRecovered, fetcher.recovered, "recovered")
				assert.Equal(t, tc.expectedFromTable, fetcher.fromTable, "from table")
				assert.Equal(t, tc.expectedUnrecovered, fetcher.unrecovered, "unrecovered")
				assert.Empty(t, fetcher.read, "every value read is filled into its record, so none is kept past the batch")
				cached += fetcher.cachedValues.bytes
			}
			assert.LessOrEqual(t, cached, maxCachedBytes, "the threads' caches stay within the budget together")
		})
	}
}

// A row too large for one message comes back in parts; each part must be merged into the row.
func TestRecoverUnavailableValuesRowInParts(t *testing.T) {
	testCases := []struct {
		name                string
		opType              string
		expected            map[string]any
		expectedRecovered   int64
		expectedUnrecovered int64
	}{
		{
			name:              "every part of a live row is recovered",
			opType:            "u",
			expected:          map[string]any{"payload": "big value", "doc": "big doc"},
			expectedRecovered: 2,
		},
		{
			name:                "no part of a deleted row is recovered",
			opType:              "d",
			expected:            map[string]any{"payload": placeholder, "doc": placeholder},
			expectedUnrecovered: 2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			table := testTable{"1": {constants.OpType: tc.opType, "payload": "big value", "doc": "big doc"}}
			fetcher := testValueFetcher(t, true, &testRowReader{table: table, inParts: true})
			records := []types.RawRecord{testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder, "doc": placeholder}, "payload", "doc")}

			require.NoError(t, testRecoverAll(fetcher, records))

			for column, expected := range tc.expected {
				assert.Equal(t, expected, records[0].Data[column], column)
			}
			assert.Equal(t, tc.expectedRecovered, fetcher.recovered, "recovered")
			assert.Equal(t, tc.expectedUnrecovered, fetcher.unrecovered, "unrecovered")
		})
	}
}

// Two source columns that share one destination column (Foo and foo) both read it, from each part it comes in.
func TestRecoverUnavailableValuesColumnsSharingDestinationName(t *testing.T) {
	table := testTable{"1": {constants.OpType: "u", "foo": "big value"}}
	fetcher := testValueFetcher(t, true, &testRowReader{table: table, inParts: true})
	fetcher.stream = &types.ConfiguredStream{
		Stream:         &types.Stream{Name: "test", Namespace: "public"},
		StreamMetadata: types.StreamMetadata{Normalization: new(true)},
	}

	records := []types.RawRecord{testRecord("1", "u", map[string]any{"id": 1, "Foo": placeholder, "foo": placeholder}, "Foo", "foo")}
	require.NoError(t, testRecoverAll(fetcher, records))

	assert.Equal(t, "big value", records[0].Data["Foo"])
	assert.Equal(t, "big value", records[0].Data["foo"])
}

// A batch whose stored values pass what the destination sends in one read is recovered a prefix at a time: a call
// fills the records its read answered, and the next call reads on from the first it did not. The row that took the
// read past the limit came whole, so no row is read twice.
func TestRecoverUnavailableValuesReadCutGoesOnFromNextCall(t *testing.T) {
	const limit = 300

	table := testTable{}
	var records []types.RawRecord
	expected := map[string]string{}
	for idx := range 40 {
		olakeID := strconv.Itoa(idx)
		value := strings.Repeat("x", 10+(idx*37)%250)
		table[olakeID] = map[string]any{"payload": value}
		expected[olakeID] = value
		records = append(records, testRecord(olakeID, "u", map[string]any{"id": idx, "payload": placeholder}, "payload"))
	}
	reader := &testRowReader{table: table, maxBytes: limit}
	fetcher := testValueFetcher(t, true, reader)
	// the cached values are trimmed to nothing after every prefix; recovery must not depend on them
	testOverBudget(t)

	for start := 0; start < len(records); {
		n, err := fetcher.RecoverUnavailableValues(context.Background(), records[start:])
		require.NoError(t, err)
		require.Positive(t, n)

		for _, record := range records[start : start+n] {
			assert.Equal(t, expected[record.OlakeColumns[constants.OlakeID].(string)], record.Data["payload"])
		}
		start += n
	}

	assert.Greater(t, len(reader.reads), 1, "the batch is read a window at a time")
	assert.Equal(t, int64(len(records)), fetcher.recovered)
	assert.Equal(t, int64(len(records)), fetcher.fromTable, "no row is read twice")
}

// The read starts at the first record missing a value and answers it, so the records after it
// stay in the same prefix.
func TestRecoverUnavailableValuesReadMidBatchKeepsOnePrefix(t *testing.T) {
	fetcher := testValueFetcher(t, true, &testRowReader{table: testTable{"2": {"payload": "stored value"}}})
	records := []types.RawRecord{
		testRecord("1", "c", map[string]any{"id": 1, "payload": "inserted"}),
		testRecord("2", "u", map[string]any{"id": 2, "payload": placeholder}, "payload"),
		testRecord("3", "c", map[string]any{"id": 3, "payload": "inserted"}),
	}

	n, err := fetcher.RecoverUnavailableValues(context.Background(), records)

	require.NoError(t, err)
	assert.Equal(t, len(records), n)
	assert.Equal(t, "stored value", records[1].Data["payload"])
}

// The records one call fills hold at most MaxDestinationBatchBytes of values, recovered ones included, so the write
// stays under the 2 GB message limit; only a lone first record may pass it. A batch with nothing to fill is written
// as it came, whatever its size.
func TestRecoverUnavailableValuesPrefixStaysWithinBatchBytes(t *testing.T) {
	// a value counted as n MB that holds one: valueSize counts an array by its elements
	mb := strings.Repeat("x", 1024*1024)
	sized := func(n int) []any { return slices.Repeat([]any{mb}, n) }

	testCases := []struct {
		name           string
		table          testTable
		prime          []types.RawRecord // a batch recovered first
		overBudget     bool              // the cache budget holds nothing, so every call drops what it caches
		records        []types.RawRecord
		expectedFilled []int // records filled per call
		expectedAsked  int
	}{
		// the second record would take the prefix past the limit, so it starts the next call
		{
			name:  "a record that takes the prefix past the limit starts the next call",
			table: testTable{"1": {"payload": "v"}},
			records: []types.RawRecord{
				testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder, "doc": sized(600)}, "payload"),
				testRecord("2", "c", map[string]any{"id": 2, "doc": sized(600)}),
			},
			expectedFilled: []int{1, 1},
			expectedAsked:  1,
		},
		// a record past the limit on its own is filled alone, as the writer can only write it alone
		{
			name:  "a record past the limit by itself is filled alone",
			table: testTable{"1": {"payload": "v"}},
			records: []types.RawRecord{
				testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder, "doc": sized(1100)}, "payload"),
				testRecord("2", "c", map[string]any{"id": 2, "payload": "small"}),
			},
			expectedFilled: []int{1, 1},
			expectedAsked:  1,
		},
		// what counts is the record once filled: the placeholder is short, the value read is not. The second row was
		// read with the first and is kept for the next call, which fills it without reading it again
		{
			name:  "the recovered value counts, not the placeholder",
			table: testTable{"1": {"payload": sized(600)}, "2": {"payload": sized(600)}},
			records: []types.RawRecord{
				testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload"),
				testRecord("2", "u", map[string]any{"id": 2, "payload": placeholder}, "payload"),
			},
			expectedFilled: []int{1, 1},
			expectedAsked:  2,
		},
		// nothing is recovered into these records, so they are as large as they came and are written as they are
		{
			name:  "a batch with nothing to fill is not cut",
			table: testTable{"1": {"payload": "v"}},
			prime: []types.RawRecord{testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload")},
			records: []types.RawRecord{
				testRecord("2", "c", map[string]any{"id": 2, "doc": sized(600)}),
				testRecord("3", "c", map[string]any{"id": 3, "doc": sized(600)}),
			},
			expectedFilled: []int{2},
			expectedAsked:  1,
		},
		// the first call reads rows 1, 2 and 3 and stops at the second change of row 1, whose value it had cached;
		// row 3 is kept. The cache is dropped before the next call, which reads row 1 again and, as a read starts
		// with nothing kept, row 3 with it: the values read never hold more than one read
		{
			name:       "a read made while values are kept reads them again",
			table:      testTable{"1": {"payload": sized(400)}, "2": {"payload": sized(400)}, "3": {"payload": sized(400)}},
			overBudget: true,
			records: []types.RawRecord{
				testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload"),
				testRecord("2", "u", map[string]any{"id": 2, "payload": placeholder}, "payload"),
				testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload"),
				testRecord("3", "u", map[string]any{"id": 3, "payload": placeholder}, "payload"),
			},
			expectedFilled: []int{2, 2},
			expectedAsked:  5,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reader := &testRowReader{table: tc.table}
			fetcher := testValueFetcher(t, true, reader)
			if tc.overBudget {
				testOverBudget(t)
			}
			if tc.prime != nil {
				require.NoError(t, testRecoverAll(fetcher, tc.prime))
			}

			var filled []int
			for start := 0; start < len(tc.records); {
				n, err := fetcher.RecoverUnavailableValues(context.Background(), tc.records[start:])
				require.NoError(t, err)
				require.Positive(t, n)
				filled = append(filled, n)
				start += n
			}

			assert.Equal(t, tc.expectedFilled, filled)
			assert.Equal(t, tc.expectedAsked, reader.asked())
			for _, record := range tc.records {
				assert.NotEqual(t, placeholder, record.Data["payload"], "every record is filled")
			}
		})
	}
}

// Without normalization a large row's _op_type comes in a part of its own, before the part holding the
// stored row; that first part must not be taken as the whole row.
func TestRecoverUnavailableValuesRawRowOpTypeInOwnPart(t *testing.T) {
	table := testTable{"1": {constants.OpType: "u", constants.StringifiedData: `{"id":1,"payload":"stored value"}`}}
	reader := &testRowReader{table: table, inParts: true}
	fetcher := testValueFetcher(t, false, reader)
	records := []types.RawRecord{testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload")}

	n, err := fetcher.RecoverUnavailableValues(context.Background(), records)

	require.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Equal(t, json.RawMessage(`"stored value"`), records[0].Data["payload"])
	assert.Equal(t, int64(1), fetcher.recovered)
	assert.Len(t, reader.reads, 1)
}

func TestRecoverUnavailableValuesRowsAsked(t *testing.T) {
	// a function, as RecoverUnavailableValues fills the records it is given
	marked := func() []types.RawRecord {
		return []types.RawRecord{
			testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload"),
			testRecord("2", "u", map[string]any{"id": 2, "payload": placeholder, "doc": placeholder}, "payload", "doc"),
			testRecord("3", "u", map[string]any{"id": 3, "doc": placeholder}, "doc"),
		}
	}

	testCases := []struct {
		name             string
		rawData          bool // normalization off: the whole source row is stored as JSON
		destinationNames bool // the stream renames source columns for the destination
		records          []types.RawRecord
		expectedRows     []RowRead
	}{
		// one call for these rows, each row asking only for its own columns
		{
			name:    "each row reads only the columns it is missing",
			records: marked(),
			expectedRows: []RowRead{
				{OlakeID: "1", Columns: []string{constants.OpType, "payload"}},
				{OlakeID: "2", Columns: []string{constants.OpType, "payload", "doc"}},
				{OlakeID: "3", Columns: []string{constants.OpType, "doc"}},
			},
		},
		// every source column lives in the one JSON column, read once per row
		{
			name:    "without normalization each row reads the stored row once",
			rawData: true,
			records: marked(),
			expectedRows: []RowRead{
				{OlakeID: "1", Columns: []string{constants.OpType, constants.StringifiedData}},
				{OlakeID: "2", Columns: []string{constants.OpType, constants.StringifiedData}},
				{OlakeID: "3", Columns: []string{constants.OpType, constants.StringifiedData}},
			},
		},
		// the destination stores "Big Doc" as big_doc, so the row asks for the destination column
		{
			name:             "a column is asked for by its destination name",
			destinationNames: true,
			records:          []types.RawRecord{testRecord("1", "u", map[string]any{"id": 1, "Big Doc": placeholder}, "Big Doc")},
			expectedRows:     []RowRead{{OlakeID: "1", Columns: []string{constants.OpType, "big_doc"}}},
		},
		// a value found in the batch itself needs no read
		{
			name: "no read when every value comes from the batch",
			records: []types.RawRecord{
				testRecord("1", "i", map[string]any{"id": 1, "payload": "inserted"}),
				testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload"),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reader := &testRowReader{}
			fetcher := testValueFetcher(t, !tc.rawData, reader)
			if tc.destinationNames {
				fetcher.stream = &types.ConfiguredStream{
					Stream:         &types.Stream{Name: "test", Namespace: "public"},
					StreamMetadata: types.StreamMetadata{Normalization: new(true)},
				}
			}

			require.NoError(t, testRecoverAll(fetcher, tc.records))

			if tc.expectedRows == nil {
				assert.Empty(t, reader.reads)
				return
			}
			require.Len(t, reader.reads, 1)
			assert.Equal(t, tc.expectedRows, reader.reads[0])
		})
	}
}

func TestRecoverUnavailableValuesErrors(t *testing.T) {
	failure := errors.New("boom")

	// every failure is wrapped with %w, so the destination error classifier can still see it
	testCases := []struct {
		name    string
		rawData bool
		reader  *testRowReader
		message string
	}{
		{
			name:    "the destination fails",
			reader:  &testRowReader{err: failure},
			message: "boom",
		},
		{
			name:    "a stored row cannot be parsed",
			rawData: true,
			reader:  &testRowReader{table: testTable{"1": {constants.StringifiedData: `{not json`}}},
			message: "failed to read row[1]: failed to parse stored row",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fetcher := testValueFetcher(t, !tc.rawData, tc.reader)
			records := []types.RawRecord{testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload")}

			err := testRecoverAll(fetcher, records)

			require.Error(t, err)
			if tc.reader.err != nil {
				assert.ErrorIs(t, err, failure)
			}
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
			fetcher := testValueFetcher(t, true, &testRowReader{})
			if tc.overBudget {
				testOverBudget(t)
			}

			require.NoError(t, testRecoverAll(fetcher, tc.records))

			assert.Len(t, fetcher.cachedValues.rows, tc.expectedCachedRows)
			if tc.expectedCachedRows == 0 {
				assert.Zero(t, fetcher.cachedValues.bytes)
			}
		})
	}
}

func TestDestinationValuesFetcherCloseReleasesBudget(t *testing.T) {
	testCases := []struct {
		name          string
		records       []types.RawRecord
		expectedTaken int64 // shares of the cache budget the thread holds after the batch
	}{
		{
			name: "a thread that caches takes a share and gives it back",
			records: []types.RawRecord{
				testRecord("1", "i", map[string]any{"id": 1, "payload": "inserted"}),
				testRecord("1", "u", map[string]any{"id": 1, "payload": placeholder}, "payload"),
			},
			expectedTaken: 1,
		},
		// a stream that never marks a column caches nothing, so it has no share to give back
		{
			name:    "a thread that never sees a marker takes no share",
			records: []types.RawRecord{testRecord("1", "u", map[string]any{"id": 1, "payload": "value"})},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			before := cachingThreads.Load()
			fetcher := testValueFetcher(t, true, &testRowReader{})

			require.NoError(t, testRecoverAll(fetcher, tc.records))
			require.Equal(t, before+tc.expectedTaken, cachingThreads.Load(), "shares taken")

			fetcher.Close()
			fetcher.Close() // closing again gives nothing back twice

			assert.Equal(t, before, cachingThreads.Load(), "the thread gives its share back")
			assert.Nil(t, fetcher.cachedValues, "the cached values are freed")
		})
	}
}

// Only a writer that can read rows back from the destination, on a stream that keeps a row index, gets one.
func TestNewDestinationValuesFetcher(t *testing.T) {
	fetching := struct {
		Writer
		*testRowReader
	}{testRowReader: &testRowReader{}}
	index := struct{ types.StreamIndex }{}

	assert.NotNil(t, newDestinationValuesFetcher(testConfiguredStream(true), fetching, index))
	assert.Nil(t, newDestinationValuesFetcher(testConfiguredStream(true), struct{ Writer }{}, index), "no read-back (Parquet)")
	assert.Nil(t, newDestinationValuesFetcher(testConfiguredStream(true), fetching, nil), "no row index (equality or append mode)")
}
