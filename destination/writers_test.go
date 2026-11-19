package destination

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
)

// prefixWriter writes what it is given and logs each call flush makes, with the ids it was given.
// Flattening drops the records whose id is in filter. The embedded interface supplies the calls
// flush never makes.
type prefixWriter struct {
	Writer
	filter   map[int]bool
	writeErr error
	calls    []string
}

func (w *prefixWriter) FlattenAndCleanData(_ context.Context, records []types.RawRecord) (bool, []types.RawRecord, any, error) {
	kept := make([]types.RawRecord, 0, len(records))
	for _, record := range records {
		if !w.filter[record.Data["id"].(int)] {
			kept = append(kept, record)
		}
	}
	return false, kept, nil, nil
}

func (w *prefixWriter) Write(_ context.Context, records []types.RawRecord) error {
	w.calls = append(w.calls, fmt.Sprintf("write %v", ids(records)))
	return w.writeErr
}

// recoveringWriter can read rows back, so the thread recovers the values its records lack. It answers
// the first prefix rows of every read and leaves the rest unanswered, so the thread fills its batch
// prefix records at a time.
type recoveringWriter struct {
	*prefixWriter
	prefix int
	err    error
}

func (w *recoveringWriter) ReadRows(_ context.Context, rows []RowRead, emit func(int, map[string]any) error) error {
	asked := []int{}
	for _, row := range rows {
		id, _ := strconv.Atoi(row.OlakeID)
		asked = append(asked, id)
	}
	w.calls = append(w.calls, fmt.Sprintf("read %v", asked))
	if w.err != nil {
		return w.err
	}
	for row := range min(w.prefix, len(rows)) {
		if err := emit(row, map[string]any{constants.OpType: "u", "payload": "stored"}); err != nil {
			return err
		}
	}
	return nil
}

// closingWriter records how many threads hold a share of the cache budget when the thread closes it.
type closingWriter struct {
	*recoveringWriter
	cachingOnClose int64
}

func (w *closingWriter) Close(_ context.Context, _ any) error {
	w.cachingOnClose = cachingThreads.Load()
	return nil
}

func ids(records []types.RawRecord) []int {
	list := []int{}
	for _, record := range records {
		list = append(list, record.Data["id"].(int))
	}
	return list
}

func TestFlushWritesRecoveredPrefixesInOrder(t *testing.T) {
	testCases := []struct {
		name             string
		prefix           int // rows each read answers; 0 for a writer that cannot read rows back
		recoverErr       error
		writeErr         error
		filter           map[int]bool
		expectedCalls    []string
		expectedFiltered int64
		expectedReleased int // the first records cleared once written
		expectedErr      string
	}{
		{
			name:             "a writer that recovers nothing writes the batch in one call",
			expectedCalls:    []string{"write [0 1 2 3 4]"},
			expectedReleased: 5,
		},
		{
			name:   "a recovering writer writes each prefix before recovering the rest",
			prefix: 2,
			expectedCalls: []string{
				"read [0 1 2 3 4]", "write [0 1]",
				"read [2 3 4]", "write [2 3]",
				"read [4]", "write [4]",
			},
			expectedReleased: 5,
		},
		{
			name:   "records filtered out of every prefix are counted once",
			prefix: 2,
			filter: map[int]bool{1: true, 4: true},
			expectedCalls: []string{
				"read [0 1 2 3 4]", "write [0]",
				"read [2 3 4]", "write [2 3]",
				"read [4]", "write []",
			},
			expectedFiltered: 2,
			expectedReleased: 5,
		},
		{
			name:          "a failed recovery fails the flush before its prefix is written",
			prefix:        2,
			recoverErr:    errors.New("index unavailable"),
			expectedCalls: []string{"read [0 1 2 3 4]"},
			expectedErr:   "failed to recover unavailable values: index unavailable",
		},
		{
			name:          "a failed write fails the flush before the rest is recovered",
			prefix:        2,
			writeErr:      errors.New("upload failed"),
			expectedCalls: []string{"read [0 1 2 3 4]", "write [0 1]"},
			expectedErr:   "failed to write records: upload failed",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			base := &prefixWriter{filter: tc.filter, writeErr: tc.writeErr}
			var writer Writer = base
			if tc.prefix > 0 {
				writer = &recoveringWriter{prefixWriter: base, prefix: tc.prefix, err: tc.recoverErr}
			}
			thread := &WriterThread{writer: writer, stats: &Stats{}, threadID: "test-thread", destinationValuesFetcher: newDestinationValuesFetcher(testConfiguredStream(true), writer, struct{ types.StreamIndex }{})}
			t.Cleanup(thread.closeUnavailableValues)
			// every record lacks a value, so a writer that can read rows back is asked for each
			var buf []types.RawRecord
			for id := range 5 {
				buf = append(buf, types.RawRecord{
					Data:               map[string]any{"id": id, "payload": placeholder},
					OlakeColumns:       map[string]any{constants.OlakeID: strconv.Itoa(id), constants.OpType: "u"},
					UnavailableColumns: []string{"payload"},
				})
			}

			err := thread.flush(context.Background(), buf)

			assert.Equal(t, tc.expectedCalls, base.calls)
			for idx, record := range buf {
				assert.Equal(t, idx < tc.expectedReleased, record.Data == nil, "record %d is released once written", idx)
			}
			if tc.expectedErr != "" {
				require.EqualError(t, err, tc.expectedErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expectedFiltered, thread.stats.RecordsFiltered.Load())
		})
	}
}

// The thread gives its share of the cache budget back before its writer commits, as other streams commit after it.
// Close has three ways to reach the writer: a commit, an abort, and a final flush that fails.
func TestCloseReleasesUnavailableValuesBeforeWriterClose(t *testing.T) {
	testCases := []struct {
		name        string
		aborted     bool  // the close context is already canceled: the thread rolls back instead of committing
		flushErr    error // the read of the final flush fails
		expectedErr string
	}{
		{
			name: "a thread that commits",
		},
		{
			name:    "a thread aborted before its commit",
			aborted: true,
		},
		{
			name:        "a thread whose final flush fails",
			flushErr:    errors.New("index unavailable"),
			expectedErr: "failed to flush data while closing: failed to recover unavailable values: index unavailable",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			before := cachingThreads.Load()
			writer := &closingWriter{recoveringWriter: &recoveringWriter{prefixWriter: &prefixWriter{}, prefix: 1}}
			thread := &WriterThread{writer: writer, stats: &Stats{}, streamArtifact: &writerSchema{}, group: utils.NewCGroupWithLimit(context.Background(), 1), destinationValuesFetcher: newDestinationValuesFetcher(testConfiguredStream(true), writer, struct{ types.StreamIndex }{})}
			marked := func(id int) types.RawRecord {
				return types.RawRecord{
					Data:               map[string]any{"id": id, "payload": placeholder},
					OlakeColumns:       map[string]any{constants.OlakeID: strconv.Itoa(id), constants.OpType: "u"},
					UnavailableColumns: []string{"payload"},
				}
			}
			// a marked record makes the thread cache, so it holds a share of the budget
			require.NoError(t, thread.flush(context.Background(), []types.RawRecord{marked(1)}))
			require.Equal(t, before+1, cachingThreads.Load())
			// the record left for the final flush needs a read
			thread.buffer = []types.RawRecord{marked(2)}
			writer.err = tc.flushErr
			closeCtx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			if tc.aborted {
				cancel()
			}

			err := thread.Close(closeCtx, nil)

			assert.Equal(t, before, writer.cachingOnClose, "the share is given back before the writer closes")
			if tc.expectedErr != "" {
				require.EqualError(t, err, tc.expectedErr)
				return
			}
			require.NoError(t, err)
		})
	}
}
