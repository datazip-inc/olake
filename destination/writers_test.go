package destination

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datazip-inc/olake/types"
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

// resolvingWriter hands the batch over prefix records at a time, as a writer that recovers
// values does.
type resolvingWriter struct {
	*prefixWriter
	prefix int
	err    error
}

func (w *resolvingWriter) Resolve(_ context.Context, records []types.RawRecord) (int, error) {
	w.calls = append(w.calls, fmt.Sprintf("resolve %v", ids(records)))
	return min(w.prefix, len(records)), w.err
}

func ids(records []types.RawRecord) []int {
	list := []int{}
	for _, record := range records {
		list = append(list, record.Data["id"].(int))
	}
	return list
}

func TestFlushWritesResolvedPrefixesInOrder(t *testing.T) {
	testCases := []struct {
		name             string
		prefix           int // records each Resolve hands over; 0 for a writer that recovers nothing
		resolveErr       error
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
			name:   "a resolving writer writes each prefix before resolving the rest",
			prefix: 2,
			expectedCalls: []string{
				"resolve [0 1 2 3 4]", "write [0 1]",
				"resolve [2 3 4]", "write [2 3]",
				"resolve [4]", "write [4]",
			},
			expectedReleased: 5,
		},
		{
			name:   "records filtered out of every prefix are counted once",
			prefix: 2,
			filter: map[int]bool{1: true, 4: true},
			expectedCalls: []string{
				"resolve [0 1 2 3 4]", "write [0]",
				"resolve [2 3 4]", "write [2 3]",
				"resolve [4]", "write []",
			},
			expectedFiltered: 2,
			expectedReleased: 5,
		},
		{
			name:          "a failed resolve fails the flush before its prefix is written",
			prefix:        2,
			resolveErr:    errors.New("index unavailable"),
			expectedCalls: []string{"resolve [0 1 2 3 4]"},
			expectedErr:   "failed to resolve unavailable values: index unavailable",
		},
		{
			name:          "a failed write fails the flush before the rest is resolved",
			prefix:        2,
			writeErr:      errors.New("upload failed"),
			expectedCalls: []string{"resolve [0 1 2 3 4]", "write [0 1]"},
			expectedErr:   "failed to write records: upload failed",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			base := &prefixWriter{filter: tc.filter, writeErr: tc.writeErr}
			var writer Writer = base
			if tc.prefix > 0 {
				writer = &resolvingWriter{prefixWriter: base, prefix: tc.prefix, err: tc.resolveErr}
			}
			thread := &WriterThread{writer: writer, stats: &Stats{}, threadID: "test-thread"}
			var buf []types.RawRecord
			for id := range 5 {
				buf = append(buf, types.RawRecord{Data: map[string]any{"id": id}})
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
