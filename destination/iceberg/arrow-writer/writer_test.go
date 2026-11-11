package arrowwriter

import (
	"context"
	"testing"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractEqualityDeletes(t *testing.T) {
	const dataFile = "data-1.parquet"

	testCases := []struct {
		name              string
		appendMode        bool
		ops               [][2]string // {op type, olake id}
		equalityDeletes   []string
		positionalDeletes []PositionalDelete
	}{
		{
			name: "steady-state inserts write no deletes",
			ops:  [][2]string{{"c", "1"}, {"c", "2"}},
		},
		{
			name:              "repeated insert of same id in same sync deletes the earlier insert by position",
			ops:               [][2]string{{"c", "1"}, {"c", "1"}},
			positionalDeletes: []PositionalDelete{{FilePath: dataFile, Position: 0}},
		},
		{
			name:              "insert then update in same sync deletes the insert by position",
			ops:               [][2]string{{"c", "1"}, {"u", "1"}},
			positionalDeletes: []PositionalDelete{{FilePath: dataFile, Position: 0}},
		},
		{
			name:              "insert then delete in same sync deletes the insert by position",
			ops:               [][2]string{{"c", "1"}, {"d", "1"}},
			positionalDeletes: []PositionalDelete{{FilePath: dataFile, Position: 0}},
		},
		{
			name:            "overlap-window insert writes equality delete",
			ops:             [][2]string{{"i", "1"}},
			equalityDeletes: []string{"1"},
		},
		{
			name:              "overlap-window insert then update keeps equality delete and deletes the insert by position",
			ops:               [][2]string{{"i", "1"}, {"u", "1"}},
			equalityDeletes:   []string{"1"},
			positionalDeletes: []PositionalDelete{{FilePath: dataFile, Position: 0}},
		},
		{
			name:            "update and delete write equality deletes",
			ops:             [][2]string{{"u", "1"}, {"d", "2"}},
			equalityDeletes: []string{"1", "2"},
		},
		{
			name:              "delete then re-insert in same sync keeps only the re-insert",
			ops:               [][2]string{{"d", "1"}, {"c", "1"}},
			equalityDeletes:   []string{"1"},
			positionalDeletes: []PositionalDelete{{FilePath: dataFile, Position: 0}},
		},
		{
			name:            "repeated updates keep only the latest by position",
			ops:             [][2]string{{"u", "1"}, {"u", "1"}, {"u", "1"}},
			equalityDeletes: []string{"1"},
			positionalDeletes: []PositionalDelete{
				{FilePath: dataFile, Position: 0},
				{FilePath: dataFile, Position: 1},
			},
		},
		{
			name: "interleaved changes to inserted ids delete each previous occurrence",
			ops:  [][2]string{{"c", "1"}, {"c", "2"}, {"u", "1"}, {"d", "2"}, {"u", "1"}},
			positionalDeletes: []PositionalDelete{
				{FilePath: dataFile, Position: 0},
				{FilePath: dataFile, Position: 1},
				{FilePath: dataFile, Position: 2},
			},
		},
		{
			name: "snapshot rows are not tracked",
			ops:  [][2]string{{"r", "1"}, {"r", "1"}},
		},
		{
			name:       "append mode writes no deletes",
			appendMode: true,
			ops:        [][2]string{{"u", "1"}, {"u", "1"}, {"d", "2"}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			writer := newTestWriter(&RollingWriter{filePath: dataFile})
			w := &ArrowWriter{upsertMode: !tc.appendMode, writers: map[string]*Writer{"": writer}}

			records := newTestRecords(tc.ops)
			require.NoError(t, w.extract(context.Background(), records))
			assert.ElementsMatch(t, tc.equalityDeletes, writer.equalityDeletes)
			assert.ElementsMatch(t, tc.positionalDeletes, writer.positionalDeletes)

			for _, rec := range records {
				assert.NotEqual(t, "i", rec.OlakeColumns[constants.OpType], "op type i must be normalised to c")
			}
		})
	}
}

// A duplicate arriving after the data file rolled must point its positional delete at the earlier file and row.
func TestExtractDuplicateAcrossDataFiles(t *testing.T) {
	writer := newTestWriter(&RollingWriter{filePath: "data-1.parquet", currentRowCount: 5})
	w := &ArrowWriter{upsertMode: true, writers: map[string]*Writer{"": writer}}

	require.NoError(t, w.extract(context.Background(), newTestRecords([][2]string{{"c", "1"}})))

	// what Write does after a batch, then the data file rolls
	writer.data = writer.data[:0]
	writer.dataWriter = &RollingWriter{filePath: "data-2.parquet"}

	require.NoError(t, w.extract(context.Background(), newTestRecords([][2]string{{"u", "1"}})))
	assert.Empty(t, writer.equalityDeletes)
	assert.Equal(t, []PositionalDelete{{FilePath: "data-1.parquet", Position: 5}}, writer.positionalDeletes)
	assert.Equal(t, PositionalDelete{FilePath: "data-2.parquet", Position: 0}, writer.olakeIDPosition["1"])
}

func newTestWriter(dataWriter *RollingWriter) *Writer {
	return &Writer{
		dataWriter:             dataWriter,
		equalityDeleteWriter:   &RollingWriter{},
		positionalDeleteWriter: &RollingWriter{},
		olakeIDPosition:        make(map[string]PositionalDelete),
	}
}

func newTestRecords(ops [][2]string) []types.RawRecord {
	records := make([]types.RawRecord, 0, len(ops))
	for _, op := range ops {
		records = append(records, types.RawRecord{OlakeColumns: map[string]any{
			constants.OpType:         op[0],
			constants.OlakeID:        op[1],
			constants.OlakeTimestamp: time.Now().UTC(),
		}})
	}
	return records
}
