package iceberg

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datazip-inc/olake/types"
)

func TestIsValidTypeForColumn(t *testing.T) {
	testCases := []struct {
		name       string
		columnType string
		valueType  string
		valid      bool
	}{
		{
			name:       "binary column accepts bytes",
			columnType: "binary",
			valueType:  "binary",
			valid:      true,
		},
		{
			name:       "fixed column accepts fixed bytes of the same width",
			columnType: "fixed[16]",
			valueType:  "fixed[16]",
			valid:      true,
		},
		{
			name:       "fixed column accepts width-less bytes, since a value cannot reveal a width",
			columnType: "fixed[16]",
			valueType:  "binary",
			valid:      true,
		},
		{
			name:       "binary column accepts fixed bytes of any width",
			columnType: "binary",
			valueType:  "fixed[16]",
			valid:      true,
		},
		{
			name:       "fixed column rejects a different width",
			columnType: "fixed[16]",
			valueType:  "fixed[32]",
			valid:      false,
		},
		{
			name:       "string column rejects bytes",
			columnType: "string",
			valueType:  "binary",
			valid:      false,
		},
		{
			name:       "binary column accepts text as its bytes",
			columnType: "binary",
			valueType:  "string",
			valid:      true,
		},
		{
			name:       "fixed column rejects text",
			columnType: "fixed[16]",
			valueType:  "string",
			valid:      false,
		},
		{
			name:       "iceberg promotes int to long",
			columnType: "int",
			valueType:  "long",
			valid:      true,
		},
		{
			name:       "an int fits a long column as it is",
			columnType: "long",
			valueType:  "int",
			valid:      true,
		},
		{
			name:       "float column accepts double",
			columnType: "float",
			valueType:  "double",
			valid:      true,
		},
		{
			name:       "double column accepts float",
			columnType: "double",
			valueType:  "float",
			valid:      true,
		},
		{
			name:       "anything fits a string column",
			columnType: "string",
			valueType:  "int",
			valid:      true,
		},
		{
			name:       "int column rejects string",
			columnType: "int",
			valueType:  "string",
			valid:      false,
		},
		{
			name:       "boolean column rejects int",
			columnType: "boolean",
			valueType:  "int",
			valid:      false,
		},
		{
			name:       "timestamptz column accepts timestamptz",
			columnType: "timestamptz",
			valueType:  "timestamptz",
			valid:      true,
		},
		{
			name:       "timestamptz column rejects string",
			columnType: "timestamptz",
			valueType:  "string",
			valid:      false,
		},
		{
			name:       "a column type olake does not map keeps its own value type and rejects string",
			columnType: "date",
			valueType:  "string",
			valid:      false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.valid, isValidTypeForColumn(tc.columnType, tc.valueType))
		})
	}
}

// chunkWriter keeps the ids of the records each Write call received.
type chunkWriter struct {
	Writer
	calls [][]int
}

func (w *chunkWriter) Write(_ context.Context, records []types.RawRecord) error {
	ids := []int{}
	for _, record := range records {
		ids = append(ids, record.Data["id"].(int))
	}
	w.calls = append(w.calls, ids)
	return nil
}

// A batch grown past one message by recovered values must reach the writer in order, in
// chunks under MaxDestinationBatchBytes, with a record larger than that sent on its own. A
// batch nothing was recovered into goes in one call, as before.
func TestWriteChunksBySize(t *testing.T) {
	// counted as mb megabytes while holding one: valueSize sums every element of an array
	mb := strings.Repeat("x", 1024*1024)
	record := func(id, megabytes int) types.RawRecord {
		value := make([]any, megabytes)
		for idx := range value {
			value[idx] = mb
		}
		return types.RawRecord{Data: map[string]any{"id": id, "payload": value}}
	}

	overLimit := []types.RawRecord{record(0, 400), record(1, 400), record(2, 400), record(3, 400), record(4, 400)}

	testCases := []struct {
		name     string
		resolver *unavailableValuesResolver // nil: the stream recovers nothing
		records  []types.RawRecord
		expected [][]int
	}{
		{
			name:     "an empty batch is one call",
			resolver: &unavailableValuesResolver{filledBytes: 1},
			records:  []types.RawRecord{},
			expected: [][]int{{}},
		},
		{
			name:     "a batch under the limit is one call",
			resolver: &unavailableValuesResolver{filledBytes: 1},
			records:  []types.RawRecord{record(0, 400), record(1, 400)},
			expected: [][]int{{0, 1}},
		},
		{
			name:     "a batch over the limit is split in order",
			resolver: &unavailableValuesResolver{filledBytes: 1},
			records:  overLimit,
			expected: [][]int{{0, 1}, {2, 3}, {4}},
		},
		{
			name:     "a record over the limit goes alone",
			resolver: &unavailableValuesResolver{filledBytes: 1},
			records:  []types.RawRecord{record(0, 1), record(1, 1500), record(2, 1)},
			expected: [][]int{{0}, {1}, {2}},
		},
		{
			name:     "a batch nothing was recovered into is one call",
			resolver: &unavailableValuesResolver{},
			records:  overLimit,
			expected: [][]int{{0, 1, 2, 3, 4}},
		},
		{
			name:     "a stream without a resolver is one call",
			records:  overLimit,
			expected: [][]int{{0, 1, 2, 3, 4}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			writer := &chunkWriter{}
			require.NoError(t, (&Iceberg{writer: writer, unavailableValues: tc.resolver}).Write(context.Background(), tc.records))
			assert.Equal(t, tc.expected, writer.calls)
		})
	}
}
