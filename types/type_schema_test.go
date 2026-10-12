package types

import (
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/stretchr/testify/assert"
)

func TestTypeSchemaToIcebergColumnSelection(t *testing.T) {
	testCases := []struct {
		name            string
		selectedColumns []string
		syncNewColumns  bool
		defaultColumns  bool
		includeColumns  []string
		expected        []string
	}{
		{
			name:            "nil selection includes all regular columns",
			selectedColumns: nil,
			expected:        []string{"id", "email", constants.OlakeID, constants.OlakeTimestamp, constants.OpType, constants.CdcTimestamp},
		},
		{
			name:            "blank selection includes only default columns",
			selectedColumns: []string{},
			defaultColumns:  true,
			expected:        []string{constants.OlakeID, constants.OlakeTimestamp, constants.OpType, constants.CdcTimestamp, constants.StringifiedData},
		},
		{
			name:            "one selected column",
			selectedColumns: []string{"id"},
			expected:        []string{"id", constants.OlakeID, constants.OlakeTimestamp, constants.OpType, constants.CdcTimestamp},
		},
		{
			name:            "all selected columns",
			selectedColumns: []string{"id", "email"},
			expected:        []string{"id", "email", constants.OlakeID, constants.OlakeTimestamp, constants.OpType, constants.CdcTimestamp},
		},
		{
			name:            "one selected column with sync new columns",
			selectedColumns: []string{"id"},
			syncNewColumns:  true,
			expected:        []string{"id", constants.OlakeID, constants.OlakeTimestamp, constants.OpType, constants.CdcTimestamp},
		},
		{
			name:            "all selected columns with sync new columns",
			selectedColumns: []string{"id", "email"},
			syncNewColumns:  true,
			expected:        []string{"id", "email", constants.OlakeID, constants.OlakeTimestamp, constants.OpType, constants.CdcTimestamp},
		},
		{
			name:            "default columns include selected regular column",
			selectedColumns: []string{"id"},
			defaultColumns:  true,
			includeColumns:  []string{"id"},
			expected:        []string{"id", constants.OlakeID, constants.OlakeTimestamp, constants.OpType, constants.CdcTimestamp, constants.StringifiedData},
		},
		{
			name:            "default columns include selected email column",
			selectedColumns: []string{"email"},
			defaultColumns:  true,
			includeColumns:  []string{"email"},
			expected:        []string{"email", constants.OlakeID, constants.OlakeTimestamp, constants.OpType, constants.CdcTimestamp, constants.StringifiedData},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			stream := NewStream("users", "public", nil)
			stream.UpsertField("id", Int64, false, false)
			stream.UpsertField("email", String, false, false)
			stream.UpsertField(constants.OlakeID, String, false, true)
			stream.UpsertField(constants.OlakeTimestamp, TimestampMicro, false, true)
			stream.UpsertField(constants.OpType, String, false, true)
			stream.UpsertField(constants.CdcTimestamp, TimestampMicro, true, true)
			configured := &ConfiguredStream{
				Stream: stream,
				StreamMetadata: StreamMetadata{
					SelectedColumns: &SelectedColumns{
						Columns:        testCase.selectedColumns,
						SyncNewColumns: testCase.syncNewColumns,
					},
				},
			}

			fields := stream.Schema.ToIceberg(testCase.defaultColumns, configured, testCase.includeColumns...)
			fieldNames := make([]string, 0, len(fields))
			for _, field := range fields {
				fieldNames = append(fieldNames, field.Key)
			}
			assert.ElementsMatch(t, testCase.expected, fieldNames)
		})
	}
}
