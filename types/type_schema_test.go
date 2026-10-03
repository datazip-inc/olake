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

// CDC metadata columns are injected into the stream schema at sync time (see
// abstract.injectCDCColumns), so a catalog produced by discover never lists them in
// selected_columns. They must still reach the destination, in both normalization modes.
func TestTypeSchemaToParquetKeepsOlakeColumnsOutsideSelection(t *testing.T) {
	const cdcLSN = "_cdc_lsn"

	newStream := func() *Stream {
		stream := NewStream("users", "public", nil)
		stream.UpsertField("id", Int64, false, false)
		stream.UpsertField("email", String, false, false)
		stream.UpsertField(constants.OlakeID, String, false, true)
		stream.UpsertField(constants.OlakeTimestamp, TimestampMicro, false, true)
		stream.UpsertField(constants.OpType, String, false, true)
		// injected at sync time for a cdc stream
		stream.UpsertField(constants.CdcTimestamp, TimestampMicro, true, true)
		stream.UpsertField(cdcLSN, String, true, true)
		return stream
	}

	// mirrors typeutils.Fields.ToTypeSchema, which builds every property with
	// isOlakeColumn=false because the detected field types carry no olake flag
	detectedSchema := func() *TypeSchema {
		schema := NewTypeSchema()
		for column, typ := range map[string]DataType{
			"id":                     Int64,
			"email":                  String,
			constants.OlakeID:        String,
			constants.OlakeTimestamp: TimestampMicro,
			constants.OpType:         String,
			constants.CdcTimestamp:   TimestampMicro,
			cdcLSN:                   String,
		} {
			schema.AddTypes(column, false, typ)
		}
		return schema
	}

	testCases := []struct {
		name           string
		defaultColumns bool
		schemaOf       func(stream *Stream) *TypeSchema
		expected       []string
	}{
		{
			name:           "normalization disabled reads the catalog schema",
			defaultColumns: true,
			schemaOf:       func(stream *Stream) *TypeSchema { return stream.Schema },
			expected: []string{
				constants.OlakeID, constants.OlakeTimestamp, constants.OpType,
				constants.CdcTimestamp, cdcLSN, constants.StringifiedData,
			},
		},
		{
			name:           "normalization enabled reads the detected schema",
			defaultColumns: false,
			schemaOf:       func(*Stream) *TypeSchema { return detectedSchema() },
			expected: []string{
				"id",
				constants.OlakeID, constants.OlakeTimestamp, constants.OpType,
				constants.CdcTimestamp, cdcLSN,
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			stream := newStream()
			configured := &ConfiguredStream{
				Stream: stream,
				StreamMetadata: StreamMetadata{
					SelectedColumns: &SelectedColumns{
						// "email" is deselected and no cdc column is listed, exactly as
						// discover now writes it
						Columns:        []string{"id", constants.OlakeID, constants.OlakeTimestamp, constants.OpType},
						SyncNewColumns: true,
					},
				},
			}

			schema := testCase.schemaOf(stream).ToParquet(testCase.defaultColumns, configured)
			fieldNames := make([]string, 0, len(schema.Fields()))
			for _, field := range schema.Fields() {
				fieldNames = append(fieldNames, field.Name())
			}
			assert.ElementsMatch(t, testCase.expected, fieldNames)
		})
	}
}
