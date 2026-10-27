package typeutils

import (
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
)

// The normalized Parquet writer builds its file schema from Fields (FromSchema -> ToTypeSchema ->
// ToParquet). That round trip drops the OlakeColumn flag, so OLake columns that are not hardcoded
// into the Parquet group must still be recognised through the stream's own schema.
func TestFieldsToParquetKeepsOlakeColumnsWithColumnSelection(t *testing.T) {
	const driverCDCColumn = "_cdc_binlog_file_pos"

	stream := types.NewStream("orders", "shop", nil)
	stream.UpsertField("id", types.Int64, false, false)
	stream.UpsertField("amount", types.Float64, false, false)
	stream.UpsertField(driverCDCColumn, types.Int64, true, true)

	configured := &types.ConfiguredStream{
		Stream: stream,
		StreamMetadata: types.StreamMetadata{
			SelectedColumns: &types.SelectedColumns{Columns: []string{"id"}},
		},
	}

	fields := make(Fields)
	fields.FromSchema(configured.Schema(), configured.ResolveColumnName)

	schema := fields.ToTypeSchema().ToParquet(false, configured)
	names := make([]string, 0, len(schema.Fields()))
	for _, field := range schema.Fields() {
		names = append(names, field.Name())
	}

	expected := []string{"id", driverCDCColumn, constants.OlakeID, constants.OlakeTimestamp, constants.OpType, constants.CdcTimestamp}
	assert.ElementsMatch(t, expected, names)
}
