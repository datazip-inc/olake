package iceberg

import (
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/destination/iceberg/internal"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
)

func TestFlattenAndCleanData_KeyOnlyDelete(t *testing.T) {
	for _, tc := range []struct {
		name, partition    string
		full, raw, wantErr bool
	}{
		{name: "missing old region", partition: "region", wantErr: true},
		{name: "missing old region in raw records", partition: "region", raw: true, wantErr: true},
		{name: "partition column excluded from the record", partition: "excluded", wantErr: true},
		{name: "partitioned by logged key", partition: "id"},
		{name: "partitioned by normalized key name", partition: "order_id"},
		{name: "full old row with null region", partition: "region", full: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stream := types.NewStream("orders", "public", nil).WithPrimaryKey("id", "Order ID").Wrap(0)
			stream.StreamMetadata.Normalization = new(!tc.raw)
			stream.StreamMetadata.AppendMode = new(false)
			stream.StreamMetadata.SelectedColumns = &types.SelectedColumns{Columns: []string{"id", "Order ID", "region"}}
			writer := Iceberg{
				stream: stream, options: &destination.Options{}, schema: map[string]string{},
				partitionInfo: []internal.PartitionInfo{{Field: tc.partition, SchemaField: stream.ResolveColumnName(tc.partition), Transform: "identity"}},
			}
			record := types.CreateRawRecord(map[string]any{"id": int64(1), "Order ID": int64(1), "region": nil}, map[string]any{constants.OpType: "d"})
			record.KeyOnlyDelete = !tc.full
			_, _, _, err := writer.FlattenAndCleanData(t.Context(), []types.RawRecord{record})
			if tc.wantErr {
				assert.ErrorIs(t, err, constants.ErrNonRetryable)
				assert.ErrorContains(t, err, `partition column "`+tc.partition+`"`)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

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
