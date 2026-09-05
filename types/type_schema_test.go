package types

import (
	"fmt"
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/stretchr/testify/assert"
)

func TestTypeSchemaToIcebergAlwaysIncludesOlakeColumns(t *testing.T) {
	for _, syncNewColumns := range []bool{false, true} {
		t.Run(fmt.Sprintf("sync_new_columns_%t", syncNewColumns), func(t *testing.T) {
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
						Columns:        []string{"id"},
						SyncNewColumns: syncNewColumns,
					},
				},
			}

			fields := stream.Schema.ToIceberg(false, configured)
			fieldNames := make([]string, 0, len(fields))
			for _, field := range fields {
				fieldNames = append(fieldNames, field.Key)
			}

			assert.ElementsMatch(t, []string{
				"id",
				constants.OlakeID,
				constants.OlakeTimestamp,
				constants.OpType,
				constants.CdcTimestamp,
			}, fieldNames)
		})
	}
}
