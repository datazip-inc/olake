package parquet

import (
	"encoding/json"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/writers/roller"
)

// fileName is the unique name for a rolled parquet file. TimestampedFileName embeds
// a ULID, so files from different rolls / schema generations sharing one partition
// directory never collide.
func fileName() string {
	return utils.TimestampedFileName(constants.ParquetFileExt)
}

// stringifiedConverter is the denormalized-mode Converter: it JSON-encodes each
// record's Data into the "data" column, while CreateArrowRecord reads the system
// columns straight from OlakeColumns — reproducing the legacy recordsMap. Its
// allocator is owned by a single partition (the Roller writes sequentially).
func stringifiedConverter(schema *arrow.Schema) roller.Converter[*types.RawRecord] {
	allocator := memory.NewGoAllocator()
	return func(rows []*types.RawRecord) (arrow.Record, error) {
		shaped := make([]*types.RawRecord, len(rows))
		for i, rec := range rows {
			dataBytes, err := json.Marshal(rec.Data)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal data: %w", err)
			}
			shaped[i] = &types.RawRecord{
				Data:         map[string]any{constants.StringifiedData: string(dataBytes)},
				OlakeColumns: rec.OlakeColumns,
			}
		}
		return roller.CreateArrowRecord(shaped, allocator, schema)
	}
}
