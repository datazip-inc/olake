package iceberg

import (
	"context"

	"github.com/datazip-inc/olake/types"
)

type IcebergWriter interface {
	Write(ctx context.Context, records []types.RawRecord) error
	Close(ctx context.Context) error
}