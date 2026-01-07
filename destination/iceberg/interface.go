package iceberg

import (
	"context"

	"github.com/datazip-inc/olake/types"
)

type Writer interface {
	Write(ctx context.Context, records []types.RawRecord) error
	EvolveSchema(ctx context.Context, newSchema map[string]string) error
	Close(ctx context.Context) error
	// GetComputedColumnsSchema returns schema of computed columns (for schema evolution)
	GetComputedColumnsSchema() map[string]string
}
