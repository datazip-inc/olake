package iceberg

import (
	"context"

	"github.com/datazip-inc/olake/types"
)

type Writer interface {
	Write(ctx context.Context, records []types.RawRecord) error
	EvolveSchema(ctx context.Context, newSchema map[string]string) error
	Close(ctx context.Context, finalMetadataState any) error
	// Abort discards whatever the writer staged outside Iceberg, for the paths
	// that give up before Close can commit.
	Abort()
}
