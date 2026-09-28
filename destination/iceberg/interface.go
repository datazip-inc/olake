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
	// Lookup returns where a row's newest version is, including rows written but not yet
	// committed. Returns false in equality mode.
	Lookup(olakeID string) (types.RowLocation, bool, error)
	// EnsureReadable closes any file in paths this writer still has open, without
	// committing it, so its rows can be read (an open file has no footer).
	EnsureReadable(ctx context.Context, paths []string) error
}
