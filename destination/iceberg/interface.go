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
	// EnsureReadable closes any file in paths the Go side still has open, without
	// committing it, so its rows can be read (an open file has no footer). Files written
	// by Java are closed by Java itself when ReadRows asks for them.
	EnsureReadable(ctx context.Context, paths []string) error
}
