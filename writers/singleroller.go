package writers

import (
	"context"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/writers/roller"
	"github.com/datazip-inc/olake/writers/sink"
)

// SingleRoller is the ready-made RollingWriter for append-only backends
// (parquet/S3): one roller.Roller writing a single data-file stream into one sink.
// The Roller drives the staging half (Open/Stage); SingleRoller forwards the
// two-phase Commit/Abort to the same sink — so a PartitionedRollingWriter can
// finalize over it. The backend passes the same sink to both fields. Build one per
// partition inside a writers.NewRollingWriter:
//
//	func(p writers.Partition) (writers.RollingWriter, error) {
//	    s, err := s3sink.NewS3Sink(cfg, keyFor(p.Key))
//	    return writers.SingleRoller{
//	        Roller: roller.NewRoller(s, enc, conv, rollCfg),
//	        Sink:   s,
//	    }, nil
//	}
type SingleRoller struct {
	Roller *roller.Roller[*types.RawRecord]
	Sink   sink.StagedSink
}

func (s SingleRoller) Write(ctx context.Context, rows []*types.RawRecord) error {
	return s.Roller.Write(ctx, rows)
}

func (s SingleRoller) Close(ctx context.Context) error { return s.Roller.Close(ctx) }

func (s SingleRoller) Commit(ctx context.Context) error { return s.Sink.Commit(ctx) }

func (s SingleRoller) Abort(ctx context.Context) error { return s.Sink.Abort(ctx) }
