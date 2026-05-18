// Package destination is the facade drivers and protocol code import. It
// hides whether the active write path is legacy (row-based) or arrow-native,
// so a driver can be written once and routed at runtime.
//
// Layering:
//
//	drivers, protocol         ──►  destination          (this package)
//	destination               ──►  destination/core     (Pool, Thread, Stats, Options, With*)
//	                          ──►  destination/legacy   (Writer contract + LegacyPool)
//	                          ──►  destination/arrow    (DestinationAdapter contract + Pool + library)
//	destination/legacy        ──►  destination/core
//	destination/arrow         ──►  destination/core
//
// No upward edges, no cycles.
package destination

import (
	"context"
	"fmt"

	arrowdst "github.com/datazip-inc/olake/destination/arrow"
	dstcore "github.com/datazip-inc/olake/destination/core"
	legacydst "github.com/datazip-inc/olake/destination/legacy"
	"github.com/datazip-inc/olake/types"
)

// ---------------------------------------------------------------------------
// Re-exports — drivers and protocol code only reference these.
// Switching package depths below has zero impact on external callers.
// ---------------------------------------------------------------------------

type (
	// Pool is the orchestration interface a driver consumes.
	Pool = dstcore.Pool
	// Thread is the per-stream writer handle a driver consumes.
	Thread = dstcore.Thread
	// Stats tracks records across all threads of a Pool.
	Stats = dstcore.Stats
	// Options is the per-thread option bag passed through NewWriter.
	Options = dstcore.Options
	// ThreadOptions is a functional setter for Options.
	ThreadOptions = dstcore.ThreadOptions
)

// Functional thread-option helpers, re-exported so callers do not need to
// import destination/core directly.
var (
	WithIdentifier  = dstcore.WithIdentifier
	WithNumber      = dstcore.WithNumber
	WithBackfill    = dstcore.WithBackfill
	WithThreadID    = dstcore.WithThreadID
	WithApplyFilter = dstcore.WithApplyFilter
)

// ---------------------------------------------------------------------------
// Entry point used by protocol/sync.go.
// ---------------------------------------------------------------------------

// NewPool picks the right implementation:
//   - arrowOverride == nil  -> read arrow_writes from destination config
//   - arrowOverride != nil  -> CLI value wins
func NewPool(ctx context.Context, cfg *types.WriterConfig, syncStreams []string, batchSize int64, arrowOverride *bool) (Pool, error) {
	if resolveArrowMode(cfg, arrowOverride) {
		return arrowdst.NewPool(ctx, cfg, syncStreams, batchSize)
	}
	return legacydst.NewPool(ctx, cfg, syncStreams, batchSize)
}

func resolveArrowMode(cfg *types.WriterConfig, arrowOverride *bool) bool {
	if arrowOverride != nil {
		return *arrowOverride
	}
	if raw, ok := cfg.WriterConfig.(map[string]any); ok {
		if v, ok := raw["arrow_writes"].(bool); ok {
			return v
		}
	}
	return false
}

// ClearDestination delegates to the legacy registry. The arrow path piggybacks
// on legacy adapters' DropStreams implementation for now.
func ClearDestination(ctx context.Context, cfg *types.WriterConfig, dropStreams []types.StreamInterface) error {
	if cfg == nil {
		return fmt.Errorf("nil writer config")
	}
	return legacydst.ClearDestination(ctx, cfg, dropStreams)
}
