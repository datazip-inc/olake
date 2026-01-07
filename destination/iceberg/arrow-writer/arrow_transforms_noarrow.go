//go:build !duckdb_arrow
// +build !duckdb_arrow

package arrowwriter

import (
	"context"
	"errors"

	"github.com/apache/arrow-go/v18/arrow"
)

// ArrowTransformEngine is a stub used when Arrow support is disabled at build time.
type ArrowTransformEngine struct {
	config *TransformConfig
}

// NewArrowTransformEngine returns nil when transforms aren't required or a stub that fails fast.
func NewArrowTransformEngine(config *TransformConfig) *ArrowTransformEngine {
	if config == nil || (len(config.RowFilters) == 0 && len(config.ComputedColumns) == 0) {
		return nil
	}
	return &ArrowTransformEngine{config: config}
}

// TransformRecord returns an error indicating Arrow builds are required.
func (e *ArrowTransformEngine) TransformRecord(_ context.Context, _ arrow.RecordBatch) (arrow.RecordBatch, error) {
	if e == nil {
		return nil, errors.New("arrow transforms are not enabled in this build")
	}
	return nil, errors.New("duckdb_arrow build tag required to run arrow transforms")
}
