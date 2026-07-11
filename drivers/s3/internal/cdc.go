package driver

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
)

// ChangeStreamConfig returns the change stream configuration for S3
func (s *S3) ChangeStreamConfig() (bool, bool, bool) {
	return false, false, false
}

// CDCSupported returns false as S3 does not support CDC
func (s *S3) CDCSupported() bool {
	return false
}

// PreCDC is not supported for S3
func (s *S3) PreCDC(ctx context.Context, streams []types.StreamInterface) error {
	return fmt.Errorf("CDC is not supported for S3 source")
}

// StreamChanges is not supported for S3
func (s *S3) StreamChanges(ctx context.Context, streamIndex int, metadataStates map[string]any, processFn abstract.CDCMsgFn) (any, error) {
	return nil, fmt.Errorf("CDC is not supported for S3 source")
}

// PostCDC is not supported for S3
func (s *S3) PostCDC(ctx context.Context, streamIndex int) error {
	return fmt.Errorf("CDC is not supported for S3 source")
}
