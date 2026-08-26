package driver

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/errs"
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
func (s *S3) PreCDC(_ context.Context, _ []types.StreamInterface) error {
	return errs.Precondition(errs.UnsupportedFeature, codeCDCUnsupported,
		fmt.Errorf("CDC is not supported for S3 source"))
}

// StreamChanges is not supported for S3
func (s *S3) StreamChanges(_ context.Context, _ int, _ map[string]any, _ abstract.CDCMsgFn) (any, error) {
	return nil, errs.Precondition(errs.UnsupportedFeature, codeCDCUnsupported,
		fmt.Errorf("CDC is not supported for S3 source"))
}

// PostCDC is not supported for S3
func (s *S3) PostCDC(_ context.Context, _ int) error {
	return errs.Precondition(errs.UnsupportedFeature, codeCDCUnsupported,
		fmt.Errorf("CDC is not supported for S3 source"))
}
