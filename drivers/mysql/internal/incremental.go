package driver

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
)

// PreIncremental is not supported for MySQL
func (m *MySQL) PreIncremental(ctx context.Context, streams ...types.StreamInterface) error {
	return fmt.Errorf("incremental sync is not supported for MySQL driver")
}

// PostIncremental is not supported for MySQL
func (m *MySQL) PostIncremental(ctx context.Context, stream types.StreamInterface, success bool) error {
	return fmt.Errorf("incremental sync is not supported for MySQL driver")
}

// IncrementalChanges is not supported for MySQL
func (m *MySQL) IncrementalChanges(ctx context.Context, stream types.StreamInterface, cb abstract.BackfillMsgFn) error {
	return fmt.Errorf("incremental sync is not supported for MySQL driver")
}
