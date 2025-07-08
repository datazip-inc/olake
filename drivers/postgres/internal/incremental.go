package driver

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
)

// PreIncremental is not supported for PostgreSQL
func (p *Postgres) PreIncremental(ctx context.Context, streams ...types.StreamInterface) error {
	return fmt.Errorf("incremental sync is not supported for PostgreSQL driver")
}

// PostIncremental is not supported for PostgreSQL
func (p *Postgres) PostIncremental(ctx context.Context, stream types.StreamInterface, success bool) error {
	return fmt.Errorf("incremental sync is not supported for PostgreSQL driver")
}

// IncrementalChanges is not supported for PostgreSQL
func (p *Postgres) IncrementalChanges(ctx context.Context, stream types.StreamInterface, cb abstract.BackfillMsgFn) error {
	return fmt.Errorf("incremental sync is not supported for PostgreSQL driver")
}
