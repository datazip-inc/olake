package driver

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
)

// PreCDC initializes CDC state. CDC is not supported for Elasticsearch.
func (e *Elasticsearch) PreCDC(ctx context.Context, streams []types.StreamInterface) error {
	return fmt.Errorf("CDC is not supported for Elasticsearch")
}

// StreamChanges streams CDC changes. CDC is not supported for Elasticsearch.
func (e *Elasticsearch) StreamChanges(ctx context.Context, stream types.StreamInterface, processFn abstract.CDCMsgFn) error {
	return fmt.Errorf("CDC is not supported for Elasticsearch")
}

// PostCDC finalizes CDC state. CDC is not supported for Elasticsearch.
func (e *Elasticsearch) PostCDC(ctx context.Context, stream types.StreamInterface, success bool, readerID string) error {
	return fmt.Errorf("CDC is not supported for Elasticsearch")
}
