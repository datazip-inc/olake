package driver

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
)

// PreCDC initializes CDC state. CDC is not supported for Elasticsearch.
func (e *Elasticsearch) PreCDC(_ context.Context, _ []types.StreamInterface) error {
	return fmt.Errorf("CDC is not supported for Elasticsearch")
}

// StreamChanges streams CDC changes. CDC is not supported for Elasticsearch.
func (e *Elasticsearch) StreamChanges(_ context.Context, _ int, _ abstract.CDCMsgFn) error {
	return fmt.Errorf("CDC is not supported for Elasticsearch")
}

// PostCDC finalizes CDC state. CDC is not supported for Elasticsearch.
func (e *Elasticsearch) PostCDC(_ context.Context, _ int) error {
	return fmt.Errorf("CDC is not supported for Elasticsearch")
}

// ChangeStreamConfig configures change stream settings. Not applicable for Elasticsearch.
func (e *Elasticsearch) ChangeStreamConfig() (bool, bool, bool) {
	return false, false, false
}
