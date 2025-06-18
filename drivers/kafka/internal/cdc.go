package driver

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
)

// PreCDC returns an error, as Kafka does not support CDC operations.
func (k *Kafka) PreCDC(_ context.Context, _ []types.StreamInterface) error {
	return fmt.Errorf("CDC not supported for Kafka")
}

// StreamChanges returns an error, as Kafka does not support CDC streaming.
func (k *Kafka) StreamChanges(_ context.Context, _ types.StreamInterface, _ abstract.CDCMsgFn) error {
	return fmt.Errorf("CDC not supported for Kafka")
}

// PostCDC returns an error, as Kafka does not support CDC post-processing.
func (k *Kafka) PostCDC(_ context.Context, _ types.StreamInterface, _ bool) error {
	return fmt.Errorf("CDC not supported for Kafka")
}
