package driver

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/errs"
)

// CDC is not supported yet
func (d *DB2) ChangeStreamConfig() (bool, bool, bool) { return false, false, false }

func (d *DB2) PreCDC(_ context.Context, _ []types.StreamInterface) error { return nil }

func (d *DB2) StreamChanges(_ context.Context, _ int, _ map[string]any, _ abstract.CDCMsgFn) (any, error) {
	return nil, errs.Precondition(errs.UnsupportedFeature, codeCDCUnsupported,
		fmt.Errorf("CDC is not supported for DB2"))
}

func (d *DB2) PostCDC(_ context.Context, _ int) error { return nil }
