package abstract

import (
	"context"
	"fmt"
	"sync"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/typeutils"
)

type CDCChange struct {
	Stream    types.StreamInterface
	Timestamp typeutils.Time
	Kind      string
	Data      map[string]interface{}
}

type AbstractDriver struct { //nolint:gosec,revive
	driver          DriverInterface
	state           *types.State
	GlobalConnGroup *utils.CxGroup
	GlobalCtxGroup  *utils.CxGroup
}

const (
	DefaultRetryCount  = 3
	DefaultThreadCount = 3
)

var DefaultColumns = map[string]types.DataType{
	constants.OlakeID:        types.String,
	constants.OlakeTimestamp: types.Int64,
	constants.OpType:         types.String,
	constants.CdcTimestamp:   types.Int64,
}

func NewAbstractDriver(ctx context.Context, driver DriverInterface) *AbstractDriver {
	return &AbstractDriver{
		driver:          driver,
		GlobalCtxGroup:  utils.NewCGroup(ctx),
		GlobalConnGroup: utils.NewCGroupWithLimit(ctx, 10), // default max connections
	}
}

func (a *AbstractDriver) SetupState(state *types.State) {
	a.state = state
}

func (a *AbstractDriver) GetConfigRef() Config {
	return a.driver.GetConfigRef()
}

func (a *AbstractDriver) Spec() any {
	return a.driver.Spec()
}

func (a *AbstractDriver) Check(ctx context.Context) error {
	return a.driver.Setup(ctx)
}

func (a *AbstractDriver) Discover(ctx context.Context) ([]*types.Stream, error) {
	streams, err := a.driver.GetStreamNames(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get stream names: %s", err)
	}
	var streamMap sync.Map
	// set max connections
	if a.driver.MaxConnections() > 0 {
		a.GlobalConnGroup = utils.NewCGroupWithLimit(ctx, a.driver.MaxConnections())
	}
	utils.ConcurrentInGroup(a.GlobalConnGroup, streams, func(ctx context.Context, stream string) error {
		streamSchema, err := a.driver.ProduceSchema(ctx, stream)
		if err != nil {
			return err
		}
		streamMap.Store(streamSchema.ID(), streamSchema)
		return nil
	})
	var result []*types.Stream
	streamMap.Range(func(_, value any) bool {
		result = append(result, value.(*types.Stream))
		return true
	})
	return result, nil
}

func (a *AbstractDriver) Setup(ctx context.Context) error {
	return a.driver.Setup(ctx)
}

// Read handles different sync modes for data retrieval
func (a *AbstractDriver) Read(ctx context.Context, pool *destination.WriterPool, standardStreams, cdcStreams []types.StreamInterface) error {
	// set max read connections
	if a.driver.MaxConnections() > 0 {
		a.GlobalConnGroup = utils.NewCGroupWithLimit(ctx, a.driver.MaxConnections())
	}

	if a.driver.CDCSupported() {
		if err := a.RunChangeStream(ctx, pool, cdcStreams...); err != nil {
			return fmt.Errorf("failed to run change stream: %s", err)
		}
	} else {
		return fmt.Errorf("CDC is not supported, use full refresh for all streams")
	}
	// start backfill for standard streams
	for _, stream := range standardStreams {
		a.GlobalCtxGroup.Add(func(ctx context.Context) error {
			return a.Backfill(ctx, nil, pool, stream)
		})
	}

	return nil
}
