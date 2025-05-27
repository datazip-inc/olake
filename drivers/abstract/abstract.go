package abstract

import (
	"context"
	"fmt"

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

type AbDriver struct {
	driver          DriverInterface
	state           *types.State
	batchSize       int
	GlobalConnGroup *utils.CxGroup
	GlobalCtxGroup  *utils.CxGroup
}

func NewAbstractDriver(ctx context.Context, driver DriverInterface) *AbDriver {
	return &AbDriver{
		driver:         driver,
		GlobalCtxGroup: utils.NewCGroup(ctx),
	}
}

func (a *AbDriver) SetupState(state *types.State) {
	a.state = state
}

func (a *AbDriver) SetBatchSize(batchSize int) {
	a.batchSize = batchSize
}

func (a *AbDriver) GetConfigRef() Config {
	return a.driver.GetConfigRef()
}

func (a *AbDriver) Spec() any {
	return a.driver.Spec()
}

func (a *AbDriver) Type() string {
	return a.driver.Type()
}

func (a *AbDriver) Check(ctx context.Context) error {
	return a.driver.Setup(ctx)
}

func (a *AbDriver) Discover(ctx context.Context) ([]*types.Stream, error) {
	return a.driver.Discover(ctx)
}

func (a *AbDriver) Setup(ctx context.Context) error {
	return a.driver.Setup(ctx)
}

// Read handles different sync modes for data retrieval
func (a *AbDriver) Read(ctx context.Context, pool *destination.WriterPool, standardStreams, cdcStreams []types.StreamInterface) error {
	// set max read connections
	a.GlobalConnGroup = utils.NewCGroupWithLimit(ctx, a.driver.MaxConnections())
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
