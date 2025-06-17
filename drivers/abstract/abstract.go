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
		GlobalConnGroup: utils.NewCGroupWithLimit(ctx, constants.DefaultThreadCount), // default max connections
	}
}

func (a *AbstractDriver) SetupState(state *types.State) {
	a.state = state
	a.driver.SetupState(state)
}

func (a *AbstractDriver) GetConfigRef() Config {
	return a.driver.GetConfigRef()
}

func (a *AbstractDriver) Spec() any {
	return a.driver.Spec()
}

func (a *AbstractDriver) Discover(ctx context.Context) ([]*types.Stream, error) {
	discoverCtx, cancel := context.WithTimeout(ctx, constants.DefaultDiscoverTimeout)
	defer cancel()

	// set max connections
	if a.driver.MaxConnections() > 0 {
		a.GlobalConnGroup = utils.NewCGroupWithLimit(discoverCtx, a.driver.MaxConnections())
	}

	streams, err := a.driver.GetStreamNames(discoverCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to get stream names: %s", err)
	}
	var streamMap sync.Map

	utils.ConcurrentInGroup(a.GlobalConnGroup, streams, func(ctx context.Context, stream string) error {
		streamSchema, err := a.driver.ProduceSchema(ctx, stream) // use conn group context which is discoverCtx
		if err != nil {
			return err
		}
		streamMap.Store(streamSchema.ID(), streamSchema)
		return nil
	})

	if err := a.GlobalConnGroup.Block(); err != nil {
		return nil, fmt.Errorf("error occurred while waiting for connection group: %s", err)
	}

	var finalStreams []*types.Stream
	streamMap.Range(func(_, value any) bool {
		convStream, _ := value.(*types.Stream)
		// Add CDC columns if supported
		if a.driver.CDCSupported() {
			for column, typ := range DefaultColumns {
				convStream.UpsertField(column, typ, true)
			}
			convStream.SyncMode = types.CDC
			convStream.WithSyncMode(types.CDC)
		} else {
			convStream.SyncMode = types.FULLREFRESH
		}
		finalStreams = append(finalStreams, convStream)
		return true
	})

	return finalStreams, nil
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

	// run cdc sync
	if len(cdcStreams) > 0 {
		if a.driver.CDCSupported() {
			if err := a.RunChangeStream(ctx, pool, cdcStreams...); err != nil {
				return fmt.Errorf("failed to run change stream: %s", err)
			}
		} else {
			return fmt.Errorf("%s cdc configuration not provided, use full refresh for all streams", a.driver.Type())
		}
	}

	// start backfill for standard streams
	for _, stream := range standardStreams {
		a.GlobalCtxGroup.Add(func(ctx context.Context) error {
			return a.Backfill(ctx, nil, pool, stream)
		})
	}

	// wait for all threads to finish
	if err := a.GlobalCtxGroup.Block(); err != nil {
		return fmt.Errorf("error occurred while waiting for context groups: %s", err)
	}

	// wait for all threads to finish
	if err := a.GlobalConnGroup.Block(); err != nil {
		return fmt.Errorf("error occurred while waiting for connections: %s", err)
	}
	return nil
}
