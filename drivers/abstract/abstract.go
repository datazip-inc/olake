package abstract

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
)

type CDCChange struct {
	Stream    types.StreamInterface
	Timestamp time.Time
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
	constants.OlakeTimestamp: types.TimestampMicro,
	constants.OpType:         types.String,
	constants.CdcTimestamp:   types.TimestampMicro,
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

func (a *AbstractDriver) Type() string {
	return a.driver.Type()
}

func (a *AbstractDriver) IsKafkaDriver() bool {
	_, ok := a.driver.(KafkaInterface)
	return ok
}

func (a *AbstractDriver) Discover(ctx context.Context) ([]*types.Stream, error) {
	// set max connections
	if a.driver.MaxConnections() > 0 {
		a.GlobalConnGroup = utils.NewCGroupWithLimit(ctx, a.driver.MaxConnections())
	}

	streams, err := a.driver.GetStreamNames(ctx)
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
		if convStream.SupportedSyncModes.Len() == 0 {
			convStream.WithSyncMode(types.FULLREFRESH, types.INCREMENTAL)
		}
		convStream.SyncMode = utils.Ternary(convStream.SyncMode == "", types.FULLREFRESH, convStream.SyncMode).(types.SyncMode)

		// add default columns
		for column, typ := range DefaultColumns {
			convStream.UpsertField(column, typ, true)
		}

		if a.driver.CDCSupported() && !a.IsKafkaDriver() {
			convStream.WithSyncMode(types.CDC, types.STRICTCDC)
			convStream.SyncMode = types.CDC
		} else {
			// remove cdc column as it is not supported
			convStream.Schema.Properties.Delete(constants.CdcTimestamp)
		}
		finalStreams = append(finalStreams, convStream)
		return true
	})

	return finalStreams, nil
}

func (a *AbstractDriver) Setup(ctx context.Context) error {
	return a.driver.Setup(ctx)
}

func (a *AbstractDriver) ClearState(streams []types.StreamInterface) (*types.State, error) {
	if a.state == nil {
		return &types.State{}, nil
	}

	dropStreams := make(map[string]bool)
	for _, stream := range streams {
		dropStreams[stream.ID()] = true
	}

	// if global state exists (in case of relational sources)
	if a.state.Global != nil && a.state.Global.Streams != nil {
		for streamID := range dropStreams {
			a.state.Global.Streams.Remove(streamID)
		}
	}

	if len(a.state.Streams) > 0 {
		for _, streamState := range a.state.Streams {
			if dropStreams[fmt.Sprintf("%s.%s", streamState.Namespace, streamState.Stream)] {
				streamState.HoldsValue.Store(false)
				streamState.State = sync.Map{}
			}
		}
	}
	return a.state, nil
}

func (a *AbstractDriver) Read(ctx context.Context, pool *destination.WriterPool, backfillStreams, cdcStreams, incrementalStreams []types.StreamInterface) error {
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

	// run incremental sync
	if len(incrementalStreams) > 0 {
		if err := a.Incremental(ctx, pool, incrementalStreams...); err != nil {
			return fmt.Errorf("failed to run incremental sync: %s", err)
		}
	}

	// handle standard streams (full refresh)
	for _, stream := range backfillStreams {
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
