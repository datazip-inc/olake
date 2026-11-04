package abstract

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// markerStubDriver extends stubDriver with the failures the retry marker tests need.
type markerStubDriver struct {
	stubDriver
	streamNames      []types.StreamID
	produceSchemaErr error
	splitChunksErr   error
	produceCalls     *int
}

func (s markerStubDriver) MaxRetries() int { return 0 } // no retries, so no test waits on backoff
func (s markerStubDriver) GetStreamNames(context.Context) ([]types.StreamID, error) {
	return s.streamNames, nil
}
func (s markerStubDriver) ProduceSchema(context.Context, types.StreamID) (*types.Stream, error) {
	*s.produceCalls++
	return nil, s.produceSchemaErr
}
func (s markerStubDriver) GetOrSplitChunks(context.Context, *destination.WriterPool, types.StreamInterface) (*types.Set[types.Chunk], error) {
	return nil, s.splitChunksErr
}

// TestDiscoverProduceSchemaMarker: a ProduceSchema failure is marked ErrRestartRequired (no
// in-process retry, exit 1), unless the driver already marked it ErrManualIntervention.
func TestDiscoverProduceSchemaMarker(t *testing.T) {
	transient := networkReset()
	driverMarked := fmt.Errorf("%w: table was dropped", constants.ErrManualIntervention)

	testCases := []struct {
		name                       string
		produceSchemaErr           error
		expectedRestartRequired    bool
		expectedManualIntervention bool
	}{
		{name: "transient failure is ErrRestartRequired", produceSchemaErr: transient, expectedRestartRequired: true},
		{name: "plain failure is ErrRestartRequired", produceSchemaErr: errors.New("unknown failure"), expectedRestartRequired: true},
		{name: "driver's ErrManualIntervention is kept", produceSchemaErr: driverMarked, expectedRestartRequired: true, expectedManualIntervention: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			driver := NewAbstractDriver(context.Background(), markerStubDriver{
				stubDriver:       stubDriver{typ: "postgres"},
				streamNames:      []types.StreamID{{Namespace: "public", Name: "users"}},
				produceSchemaErr: tc.produceSchemaErr,
				produceCalls:     &calls,
			})

			streams, err := driver.Discover(context.Background(), 0, false)

			require.Error(t, err)
			assert.Nil(t, streams)
			assert.Equal(t, 1, calls, "ProduceSchema runs once per attempt")
			assert.ErrorIs(t, err, tc.produceSchemaErr, "the cause stays in the chain")
			assert.Contains(t, err.Error(), "failed to produce schema for stream public.users")
			assert.Equal(t, tc.expectedRestartRequired, utils.IsRestartRequired(err), "IsRestartRequired")
			assert.Equal(t, tc.expectedManualIntervention, utils.IsManualIntervention(err), "IsManualIntervention")
		})
	}
}

// TestRunChangeStreamBackfillMarker: a failure while planning backfill chunks in a CDC sync is
// marked ErrRestartRequired (exit 1, restart), unless the driver already marked it ErrManualIntervention.
func TestRunChangeStreamBackfillMarker(t *testing.T) {
	transient := networkReset()
	driverMarked := fmt.Errorf("%w: permission denied", constants.ErrManualIntervention)

	testCases := []struct {
		name                       string
		splitChunksErr             error
		expectedManualIntervention bool
	}{
		{name: "transient chunk planning failure is ErrRestartRequired", splitChunksErr: transient},
		{name: "driver's ErrManualIntervention is kept", splitChunksErr: driverMarked, expectedManualIntervention: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stream := types.NewStream("users", "public", nil)
			stream.SyncMode = types.CDC
			configured := stream.Wrap(0)

			// fresh global state: backfill has not completed, so chunk planning runs
			state := &types.State{RWMutex: &sync.RWMutex{}, Type: types.GlobalType}

			calls := 0
			driver := NewAbstractDriver(context.Background(), markerStubDriver{
				stubDriver:     stubDriver{typ: "postgres", cdcSupported: true},
				splitChunksErr: tc.splitChunksErr,
				produceCalls:   &calls,
			})
			driver.SetupState(state)

			err := driver.RunChangeStream(context.Background(), nil, configured)

			require.Error(t, err)
			assert.ErrorIs(t, err, tc.splitChunksErr, "the cause stays in the chain")
			assert.Contains(t, err.Error(), "failed to run backfill")
			assert.True(t, utils.IsRestartRequired(err), "stops in-process retries")
			assert.Equal(t, tc.expectedManualIntervention, utils.IsManualIntervention(err), "IsManualIntervention")
		})
	}
}
