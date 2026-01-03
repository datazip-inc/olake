package abstract

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tests for core AbstractDriver functionality

func TestNewAbstractDriver(t *testing.T) {
	ctx := context.Background()
	mockDriver := &MockDriver{}

	abstractDriver := NewAbstractDriver(ctx, mockDriver)

	require.NotNil(t, abstractDriver)
	assert.NotNil(t, abstractDriver.driver)
	assert.NotNil(t, abstractDriver.GlobalCtxGroup)
	assert.NotNil(t, abstractDriver.GlobalConnGroup)
}

func TestSetupState(t *testing.T) {
	ctx := context.Background()
	mockDriver := &MockDriver{
		setupStateFunc: func(state *types.State) {
			// Verify state is passed to driver
		},
	}

	abstractDriver := NewAbstractDriver(ctx, mockDriver)
	state := &types.State{
		RWMutex: &sync.RWMutex{},
		Type:    types.StreamType,
	}

	abstractDriver.SetupState(state)

	assert.Equal(t, state, abstractDriver.state)
}

func TestGetConfigRef(t *testing.T) {
	ctx := context.Background()
	expectedConfig := &MockConfig{}
	mockDriver := &MockDriver{
		getConfigRefFunc: func() Config {
			return expectedConfig
		},
	}

	abstractDriver := NewAbstractDriver(ctx, mockDriver)
	config := abstractDriver.GetConfigRef()

	assert.Equal(t, expectedConfig, config)
}

func TestSpec(t *testing.T) {
	ctx := context.Background()
	expectedSpec := map[string]string{"version": "1.0"}
	mockDriver := &MockDriver{
		specFunc: func() any {
			return expectedSpec
		},
	}

	abstractDriver := NewAbstractDriver(ctx, mockDriver)
	spec := abstractDriver.Spec()

	assert.Equal(t, expectedSpec, spec)
}

func TestType(t *testing.T) {
	ctx := context.Background()
	mockDriver := &MockDriver{
		typeFunc: func() string {
			return "postgres"
		},
	}

	abstractDriver := NewAbstractDriver(ctx, mockDriver)
	driverType := abstractDriver.Type()

	assert.Equal(t, "postgres", driverType)
}

func TestGetKafkaInterface_NotKafka(t *testing.T) {
	ctx := context.Background()
	mockDriver := &MockDriver{}

	abstractDriver := NewAbstractDriver(ctx, mockDriver)
	kafkaInterface, ok := abstractDriver.GetKafkaInterface()

	assert.False(t, ok)
	assert.Nil(t, kafkaInterface)
}

func TestGetKafkaInterface_IsKafka(t *testing.T) {
	ctx := context.Background()
	mockKafkaDriver := &MockKafkaDriver{}

	abstractDriver := NewAbstractDriver(ctx, mockKafkaDriver)
	kafkaInterface, ok := abstractDriver.GetKafkaInterface()

	assert.True(t, ok)
	assert.NotNil(t, kafkaInterface)
}

func TestSetup_Success(t *testing.T) {
	ctx := context.Background()
	setupCalled := false
	mockDriver := &MockDriver{
		setupFunc: func(ctx context.Context) error {
			setupCalled = true
			return nil
		},
	}

	abstractDriver := NewAbstractDriver(ctx, mockDriver)
	err := abstractDriver.Setup(ctx)

	require.NoError(t, err)
	assert.True(t, setupCalled)
}

func TestSetup_Error(t *testing.T) {
	ctx := context.Background()
	mockDriver := &MockDriver{
		setupFunc: func(ctx context.Context) error {
			return errors.New("setup failed")
		},
	}

	abstractDriver := NewAbstractDriver(ctx, mockDriver)
	err := abstractDriver.Setup(ctx)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "setup failed")
}
