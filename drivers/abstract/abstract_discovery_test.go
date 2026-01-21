package abstract

import (
	"context"
	"errors"
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tests for stream discovery functionality

func TestDiscover_Success(t *testing.T) {
	ctx := context.Background()
	mockDriver := &MockDriver{
		getStreamNamesFunc: func(ctx context.Context) ([]string, error) {
			return []string{"users", "orders"}, nil
		},
		produceSchemaFunc: func(ctx context.Context, stream string) (*types.Stream, error) {
			return createMockStream(stream, "public", types.FULLREFRESH), nil
		},
	}

	abstractDriver := NewAbstractDriver(ctx, mockDriver)
	streams, err := abstractDriver.Discover(ctx)

	require.NoError(t, err)
	assert.Len(t, streams, 2)

	// Verify default columns are added (except CDC timestamp since CDC is not supported)
	for _, stream := range streams {
		for column, expectedType := range DefaultColumns {
			if column == constants.CdcTimestamp {
				// CDC timestamp should not exist for non-CDC drivers
				_, exists := stream.Schema.Properties.Load(column)
				assert.False(t, exists, "CDC timestamp should not exist for non-CDC driver")
			} else {
				prop, exists := stream.Schema.Properties.Load(column)
				assert.True(t, exists, "Default column %s should exist", column)
				if exists {
					property := prop.(*types.Property)
					assert.True(t, property.Type.Exists(expectedType), "Column %s should have correct type", column)
				}
			}
		}
	}
}

func TestDiscover_EmptyStreams(t *testing.T) {
	ctx := context.Background()
	mockDriver := &MockDriver{
		getStreamNamesFunc: func(ctx context.Context) ([]string, error) {
			return []string{}, nil
		},
	}

	abstractDriver := NewAbstractDriver(ctx, mockDriver)
	streams, err := abstractDriver.Discover(ctx)

	require.NoError(t, err)
	assert.Len(t, streams, 0)
}

func TestDiscover_GetStreamNamesError(t *testing.T) {
	ctx := context.Background()
	mockDriver := &MockDriver{
		getStreamNamesFunc: func(ctx context.Context) ([]string, error) {
			return nil, errors.New("connection failed")
		},
	}

	abstractDriver := NewAbstractDriver(ctx, mockDriver)
	streams, err := abstractDriver.Discover(ctx)

	require.Error(t, err)
	assert.Nil(t, streams)
	assert.Contains(t, err.Error(), "failed to get stream names")
}

func TestDiscover_ProduceSchemaError(t *testing.T) {
	ctx := context.Background()
	mockDriver := &MockDriver{
		getStreamNamesFunc: func(ctx context.Context) ([]string, error) {
			return []string{"users"}, nil
		},
		produceSchemaFunc: func(ctx context.Context, stream string) (*types.Stream, error) {
			return nil, errors.New("schema error")
		},
	}

	abstractDriver := NewAbstractDriver(ctx, mockDriver)
	streams, err := abstractDriver.Discover(ctx)

	require.Error(t, err)
	assert.Nil(t, streams)
}

func TestDiscover_CDCSupported(t *testing.T) {
	ctx := context.Background()
	mockDriver := &MockDriver{
		getStreamNamesFunc: func(ctx context.Context) ([]string, error) {
			return []string{"users"}, nil
		},
		produceSchemaFunc: func(ctx context.Context, stream string) (*types.Stream, error) {
			return createMockStream(stream, "public", types.FULLREFRESH), nil
		},
		cdcSupportedFunc: func() bool {
			return true
		},
	}

	abstractDriver := NewAbstractDriver(ctx, mockDriver)
	streams, err := abstractDriver.Discover(ctx)

	require.NoError(t, err)
	require.Len(t, streams, 1)
	assert.True(t, streams[0].SupportedSyncModes.Exists(types.CDC))
	assert.Equal(t, types.CDC, streams[0].SyncMode)

	// Verify CDC timestamp column exists
	_, exists := streams[0].Schema.Properties.Load(constants.CdcTimestamp)
	assert.True(t, exists)
}

func TestDiscover_KafkaDriver(t *testing.T) {
	ctx := context.Background()
	mockKafkaDriver := &MockKafkaDriver{
		MockDriver: MockDriver{
			getStreamNamesFunc: func(ctx context.Context) ([]string, error) {
				return []string{"topic1"}, nil
			},
			produceSchemaFunc: func(ctx context.Context, stream string) (*types.Stream, error) {
				return createMockStream(stream, "kafka", types.FULLREFRESH), nil
			},
			cdcSupportedFunc: func() bool {
				return true
			},
		},
	}

	abstractDriver := NewAbstractDriver(ctx, mockKafkaDriver)
	streams, err := abstractDriver.Discover(ctx)

	require.NoError(t, err)
	require.Len(t, streams, 1)

	// Kafka driver should not have CDC timestamp even if CDC is supported
	_, exists := streams[0].Schema.Properties.Load(constants.CdcTimestamp)
	assert.False(t, exists)
}

func TestDiscover_MaxConnections(t *testing.T) {
	ctx := context.Background()
	mockDriver := &MockDriver{
		maxConnectionsFunc: func() int {
			return 5
		},
		getStreamNamesFunc: func(ctx context.Context) ([]string, error) {
			return []string{"stream1"}, nil
		},
		produceSchemaFunc: func(ctx context.Context, stream string) (*types.Stream, error) {
			return createMockStream(stream, "public", types.FULLREFRESH), nil
		},
	}

	abstractDriver := NewAbstractDriver(ctx, mockDriver)
	_, err := abstractDriver.Discover(ctx)

	require.NoError(t, err)
	// Verify that GlobalConnGroup was recreated with limit
	assert.NotNil(t, abstractDriver.GlobalConnGroup)
}

func TestDiscover_DefaultColumnsAllDrivers(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name         string
		cdcSupported bool
		isKafka      bool
	}{
		{"Regular driver without CDC", false, false},
		{"Regular driver with CDC", true, false},
		{"Kafka driver", true, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var mockDriver DriverInterface
			if tc.isKafka {
				mockDriver = &MockKafkaDriver{
					MockDriver: MockDriver{
						getStreamNamesFunc: func(ctx context.Context) ([]string, error) {
							return []string{"stream1"}, nil
						},
						produceSchemaFunc: func(ctx context.Context, stream string) (*types.Stream, error) {
							return createMockStream(stream, "public", types.FULLREFRESH), nil
						},
						cdcSupportedFunc: func() bool {
							return tc.cdcSupported
						},
					},
				}
			} else {
				mockDriver = &MockDriver{
					getStreamNamesFunc: func(ctx context.Context) ([]string, error) {
						return []string{"stream1"}, nil
					},
					produceSchemaFunc: func(ctx context.Context, stream string) (*types.Stream, error) {
						return createMockStream(stream, "public", types.FULLREFRESH), nil
					},
					cdcSupportedFunc: func() bool {
						return tc.cdcSupported
					},
				}
			}

			abstractDriver := NewAbstractDriver(ctx, mockDriver)
			streams, err := abstractDriver.Discover(ctx)

			require.NoError(t, err)
			require.Len(t, streams, 1)

			// Check default columns
			for column, expectedType := range DefaultColumns {
				if column == constants.CdcTimestamp && (!tc.cdcSupported || tc.isKafka) {
					_, exists := streams[0].Schema.Properties.Load(column)
					assert.False(t, exists, "CDC timestamp should not exist for non-CDC or Kafka drivers")
				} else {
					prop, exists := streams[0].Schema.Properties.Load(column)
					assert.True(t, exists, "Column %s should exist", column)
					if exists {
						property := prop.(*types.Property)
						assert.True(t, property.Type.Exists(expectedType), "Column %s should have type %v", column, expectedType)
					}
				}
			}
		})
	}
}
