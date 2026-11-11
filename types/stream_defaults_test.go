package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApplyDestinationDefaults(t *testing.T) {
	available := []UpdateType{UpdateTypeEquality, UpdateTypePosition}
	source := &Stream{Name: "users", DefaultStreamProperties: &DefaultStreamProperties{UpdateType: UpdateTypeEquality}}
	noDefaults := &Stream{Name: "legacy"}

	ApplyDestinationDefaults([]*Stream{source, noDefaults}, available, false)

	assert.Equal(t, available, source.AvailableUpdateTypes)
	assert.Equal(t, UpdateTypeEquality, source.DefaultStreamProperties.UpdateType)
	assert.False(t, source.DefaultStreamProperties.AppendMode)
	assert.Equal(t, available, noDefaults.AvailableUpdateTypes)

	appendOnly := &Stream{Name: "users", DefaultStreamProperties: &DefaultStreamProperties{UpdateType: UpdateTypeEquality}}
	ApplyDestinationDefaults([]*Stream{appendOnly}, []UpdateType{}, true)

	assert.True(t, appendOnly.DefaultStreamProperties.AppendMode)
	assert.Equal(t, UpdateType(""), appendOnly.DefaultStreamProperties.UpdateType)
}

func TestAvailableUpdateTypesWithSourceRestriction(t *testing.T) {
	positionalOnly := []UpdateType{UpdateTypePosition}

	// Kafka feeds positional only, so equality is never offered and positional becomes the default.
	available, err := AvailableUpdateTypes(nil, nil, positionalOnly)
	require.NoError(t, err)
	assert.Equal(t, positionalOnly, available)

	// Snowflake reads positional, so the restriction leaves it.
	available, err = AvailableUpdateTypes([]QueryEngine{QueryEngineSnowflake}, nil, positionalOnly)
	require.NoError(t, err)
	assert.Equal(t, positionalOnly, available)

	// Athena cannot read deletion vectors, so a source feeding only those leaves nothing: upserts need append mode.
	available, err = AvailableUpdateTypes([]QueryEngine{QueryEngineAthena}, nil, []UpdateType{UpdateTypeDeletionVector})
	require.NoError(t, err)
	assert.Empty(t, available)
}
