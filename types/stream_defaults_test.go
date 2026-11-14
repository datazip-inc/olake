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

	both := AvailableWriteModes(Iceberg, nil)
	ApplyDestinationDefaults([]*Stream{source, noDefaults}, available, both)

	assert.Equal(t, available, source.AvailableUpdateTypes)
	assert.Equal(t, both, source.AvailableWriteModes)
	assert.Equal(t, UpdateTypeEquality, source.DefaultStreamProperties.UpdateType)
	assert.False(t, source.DefaultStreamProperties.AppendMode)
	assert.Equal(t, available, noDefaults.AvailableUpdateTypes)

	// a source that defaults to append (Kafka) keeps it when both modes are allowed
	appendDefault := &Stream{Name: "topic", DefaultStreamProperties: &DefaultStreamProperties{AppendMode: true}}
	ApplyDestinationDefaults([]*Stream{appendDefault}, available, both)
	assert.True(t, appendDefault.DefaultStreamProperties.AppendMode)

	appendOnly := &Stream{Name: "users", DefaultStreamProperties: &DefaultStreamProperties{UpdateType: UpdateTypeEquality}}
	ApplyDestinationDefaults([]*Stream{appendOnly}, []UpdateType{}, AvailableWriteModes(Parquet, nil))

	assert.True(t, appendOnly.DefaultStreamProperties.AppendMode)
	assert.Equal(t, UpdateType(""), appendOnly.DefaultStreamProperties.UpdateType)

	// nothing in common (upsert-only source, parquet): append, as parquet always got; sync rejects it anyway
	none := &Stream{Name: "users", DefaultStreamProperties: &DefaultStreamProperties{}}
	ApplyDestinationDefaults([]*Stream{none}, []UpdateType{}, []WriteMode{})
	assert.True(t, none.DefaultStreamProperties.AppendMode)
}

func TestAvailableWriteModes(t *testing.T) {
	upsertOnly := []WriteMode{WriteModeUpsert}

	assert.Equal(t, []WriteMode{WriteModeAppend, WriteModeUpsert}, AvailableWriteModes(Iceberg, nil))
	assert.Equal(t, []WriteMode{WriteModeAppend, WriteModeUpsert}, AvailableWriteModes("", nil))
	assert.Equal(t, []WriteMode{WriteModeAppend}, AvailableWriteModes(Parquet, nil))
	assert.Equal(t, upsertOnly, AvailableWriteModes(Iceberg, upsertOnly))
	// a source that only upserts and a destination that only appends leave nothing
	assert.Empty(t, AvailableWriteModes(Parquet, upsertOnly))
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
