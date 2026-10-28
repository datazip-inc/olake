package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestApplyDestinationDefaults(t *testing.T) {
	available := []UpdateType{UpdateTypeEquality, UpdateTypePosition}
	source := &Stream{Name: "users", DefaultStreamProperties: &DefaultStreamProperties{UpdateType: UpdateTypeEquality}}
	// Kafka presets positional: its upserts always write positional deletes.
	kafka := &Stream{Name: "topic", DefaultStreamProperties: &DefaultStreamProperties{UpdateType: UpdateTypePosition}}
	noDefaults := &Stream{Name: "legacy"}

	ApplyDestinationDefaults([]*Stream{source, kafka, noDefaults}, available, false)

	assert.Equal(t, available, source.AvailableUpdateTypes)
	assert.Equal(t, UpdateTypeEquality, source.DefaultStreamProperties.UpdateType)
	assert.False(t, source.DefaultStreamProperties.AppendMode)
	assert.Equal(t, UpdateTypePosition, kafka.DefaultStreamProperties.UpdateType)
	assert.Equal(t, available, noDefaults.AvailableUpdateTypes)

	appendOnly := &Stream{Name: "users", DefaultStreamProperties: &DefaultStreamProperties{UpdateType: UpdateTypeEquality}}
	ApplyDestinationDefaults([]*Stream{appendOnly}, []UpdateType{}, true)

	assert.True(t, appendOnly.DefaultStreamProperties.AppendMode)
	assert.Equal(t, UpdateType(""), appendOnly.DefaultStreamProperties.UpdateType)
}
