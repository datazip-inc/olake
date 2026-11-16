package driver

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/datazip-inc/olake/types"
)

// SupportedUpdateTypes runs before Setup, so it reads the raw update_method, never cdcConfig.
func TestSupportedUpdateTypes(t *testing.T) {
	indexed := []types.UpdateType{types.UpdateTypePosition, types.UpdateTypeDeletionVector}

	tests := []struct {
		name         string
		updateMethod any
		cdcConfig    CDC
		want         []types.UpdateType
	}{
		{name: "no update method has no restriction"},
		{name: "standard replication has no restriction", updateMethod: map[string]any{"type": "Standard"}},
		{name: "cdc without preserve_toast_values has no restriction", updateMethod: map[string]any{"type": "CDC", "replication_slot": "olake_slot"}},
		{
			name:         "preserve_toast_values off has no restriction",
			updateMethod: map[string]any{"type": "CDC", "replication_slot": "olake_slot", "preserve_toast_values": false},
		},
		{
			name:         "preserve_toast_values offers only the indexed formats",
			updateMethod: map[string]any{"type": "CDC", "replication_slot": "olake_slot", "preserve_toast_values": true},
			want:         indexed,
		},
		{
			name:         "preserve_toast_values alone decides, without a replication slot",
			updateMethod: map[string]any{"preserve_toast_values": true},
			want:         indexed,
		},
		{
			name:         "preserve_toast_values of the wrong type is left to Setup",
			updateMethod: map[string]any{"type": "CDC", "replication_slot": "olake_slot", "preserve_toast_values": "true"},
		},
		{name: "update method that is not an object is left to Setup", updateMethod: "CDC"},
		{
			name:      "parsed cdc config is not read before Setup",
			cdcConfig: CDC{ReplicationSlot: "olake_slot", PreserveToastValues: true},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := &Postgres{config: &Config{UpdateMethod: tc.updateMethod}, cdcConfig: tc.cdcConfig}
			assert.Equal(t, tc.want, p.SupportedUpdateTypes())
		})
	}
}

// SupportedWriteModes runs after Setup, so it reads the cdcConfig Setup parsed, never the raw update_method.
func TestSupportedWriteModes(t *testing.T) {
	tests := []struct {
		name         string
		updateMethod any
		cdcConfig    CDC
		want         []types.WriteMode
	}{
		{name: "standard replication has no restriction"},
		{name: "cdc without preserve_toast_values has no restriction", cdcConfig: CDC{ReplicationSlot: "olake_slot"}},
		{
			name:      "preserve_toast_values offers only upsert",
			cdcConfig: CDC{ReplicationSlot: "olake_slot", PreserveToastValues: true},
			want:      []types.WriteMode{types.WriteModeUpsert},
		},
		{
			name:         "raw update method is not read",
			updateMethod: map[string]any{"type": "CDC", "replication_slot": "olake_slot", "preserve_toast_values": true},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := &Postgres{config: &Config{UpdateMethod: tc.updateMethod}, cdcConfig: tc.cdcConfig}
			assert.Equal(t, tc.want, p.SupportedWriteModes())
		})
	}
}
