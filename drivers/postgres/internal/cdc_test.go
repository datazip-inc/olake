package driver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/errs"
)

func TestValidateCDCStreams(t *testing.T) {
	stream := func(updateType string, appendMode bool) types.StreamInterface {
		return &types.ConfiguredStream{
			Stream:         &types.Stream{Name: "users", Namespace: "public"},
			StreamMetadata: types.StreamMetadata{StreamName: "users", UpdateType: &updateType, AppendMode: &appendMode},
		}
	}

	tests := []struct {
		name        string
		preserve    bool
		destination types.DestinationType
		stream      types.StreamInterface
		wantCode    string
	}{
		{name: "pos keeps the index", preserve: true, destination: types.Iceberg, stream: stream("pos", false)},
		{name: "eq keeps no index", preserve: true, destination: types.Iceberg, stream: stream("eq", false), wantCode: codeUpdateTypeWithoutIndex},
		{name: "append mode keeps no index", preserve: true, destination: types.Iceberg, stream: stream("pos", true), wantCode: codeAppendModeWithToast},
		{name: "parquet keeps no index", preserve: true, destination: types.Parquet, stream: stream("", true), wantCode: codeDestinationWithoutIndex},
		{name: "parquet without cdc streams is not checked", preserve: true, destination: types.Parquet},
		{name: "option off is not checked", destination: types.Parquet, stream: stream("", true)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := &Postgres{cdcConfig: CDC{PreserveToastValues: tc.preserve}}
			var streams []types.StreamInterface
			if tc.stream != nil {
				streams = append(streams, tc.stream)
			}
			err := p.ValidateCDCStreams(streams, tc.destination)
			if tc.wantCode == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Equal(t, tc.wantCode, errs.From(errs.Classify(err)).Code)
		})
	}
}

func TestSupportedUpdateTypes(t *testing.T) {
	tests := []struct {
		name         string
		updateMethod any
		expected     []types.UpdateType
	}{
		{name: "no update method has no restriction"},
		{name: "standard replication has no restriction", updateMethod: map[string]any{"type": "standard"}},
		{name: "cdc without preserve_toast_values has no restriction", updateMethod: map[string]any{"replication_slot": "olake_slot"}},
		{
			name:         "preserve_toast_values offers only the indexed formats",
			updateMethod: map[string]any{"replication_slot": "olake_slot", "preserve_toast_values": true},
			expected:     []types.UpdateType{types.UpdateTypePosition, types.UpdateTypeDeletionVector},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := &Postgres{config: &Config{UpdateMethod: tc.updateMethod}}
			assert.Equal(t, tc.expected, p.SupportedUpdateTypes())
		})
	}
}
