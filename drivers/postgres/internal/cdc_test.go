package driver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/errs"
)

func TestValidateToastStreams(t *testing.T) {
	stream := func(updateType string, appendMode bool) types.StreamInterface {
		return &types.ConfiguredStream{
			Stream:         &types.Stream{Name: "users", Namespace: "public"},
			StreamMetadata: types.StreamMetadata{StreamName: "users", UpdateType: &updateType, AppendMode: &appendMode},
		}
	}

	tests := []struct {
		name     string
		stream   types.StreamInterface
		wantCode string
	}{
		{name: "pos keeps the index", stream: stream("pos", false)},
		{name: "eq keeps no index", stream: stream("eq", false), wantCode: codeUpdateTypeWithoutIndex},
		{name: "append mode keeps no index", stream: stream("pos", true), wantCode: codeAppendModeWithToast},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateToastStreams([]types.StreamInterface{tc.stream})
			if tc.wantCode == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Equal(t, tc.wantCode, errs.From(errs.Classify(err)).Code)
		})
	}
}
