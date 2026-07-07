package abstract

import (
	"testing"

	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
)

type testCDCCheckpointProvider map[string]string

func (p testCDCCheckpointProvider) PersistedCDCCheckpoint(stream types.StreamInterface) string {
	return p[stream.ID()]
}

func TestCDCThreadSuffixes(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "leaves suffix empty when driver has no checkpoint provider",
			run: func(t *testing.T) {
				stream := types.NewStream("users", "public", nil).Wrap(0)

				suffixes := cdcThreadSuffixes([]types.StreamInterface{stream}, nil)

				require.Empty(t, suffixes[stream.ID()])
			},
		},
		{
			name: "uses stable initial suffix before source state exists",
			run: func(t *testing.T) {
				stream := types.NewStream("users", "public", nil).Wrap(0)

				firstSuffix := cdcThreadSuffixes([]types.StreamInterface{stream}, testCDCCheckpointProvider{})[stream.ID()]
				secondSuffix := cdcThreadSuffixes([]types.StreamInterface{stream}, testCDCCheckpointProvider{})[stream.ID()]

				require.Equal(t, "initial", firstSuffix)
				require.Equal(t, firstSuffix, secondSuffix)
				require.Equal(t, generateThreadID(stream.ID(), firstSuffix), generateThreadID(stream.ID(), secondSuffix))
			},
		},
		{
			name: "uses persisted checkpoint from driver",
			run: func(t *testing.T) {
				users := types.NewStream("users", "public", nil).Wrap(0)
				orders := types.NewStream("orders", "public", nil).Wrap(0)
				provider := testCDCCheckpointProvider{
					users.ID():  "0-16B6C50",
					orders.ID(): "0-16C0000",
				}

				suffixes := cdcThreadSuffixes([]types.StreamInterface{users, orders}, provider)

				require.Equal(t, "0-16B6C50", suffixes[users.ID()])
				require.Equal(t, "0-16C0000", suffixes[orders.ID()])
				require.Equal(t, "public.users_0-16B6C50", generateThreadID(users.ID(), suffixes[users.ID()]))
			},
		},
		{
			name: "changes thread id when persisted checkpoint changes",
			run: func(t *testing.T) {
				stream := types.NewStream("users", "public", nil).Wrap(0)

				firstSuffix := cdcThreadSuffixes([]types.StreamInterface{stream}, testCDCCheckpointProvider{stream.ID(): "token-1"})[stream.ID()]
				secondSuffix := cdcThreadSuffixes([]types.StreamInterface{stream}, testCDCCheckpointProvider{stream.ID(): "token-1"})[stream.ID()]
				thirdSuffix := cdcThreadSuffixes([]types.StreamInterface{stream}, testCDCCheckpointProvider{stream.ID(): "token-2"})[stream.ID()]

				require.Equal(t, firstSuffix, secondSuffix)
				require.NotEqual(t, secondSuffix, thirdSuffix)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}
