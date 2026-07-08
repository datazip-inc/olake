package abstract

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCDCThreadSuffix(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "uses stable initial suffix before source state exists",
			run: func(t *testing.T) {
				firstSuffix := cdcThreadSuffix("", 0, false)
				secondSuffix := cdcThreadSuffix("", 0, false)

				require.Equal(t, "initial", firstSuffix)
				require.Equal(t, firstSuffix, secondSuffix)
			},
		},
		{
			name: "uses persisted checkpoint from driver",
			run: func(t *testing.T) {
				suffix := cdcThreadSuffix("0-16B6C50", 0, false)

				require.Equal(t, "0-16B6C50", suffix)
				require.Equal(t, "public.users_0-16B6C50", generateThreadID("public.users", suffix))
			},
		},
		{
			name: "changes thread id when persisted checkpoint changes",
			run: func(t *testing.T) {
				firstSuffix := cdcThreadSuffix("token-1", 0, false)
				secondSuffix := cdcThreadSuffix("token-1", 0, false)
				thirdSuffix := cdcThreadSuffix("token-2", 0, false)

				require.Equal(t, firstSuffix, secondSuffix)
				require.NotEqual(t, secondSuffix, thirdSuffix)
			},
		},
		{
			name: "adds reader identifier for parallel cdc workers",
			run: func(t *testing.T) {
				firstReaderSuffix := cdcThreadSuffix("group-1", 0, true)
				secondReaderSuffix := cdcThreadSuffix("group-1", 1, true)

				require.Equal(t, "group-1-reader[0]", firstReaderSuffix)
				require.Equal(t, "group-1-reader[1]", secondReaderSuffix)
				require.NotEqual(t, generateThreadID("topics.events", firstReaderSuffix), generateThreadID("topics.events", secondReaderSuffix))
			},
		},
		{
			name: "keeps parallel initial suffix deterministic per reader",
			run: func(t *testing.T) {
				firstAttemptSuffix := cdcThreadSuffix("", 2, true)
				retrySuffix := cdcThreadSuffix("", 2, true)

				require.Equal(t, "initial-reader[2]", firstAttemptSuffix)
				require.Equal(t, firstAttemptSuffix, retrySuffix)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}
