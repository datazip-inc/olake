package performance

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/stretchr/testify/require"
)

type Test struct {
	*testutils.TestConfig
	BackfillStreams []string
	CDCStreams      []string
}

// Validate checks that the Test is valid and complete in order to derive/setup.
func (c *Test) Validate(t *testing.T) {
	t.Helper()
	require.NotNil(t, c.TestConfig, "performance.Test.TestConfig is not set")
	c.TestConfig.Validate(t)
	// A benchmark with nothing to read still reports a rate, and it is the rate of doing nothing.
	require.Falsef(t, len(c.BackfillStreams) == 0 && len(c.CDCStreams) == 0,
		"performance.Test declares neither BackfillStreams nor CDCStreams")
	// TODO: assert BackfillStreams and CDCStreams are disjoint. GetBackfillStreamsFromCDC derives
	// one from the other by trimming "_cdc", so a CDC stream without that suffix passes through
	// unchanged and is counted on both sides of the ratio.
}
