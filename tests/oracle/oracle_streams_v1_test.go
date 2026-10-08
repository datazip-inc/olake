package oracle

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
)

// Streams v1 (--catalog / streams.json) coverage, run alongside the streams v2 suites.
// Delete this file when streams v1 support is removed.

func TestOracleStreamsV1Discover(t *testing.T) {
	cfg := oracleBaseConfig(t)
	cfg.TestConfig.StreamsV1 = true
	cfg.TestDiscover(t)
}

func TestOracleStreamsV1Sync(t *testing.T) {
	t.Parallel()
	cfg := oracleSyncConfig(t)
	cfg.TestConfig.StreamsV1 = true
	cfg.IsolateSuite(t, testutils.StreamsV1Suite)
	cfg.TestSync(t)
}
