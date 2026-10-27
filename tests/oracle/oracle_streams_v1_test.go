package oracle

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
)

// Streams v1 (--catalog / streams.json) coverage, run alongside the streams v2 suites.
// Delete this file when streams v1 support is removed.

func TestOracleStreamsV1Discover(t *testing.T) {
	oracleBaseConfig(t, testutils.WithStreamsV1()).TestDiscover(t)
}

func TestOracleStreamsV1Sync(t *testing.T) {
	t.Parallel()
	oracleSyncConfig(t, testutils.WithStreamsV1()).TestSync(t)
}
