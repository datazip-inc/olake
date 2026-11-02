package mysql

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
)

// Streams v1 (--catalog / streams.json) coverage, run alongside the streams v2 suites.
// Delete this file when streams v1 support is removed.

func TestMySQLStreamsV1Discover(t *testing.T) {
	mysqlBaseConfig(t, testutils.WithStreamsV1()).TestDiscover(t)
}

func TestMySQLStreamsV1Sync(t *testing.T) {
	t.Parallel()
	mysqlSyncConfig(t, testutils.WithStreamsV1()).TestSync(t)
}
