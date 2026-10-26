package mssql

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
)

// Streams v1 (--catalog / streams.json) coverage, run alongside the streams v2 suites.
// Delete this file when streams v1 support is removed.

func TestMSSQLStreamsV1Discover(t *testing.T) {
	mssqlBaseConfig(t, testutils.WithStreamsV1()).TestDiscover(t)
}

func TestMSSQLStreamsV1Sync(t *testing.T) {
	t.Parallel()
	mssqlSyncConfig(t, testutils.WithStreamsV1()).TestSync(t)
}
