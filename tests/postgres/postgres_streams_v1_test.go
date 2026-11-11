package postgres

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
)

// Streams v1 (--catalog / streams.json) coverage, run alongside the streams v2 suites.
// Delete this file when streams v1 support is removed.

func TestPostgresStreamsV1Discover(t *testing.T) {
	cfg := postgresBaseConfig(t, testutils.WithStreamsV1())
	createReplicationSlot(t, cfg.TestConfig)
	cfg.TestDiscover(t)
}

func TestPostgresStreamsV1Sync(t *testing.T) {
	t.Parallel()
	postgresSyncConfig(t, testutils.WithStreamsV1()).TestSync(t)
}
