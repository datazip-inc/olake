package mongodb

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
)

// Streams v1 (--catalog / streams.json) coverage, run alongside the streams v2 suites.
// Delete this file when streams v1 support is removed.

func TestMongodbStreamsV1Discover(t *testing.T) {
	cfg := mongodbBaseConfig(t)
	cfg.TestConfig.StreamsV1 = true
	cfg.TestDiscover(t)
}

func TestMongodbStreamsV1Sync(t *testing.T) {
	t.Parallel()
	cfg := mongodbSyncConfig(t)
	cfg.TestConfig.StreamsV1 = true
	cfg.IsolateSuite(t, testutils.StreamsV1Suite)
	cfg.TestSync(t)
}
