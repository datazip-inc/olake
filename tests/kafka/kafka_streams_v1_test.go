package kafka

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
)

// Streams v1 (--catalog / streams.json) coverage, run alongside the streams v2 suites.
// Delete this file when streams v1 support is removed.

func TestKafkaStreamsV1Discover(t *testing.T) {
	for _, format := range kafkaFormats(t) {
		t.Run(format.name, func(t *testing.T) {
			format.cfg.TestConfig.StreamsV1 = true
			format.cfg.TestDiscover(t)
		})
	}
}

func TestKafkaStreamsV1Sync(t *testing.T) {
	t.Parallel()
	for _, format := range kafkaFormats(t) {
		t.Run(format.name, func(t *testing.T) {
			t.Parallel()
			format.cfg.TestConfig.StreamsV1 = true
			format.cfg.IsolateSuite(t, testutils.StreamsV1Suite)
			format.cfg.TestSync(t)
		})
	}
}
