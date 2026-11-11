package s3

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
)

// Streams v1 (--catalog / streams.json) coverage, run alongside the streams v2 suites.
// Delete this file when streams v1 support is removed.

func TestS3StreamsV1Discover(t *testing.T) {
	for _, variant := range S3TestVariants {
		t.Run(variant.Name, func(t *testing.T) {
			s3BaseConfig(t, variant, testutils.WithStreamsV1()).TestDiscover(t)
		})
	}
}

func TestS3StreamsV1Sync(t *testing.T) {
	t.Parallel()
	for _, variant := range S3TestVariants {
		t.Run(variant.Name, func(t *testing.T) {
			t.Parallel()
			s3SyncConfig(t, variant, testutils.WithStreamsV1()).TestSync(t)
		})
	}
}
