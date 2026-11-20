// Package destinationtest has the destinations unit tests write to, as testhandler.DestinationKind values.
// Their containers come from utils/testutils/testutils.mk, and cleanup removes one test's database from each.
package destinationtest

import (
	"context"

	"github.com/datazip-inc/olake/utils/testutils/testhandler"
)

const (
	bucket = "warehouse"
	region = "us-east-1"
	// serverUser and serverKey log in to MinIO, as destination/iceberg/local-test/catalog/docker-compose.yml sets them.
	serverUser = "olake"
	serverKey  = "olake-testhandler"
)

// minioService is the object store behind Parquet on S3 and behind every Iceberg warehouse.
var minioService = &testhandler.Service{
	Container: "olake-catalog-minio",
	Port:      19000,
	Target:    "testutils.minio.start",
	Ready: func(ctx context.Context, s *testhandler.Service) error {
		_, err := mc(ctx, s, "ls", "local/"+bucket)
		return err
	},
}

// mc runs the MinIO client bundled in the server image against the server itself.
func mc(ctx context.Context, s *testhandler.Service, args ...string) (string, error) {
	flags := []string{"--env", "MC_HOST_local=http://" + serverUser + ":" + serverKey + "@127.0.0.1:9000"}
	return s.Exec(ctx, flags, append([]string{"mc"}, args...)...)
}

// removePrefix deletes every object under a key prefix of the shared bucket.
func removePrefix(ctx context.Context, prefix string) error {
	_, err := mc(ctx, minioService, "rm", "--recursive", "--force", "local/"+bucket+"/"+prefix+"/")
	return err
}
