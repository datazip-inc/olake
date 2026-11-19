package destinationtest

import (
	"context"

	"github.com/datazip-inc/olake/utils/testutils/testhandler"
)

// hiveMetastoreService is a standalone Hive metastore on embedded Derby.
var hiveMetastoreService = &testhandler.Service{
	Container: "olake-catalog-hive-metastore",
	Port:      19083,
	Target:    "testutils.hive-metastore.start",
	// Probed inside the container: Docker's port proxy accepts host connections before the server listens.
	Ready: func(ctx context.Context, s *testhandler.Service) error {
		_, err := s.Exec(ctx, nil, "bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/9083")
		return err
	},
}

// IcebergHive writes Iceberg tables through a Hive metastore. hive_clients must be set: the writer
// sends 0 otherwise and then never reaches the metastore.
var IcebergHive testhandler.DestinationKind = &icebergKind{
	name:      "iceberg-hive",
	catalog:   hiveMetastoreService,
	warehouse: "s3a://" + bucket + "/iceberg_hive",
	config: map[string]any{
		"catalog_type":      "hive",
		"hive_uri":          "thrift://" + hiveMetastoreService.Addr(),
		"hive_clients":      5,
		"hive_sasl_enabled": false,
	},
	dropNamespace: func(ctx context.Context, namespace string) error {
		_, err := beeline(ctx, "DROP DATABASE IF EXISTS "+namespace+" CASCADE")
		return err
	},
	files: func(namespace string) string {
		return "iceberg_hive/" + namespace + ".db"
	},
}

// beeline runs one statement on the metastore with the beeline bundled in its image.
// Beeline needs a terminal, and it must be pointed at the running metastore or it opens its own Derby.
func beeline(ctx context.Context, statement string) (string, error) {
	flags := []string{"--tty", "--env", "HADOOP_CLIENT_OPTS=-Dhive.metastore.uris=thrift://localhost:9083"}
	return hiveMetastoreService.Exec(ctx, flags, "beeline", "-u", "jdbc:hive2://", "-e", statement)
}
