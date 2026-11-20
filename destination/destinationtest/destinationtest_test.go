package destinationtest

// Keep third-party imports to testify: go mod tidy follows this file's imports into the go.sum
// of every module whose tests import the package.
import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/datazip-inc/olake/destination"
	_ "github.com/datazip-inc/olake/destination/parquet"
	"github.com/datazip-inc/olake/utils/testutils/testhandler"
)

func TestHandlerDestination(t *testing.T) {
	testCases := []struct {
		name string
		kind testhandler.DestinationKind
		// prefix is where the kind keeps one test's files in the bucket.
		prefix func(database string) string
		// write creates what the product's writer would create for the test's database.
		write func(t *testing.T, dest *testhandler.Destination)
		// entries reports what the catalog still holds for the test's database.
		entries func(t *testing.T, database string) string
		// wantErr is why Open must refuse the kind once the test binary already uses first.
		wantErr string
		first   testhandler.DestinationKind
	}{
		{
			name:   "parquet on minio passes the writer's own check",
			kind:   ParquetMinIO,
			prefix: func(database string) string { return "parquet/" + database },
			write: func(t *testing.T, dest *testhandler.Destination) {
				_, err := destination.NewWriterPool(t.Context(), dest.Config, nil, 1)
				require.NoError(t, err)
			},
		},
		{
			name:    "a second parquet kind in one test binary is refused",
			kind:    ParquetLocal,
			wantErr: "keeps its first config",
			first:   ParquetMinIO,
		},
		{
			name:   "iceberg on a jdbc catalog",
			kind:   IcebergJDBC,
			prefix: func(database string) string { return "iceberg_jdbc/" + database },
			write: func(t *testing.T, dest *testhandler.Destination) {
				// No writer runs here, so the test creates the catalog's two tables itself, locked against a parallel run.
				_, err := psql(t.Context(), jdbcCatalogService, jdbcCatalogDatabase, `DO $$ BEGIN
					PERFORM pg_advisory_xact_lock(1);
					CREATE TABLE IF NOT EXISTS iceberg_tables (
						catalog_name VARCHAR(255) NOT NULL, table_namespace VARCHAR(255) NOT NULL, table_name VARCHAR(255) NOT NULL,
						metadata_location VARCHAR(1000), previous_metadata_location VARCHAR(1000),
						PRIMARY KEY (catalog_name, table_namespace, table_name));
					CREATE TABLE IF NOT EXISTS iceberg_namespace_properties (
						catalog_name VARCHAR(255) NOT NULL, namespace VARCHAR(255) NOT NULL, property_key VARCHAR(255) NOT NULL,
						property_value VARCHAR(1000),
						PRIMARY KEY (catalog_name, namespace, property_key));
					INSERT INTO iceberg_tables VALUES ('olake_iceberg', '`+dest.Database+`', 'users', 's3a://`+bucket+`/users', NULL);
					INSERT INTO iceberg_namespace_properties VALUES ('olake_iceberg', '`+dest.Database+`', 'exists', 'true');
				END $$`)
				require.NoError(t, err)
			},
			entries: func(t *testing.T, database string) string {
				rows, err := psql(t.Context(), jdbcCatalogService, jdbcCatalogDatabase,
					`SELECT table_name FROM iceberg_tables WHERE table_namespace = '`+database+`'
					 UNION ALL SELECT property_key FROM iceberg_namespace_properties WHERE namespace = '`+database+`'`)
				require.NoError(t, err)
				return rows
			},
		},
		{
			name:   "iceberg on a rest catalog",
			kind:   IcebergREST,
			prefix: func(database string) string { return "iceberg_rest/" + database },
			write: func(t *testing.T, dest *testhandler.Destination) {
				catalog := "http://" + restCatalogService.Addr() + "/v1/namespaces"
				header := map[string]string{"Content-Type": "application/json"}
				status, body, err := request(t.Context(), http.MethodPost, catalog, header, `{"namespace":["`+dest.Database+`"]}`)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, status, body)
				// The catalog writes the table's metadata file to MinIO itself.
				status, body, err = request(t.Context(), http.MethodPost, catalog+"/"+dest.Database+"/tables", header,
					`{"name":"users","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":false,"type":"long"}]}}`)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, status, body)
			},
			entries: func(t *testing.T, database string) string {
				status, body, err := request(t.Context(), http.MethodGet, "http://"+restCatalogService.Addr()+"/v1/namespaces/"+database, nil, "")
				require.NoError(t, err)
				if status == http.StatusNotFound {
					return ""
				}
				return body
			},
		},
		{
			name:   "iceberg on a hive metastore",
			kind:   IcebergHive,
			prefix: func(database string) string { return "iceberg_hive/" + database + ".db" },
			write: func(t *testing.T, dest *testhandler.Destination) {
				// The metastore creates the namespace folder in MinIO itself.
				_, err := beeline(t.Context(), "CREATE DATABASE "+dest.Database+" LOCATION 's3a://"+bucket+"/iceberg_hive/"+dest.Database+".db'")
				require.NoError(t, err)
			},
			entries: func(t *testing.T, database string) string {
				listed, err := beeline(t.Context(), "SHOW DATABASES")
				require.NoError(t, err)
				require.Contains(t, listed, "default")
				if strings.Contains(listed, database) {
					return listed
				}
				return ""
			},
		},
		{
			name:   "a second handler and a subtest share the test's turn on the iceberg writer",
			kind:   IcebergJDBC,
			prefix: func(database string) string { return "iceberg_jdbc/" + database },
			write: func(t *testing.T, dest *testhandler.Destination) {
				require.NotEqual(t, dest.Database, testhandler.New(t).Destination(IcebergGlue).Database)
				t.Run("subtest", func(t *testing.T) {
					require.NotEqual(t, dest.Database, testhandler.New(t).Destination(IcebergGlue).Database)
				})
			},
		},
		{
			name:   "iceberg on a glue catalog",
			kind:   IcebergGlue,
			prefix: func(database string) string { return "iceberg_glue/" + database + ".db" },
			write: func(t *testing.T, dest *testhandler.Destination) {
				status, body, err := glueCall(t.Context(), "CreateDatabase", `{"DatabaseInput":{"Name":"`+dest.Database+`"}}`)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, status, body)
				status, body, err = glueCall(t.Context(), "CreateTable", `{"DatabaseName":"`+dest.Database+`","TableInput":{"Name":"users"}}`)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, status, body)
			},
			entries: func(t *testing.T, database string) string {
				status, body, err := glueCall(t.Context(), "GetDatabase", `{"Name":"`+database+`"}`)
				require.NoError(t, err)
				if status != http.StatusOK && strings.Contains(body, "EntityNotFoundException") {
					return ""
				}
				return body
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.wantErr != "" {
				testhandler.New(t).Destination(tc.first)
				_, err := tc.kind.Open(t.Context(), t)
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			var dest *testhandler.Destination
			t.Run("test", func(t *testing.T) {
				dest = testhandler.New(t).Destination(tc.kind)
				tc.write(t, dest)
				// Stands in for a data file a writer left under the test's database.
				_, err := mc(t.Context(), minioService, "cp", "/etc/hostname", "local/"+bucket+"/"+tc.prefix(dest.Database)+"/users/data/part.parquet")
				require.NoError(t, err)
				require.NotEmpty(t, objects(t, tc.prefix(dest.Database)))
			})
			require.Empty(t, objects(t, tc.prefix(dest.Database)), "cleanup must delete the test's files")
			if tc.entries != nil {
				require.Empty(t, tc.entries(t, dest.Database), "cleanup must drop the test's catalog entries")
			}
		})
	}
}

// objects lists what the shared bucket holds under a key prefix.
func objects(t *testing.T, prefix string) string {
	t.Helper()
	listed, err := mc(t.Context(), minioService, "ls", "--recursive", "local/"+bucket+"/"+prefix+"/")
	require.NoError(t, err)
	return listed
}
