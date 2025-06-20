package driver

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/datazip-inc/olake/utils"
	"github.com/docker/docker/api/types/container"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// import (
// 	"testing"

// 	"github.com/datazip-inc/olake/drivers/abstract"
// )

// // Test functions using base utilities
// func TestPostgresSetup(t *testing.T) {
// 	_, absDriver := testPostgresClient(t)
// 	absDriver.TestSetup(t)
// }

// func TestPostgresDiscover(t *testing.T) {
// 	client, absDriver := testPostgresClient(t)
// 	absDriver.TestDiscover(t, client, ExecuteQuery)
// }

// func TestPostgresRead(t *testing.T) {
// 	client, absDriver := testPostgresClient(t)
// 	absDriver.TestRead(t, client, ExecuteQuery, abstract.PostgresSchema)
// }

const (
	testTablePrefix       = "test_table_olake"
	sparkConnectAddress   = "sc://spark-iceberg:15002"
	icebergDatabase       = "olake_iceberg"
	cdcInitializationWait = 2 * time.Second
	cdcProcessingWait     = 60 * time.Second
)

var currentTestTable = fmt.Sprintf("%s_%d", testTablePrefix, time.Now().Unix())

func setupPostgresClient(t *testing.T) *sqlx.DB {
	t.Helper()

	config := Config{
		Host:     defaultPostgresHost,
		Port:     defaultPostgresPort,
		Username: defaultPostgresUser,
		Password: defaultPostgresPassword,
		Database: defaultPostgresDB,
		SSLConfiguration: &utils.SSLConfig{
			Mode: "disable",
		},
		BatchSize: defaultBatchSize,
	}

	pgDriver := &Postgres{
		config: &config,
	}
	pgDriver.CDCSupport = true
	pgDriver.cdcConfig = CDC{
		InitialWaitTime: defaultCDCWaitTime,
		ReplicationSlot: defaultReplicationSlot,
	}

	err := pgDriver.Setup(context.Background())
	require.NoError(t, err, "Failed to setup PostgreSQL driver")
	return pgDriver.client
}

func TestPostgresIntegration(t *testing.T) {
	ctx := context.Background()

	cwd, err := os.Getwd()
	require.NoErrorf(t, err, "Failed to get current working directory")
	projectRoot := filepath.Join(cwd, "../../../..")

	sourceConfigPath := "drivers/postgres/internal/testdata/source.json"
	streamsPath := "drivers/postgres/internal/testdata/streams.json"
	// destinationConfigPath := "drivers/postgres/internal/testdata/destination.json"
	// statePath := "drivers/postgres/internal/testdata/state.json"

	db := setupPostgresClient(t)
	defer db.Close()

	defer ExecuteQuery(ctx, t, db, currentTestTable, "drop")

	t.Run("Discover", func(t *testing.T) {
		ExecuteQuery(ctx, t, db, currentTestTable, "create")
		ExecuteQuery(ctx, t, db, currentTestTable, "clean")
		ExecuteQuery(ctx, t, db, currentTestTable, "add")

		req := testcontainers.ContainerRequest{
			Image: "golang:1.23.2",
			Files: []testcontainers.ContainerFile{
				{
					HostFilePath:      projectRoot,
					ContainerFilePath: "/olake",
					FileMode:          0755,
				},
			},
			ConfigModifier: func(config *container.Config) {
				config.WorkingDir = "/olake"
			},
			Cmd: []string{
				"/bin/sh", "-c", fmt.Sprintf("./build.sh driver-postgres discover --config %s > %s", sourceConfigPath, streamsPath),
			},
			Networks:   []string{"iceberg_net"},
			WaitingFor: wait.ForListeningPort("5433/tcp"),
		}

		discoverContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		require.NoError(t, err, "Failed to start discover container")
		defer discoverContainer.Terminate(ctx)

		streamsData, err := os.ReadFile(filepath.Join(projectRoot, streamsPath))
		require.NoError(t, err, "Failed to read test_streams.json")
		assert.Contains(t, string(streamsData), currentTestTable, "Test table not found in streams")
	})

}
