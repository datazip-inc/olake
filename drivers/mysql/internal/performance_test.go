package driver

import (
	"context"
	"fmt"
	"testing"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/utils"
	"github.com/docker/docker/api/types/container"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

const (
	mysqlDatabase = "mysql"
	mysqlPassword = "secret1234"
	mysqlUser     = "mysql"
	mysqlHost     = "localhost"
	mysqlPort     = 3306
)

func TestMySQLPerformance(t *testing.T) {
	ctx := context.Background()
	config := abstract.GetTestConfig("mysql")

	t.Run("performance", func(t *testing.T) {
		req := testcontainers.ContainerRequest{
			Image: "golang:1.23.2",
			HostConfigModifier: func(hc *container.HostConfig) {
				hc.Binds = []string{
					fmt.Sprintf("%s:/test-olake:rw", config.ProjectRoot),
				}
				hc.ExtraHosts = append(hc.ExtraHosts, "host.docker.internal:host-gateway")
			},
			ConfigModifier: func(c *container.Config) {
				c.WorkingDir = "/test-olake"
			},
			LifecycleHooks: []testcontainers.ContainerLifecycleHooks{
				{
					PostReadies: []testcontainers.ContainerHook{
						func(ctx context.Context, c testcontainers.Container) error {
							//install dependencies
							t.Run("install-dependencies", func(t *testing.T) {
								fmt.Printf("Installing dependencies in container")
								_, _, err := utils.ExecContainerCmd(ctx, c, abstract.InstallCmd())
								require.NoError(t, err, "Failed to install dependencies")
								t.Log("✅ Dependencies installed successfully")
							})

							//connect to mysql
							db, err := sqlx.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", mysqlUser, mysqlPassword, mysqlHost, mysqlPort, mysqlDatabase))
							if err != nil {
								return err
							}
							defer db.Close()
							t.Log("✅ Connected to MySQL successfully")

							//setup database for sync
							t.Run("backfill", func(t *testing.T) {
								err := setupDatabaseForBackfill(ctx, db)
								require.NoError(t, err, "Failed to setup database for backfill")
								t.Log("✅ Database setup completed successfully")

								// perform discover
								discoverCmd := abstract.DiscoverCommand("mysql", *config)
								_, _, err = utils.ExecContainerCmd(ctx, c, discoverCmd)
								require.NoError(t, err, "Failed to perform discover")

								// enable normalization
								_, _, err = utils.ExecContainerCmd(ctx, c, abstract.EnableNormalizationCmd("users", *config))
								require.NoError(t, err, "Failed to enable normalization")

								// perform sync
								syncCmd := abstract.SyncCommand("mysql", true, *config)
								_, _, err = utils.ExecContainerCmd(ctx, c, syncCmd)
								require.NoError(t, err, "Failed to perform sync")

								success, err := abstract.IsRPSAboveBenchmark("mysql", true)
								require.NoError(t, err, "Failed to check performance")
								require.True(t, success, "Performance test failed: mysql backfill")
								t.Log("✅ SUCCESS: mysql backfill")
							})

							t.Run("cdc", func(t *testing.T) {

								// NOW CDC
								err := setupDatabaseForCDC(ctx, db)
								require.NoError(t, err, "Failed to setup database for CDC")
								t.Log("✅ Database setup completed successfully for CDC")

								// perform discover
								_, _, err = utils.ExecContainerCmd(ctx, c, abstract.DiscoverCommand("mysql", *config))
								require.NoError(t, err, "Failed to perform discover")

								_, _, err = utils.ExecContainerCmd(ctx, c, abstract.EnableNormalizationCmd("users_cdc", *config))
								require.NoError(t, err, "Failed to enable normalization")

								// perform sync without entry
								_, _, err = utils.ExecContainerCmd(ctx, c, abstract.SyncCommand("mysql", true, *config))
								require.NoError(t, err, "Failed to perform sync")
								t.Log("✅ Initial sync completed successfully for CDC")

								// insert data into users_cdc
								_, err = db.ExecContext(ctx, "INSERT INTO users_cdc (id, name) VALUES (1, 'olake_user_cdc')")
								require.NoError(t, err, "Failed to insert data into users_cdc")
								t.Log("✅ Data inserted successfully")

								// perform sync with entry
								_, out, err := utils.ExecContainerCmd(ctx, c, abstract.SyncCommand("mysql", false, *config))
								require.NoError(t, err, "Failed to perform sync")
								t.Log("✅ CDC sync completed successfully")
								t.Log(string(out))

								success, err := abstract.IsRPSAboveBenchmark("mysql", false)
								require.NoError(t, err, "Failed to check RPS")
								require.True(t, success, "CDC RPS is less than benchmark RPS")
								t.Log("✅ SUCCESS: mysql cdc")
							})

							return nil
						},
					},
				},
			},
			Cmd: []string{"tail", "-f", "/dev/null"},
		}

		container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		require.NoError(t, err, "Failed to start container")
		defer container.Terminate(ctx)
	})

}

func setupDatabaseForBackfill(ctx context.Context, db *sqlx.DB) error {
	if _, err := db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS performance"); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, "USE performance"); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY, name VARCHAR(255))"); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, "TRUNCATE TABLE users"); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO users (id, name) VALUES (1, 'olake_user')"); err != nil {
		return err
	}
	return nil
}

func setupDatabaseForCDC(ctx context.Context, db *sqlx.DB) error {
	if _, err := db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS users_cdc (id INT PRIMARY KEY, name VARCHAR(255))"); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, "TRUNCATE TABLE users_cdc"); err != nil {
		return err
	}

	// TODO: transfer data from users to users_cdc before dropping
	if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS users"); err != nil {
		return err
	}
	return nil
}
