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

func TestMySQLPerformance(t *testing.T) {
	ctx := context.Background()
	config := abstract.GetTestConfig("mysql")
	namespace := "mysql"

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
							t.Run("install-dependencies", func(t *testing.T) {
								fmt.Printf("Installing dependencies in container")
								_, output, err := utils.ExecContainerCmd(ctx, c, abstract.InstallCmd())
								require.NoError(t, err, fmt.Sprintf("Failed to install dependencies:\n%s", string(output)))
							})

							db, err := connectDatabase(ctx)
							require.NoError(t, err, "Failed to connect to MySQL")
							defer db.Close()

							t.Run("backfill", func(t *testing.T) {
								discoverCmd := abstract.DiscoverCommand(*config)
								_, output, err := utils.ExecContainerCmd(ctx, c, discoverCmd)
								require.NoError(t, err, fmt.Sprintf("Failed to perform discover:\n%s", string(output)))
								t.Log(string(output))

								updateStreamsCmd := abstract.UpdateStreamsCmd(*config, namespace, "users")
								_, _, err = utils.ExecContainerCmd(ctx, c, updateStreamsCmd)
								require.NoError(t, err, "Failed to update streams")

								syncCmd := abstract.SyncCommand(*config, true)
								_, output, err = utils.ExecContainerCmd(ctx, c, syncCmd)
								require.NoError(t, err, fmt.Sprintf("Failed to perform sync:\n%s", string(output)))
								t.Log(string(output))

								success, err := abstract.IsRPSAboveBenchmark(config.Driver, true)
								require.NoError(t, err, "Failed to check RPS", err)
								require.True(t, success, "MySQL backfill performance below benchmark")
								t.Log("✅ SUCCESS: mysql backfill")
							})

							t.Run("cdc", func(t *testing.T) {
								err := setupDatabaseForCDC(ctx, db)
								require.NoError(t, err, "Failed to setup database for CDC")

								discoverCmd := abstract.DiscoverCommand(*config)
								_, output, err := utils.ExecContainerCmd(ctx, c, discoverCmd)
								require.NoError(t, err, fmt.Sprintf("Failed to perform discover:\n%s", string(output)))
								t.Log(string(output))

								updateStreamsCmd := abstract.UpdateStreamsCmd(*config, namespace, "test_cdc")
								_, _, err = utils.ExecContainerCmd(ctx, c, updateStreamsCmd)
								require.NoError(t, err, "Failed to update streams")

								syncCmd := abstract.SyncCommand(*config, true)
								_, output, err = utils.ExecContainerCmd(ctx, c, syncCmd)
								require.NoError(t, err, fmt.Sprintf("Failed to perform sync:\n%s", string(output)))
								t.Log(string(output))

								_, err = db.ExecContext(ctx, "INSERT INTO test_cdc SELECT * FROM test")
								require.NoError(t, err, string(output))
								t.Log(string(output))

								syncCmd = abstract.SyncCommand(*config, false)
								_, output, err = utils.ExecContainerCmd(ctx, c, syncCmd)
								require.NoError(t, err, fmt.Sprintf("Failed to perform sync:\n%s", string(output)))
								t.Log(string(output))

								success, err := abstract.IsRPSAboveBenchmark(config.Driver, false)
								require.NoError(t, err, "Failed to check RPS", err)
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

func connectDatabase(ctx context.Context) (*sqlx.DB, error) {
	var cfg MySQL
	if err := utils.UnmarshalFile("./testconfig/source.json", &cfg.config, false); err != nil {
		return nil, err
	}
	cfg.Setup(ctx)
	return cfg.client, nil
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
