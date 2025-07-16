package driver

import (
	"context"
	"fmt"
	"testing"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/utils"
	"github.com/docker/docker/api/types/container"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

func TestPostgresPerformance(t *testing.T) {
	ctx := context.Background()
	config := abstract.GetTestConfig("postgres")
	namespace := "public"
	connStr := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=disable",
		abstract.GetEnv("POSTGRES_USER", "postgres"),
		abstract.GetEnv("POSTGRES_PASSWORD", "secret1234"),
		abstract.GetEnv("POSTGRES_HOST", "localhost"),
		abstract.GetEnv("POSTGRES_PORT", "5433"),
		abstract.GetEnv("POSTGRES_DATABASE", "postgres"),
	)

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
								_, _, err := utils.ExecContainerCmd(ctx, c, abstract.InstallCmd())
								require.NoError(t, err, "Failed to install dependencies")
							})

							db, err := sqlx.Open("postgres", connStr)
							require.NoError(t, err, "Failed to connect to postgres")
							defer db.Close()

							t.Run("backfill", func(t *testing.T) {
								err = setupDatabaseForBackfill(ctx, db)
								require.NoError(t, err, "Failed to setup database for backfill")

								discoverCmd := abstract.DiscoverCommand("postgres", *config)
								_, out, err := utils.ExecContainerCmd(ctx, c, discoverCmd)
								require.NoError(t, err, fmt.Sprintf("Failed to perform discover:\n%s", string(out)))
								t.Log(string(out))

								updateStreamsCmd := abstract.UpdateStreamsCmd(*config, namespace, "users")
								_, _, err = utils.ExecContainerCmd(ctx, c, updateStreamsCmd)
								require.NoError(t, err, "Failed to update streams")

								syncCmd := abstract.SyncCommand("postgres", true, *config)
								_, out, err = utils.ExecContainerCmd(ctx, c, syncCmd)
								require.NoError(t, err, fmt.Sprintf("Failed to perform sync:\n%s", string(out)))
								t.Log(string(out))

								success, err := abstract.IsRPSAboveBenchmark("postgres", true)
								require.NoError(t, err, "Failed to check RPS", err)
								require.True(t, success, "Postgres backfill performance below benchmark")
								t.Log("✅ SUCCESS: postgres backfill")
							})

							t.Run("cdc", func(t *testing.T) {
								err := setupDatabaseForCDC(ctx, db)
								require.NoError(t, err, "Failed to setup database for cdc")

								discoverCmd := abstract.DiscoverCommand("postgres", *config)
								_, out, err := utils.ExecContainerCmd(ctx, c, discoverCmd)
								require.NoError(t, err, fmt.Sprintf("Failed to perform discover:\n%s", string(out)))
								t.Log(string(out))

								updateStreamsCmd := abstract.UpdateStreamsCmd(*config, namespace, "users_cdc")
								_, _, err = utils.ExecContainerCmd(ctx, c, updateStreamsCmd)
								require.NoError(t, err, "Failed to enable normalization")

								syncCmd := abstract.SyncCommand("postgres", true, *config)
								_, out, err = utils.ExecContainerCmd(ctx, c, syncCmd)
								require.NoError(t, err, fmt.Sprintf("Failed to perform initial sync:\n%s", string(out)))
								t.Log(string(out))

								_, err = db.ExecContext(ctx, "INSERT INTO users_cdc (id, name) VALUES (1, 'olake_user_cdc')")
								require.NoError(t, err, "Failed to insert data into users_cdc")

								syncCmd = abstract.SyncCommand("postgres", false, *config)
								_, out, err = utils.ExecContainerCmd(ctx, c, syncCmd)
								require.NoError(t, err, fmt.Sprintf("Failed to perform CDC sync:\n%s", string(out)))
								t.Log(string(out))

								success, err := abstract.IsRPSAboveBenchmark("postgres", false)
								require.NoError(t, err, "Failed to check RPS", err)
								require.True(t, success, "Postgres CDC performance below benchmark")
								t.Log("✅ SUCCESS: postgres cdc")
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
