package driver

import (
	"context"
	"fmt"
	"testing"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/utils"
	"github.com/docker/docker/api/types/container"
	"github.com/jmoiron/sqlx"
	_ "github.com/sijms/go-ora/v2"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

func TestOraclePerformance(t *testing.T) {
	ctx := context.Background()
	config := abstract.GetTestConfig("oracle")
	namespace := "ADMIN"

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

							db, err := connectDatabase(ctx)
							require.NoError(t, err, "Failed to connect to oracle")
							defer db.Close()

							t.Run("backfill", func(t *testing.T) {
								discoverCmd := abstract.DiscoverCommand(*config)
								_, out, err := utils.ExecContainerCmd(ctx, c, discoverCmd)
								require.NoError(t, err, fmt.Sprintf("Discover failed:\n%s", string(out)))
								t.Log(string(out))

								updateStreamsCmd := abstract.UpdateStreamsCmd(*config, namespace, "USERS")
								_, _, err = utils.ExecContainerCmd(ctx, c, updateStreamsCmd)
								require.NoError(t, err, "Failed to update streams")

								syncCmd := abstract.SyncCommand(*config, true)
								_, out, err = utils.ExecContainerCmd(ctx, c, syncCmd)
								require.NoError(t, err, fmt.Sprintf("Sync failed:\n%s", string(out)))
								t.Log(string(out))

								success, err := abstract.IsRPSAboveBenchmark("oracle", true)
								require.NoError(t, err, "Failed to check RPS", err)
								require.True(t, success, "Oracle backfill performance below benchmark")
								t.Log("✅ SUCCESS: oracle backfill")
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
	var cfg Oracle
	if err := utils.UnmarshalFile("./testconfig/source.json", &cfg.config, false); err != nil {
		return nil, err
	}
	cfg.Setup(ctx)
	return cfg.client, nil
}
