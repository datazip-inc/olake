package driver

import (
	"context"
	"fmt"
	"testing"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/utils"
	"github.com/docker/docker/api/types/container"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func TestMongodbPerformance(t *testing.T) {
	ctx := context.Background()
	config := abstract.GetTestConfig("mongodb")
	namespace := "mongodb"

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
								_, output, err := utils.ExecContainerCmd(ctx, c, abstract.InstallCmd())
								require.NoError(t, err, fmt.Sprintf("Failed to install dependencies:\n%s", string(output)))
							})

							db, err := connectDatabase(ctx)
							require.NoError(t, err, "Failed to connect to MongoDB")
							defer db.Client().Disconnect(ctx)

							t.Run("backfill", func(t *testing.T) {
								discoverCmd := abstract.DiscoverCommand(*config)
								_, output, err := utils.ExecContainerCmd(ctx, c, discoverCmd)
								require.NoError(t, err, fmt.Sprintf("Discover failed:\n%s", string(output)))
								t.Log(string(output))

								// TODO: change stream name
								updateStreamsCmd := abstract.UpdateStreamsCmd(*config, namespace, "test")
								_, _, err = utils.ExecContainerCmd(ctx, c, updateStreamsCmd)
								require.NoError(t, err, "Failed to update streams:\n%s")

								syncCmd := abstract.SyncCommand(*config, true)
								_, output, err = utils.ExecContainerCmd(ctx, c, syncCmd)
								require.NoError(t, err, fmt.Sprintf("Backfill sync failed:\n%s", string(output)))
								t.Log(string(output))

								success, err := abstract.IsRPSAboveBenchmark(config.Driver, true)
								require.NoError(t, err, "Failed to check RPS", err)
								require.True(t, success, "MongoDB backfill performance below benchmark")
								t.Log("✅ SUCCESS: mongodb backfill")
							})

							t.Run("cdc", func(t *testing.T) {
								err := setupMongoDBForCDC(ctx, db)
								require.NoError(t, err, "Failed to setup database for CDC")

								discoverCmd := abstract.DiscoverCommand(*config)
								_, output, err := utils.ExecContainerCmd(ctx, c, discoverCmd)
								require.NoError(t, err, fmt.Sprintf("Discover failed:\n%s", string(output)))
								t.Log(string(output))

								// TODO: change stream name
								updateStreamsCmd := abstract.UpdateStreamsCmd(*config, namespace, "test_cdc")
								_, _, err = utils.ExecContainerCmd(ctx, c, updateStreamsCmd)
								require.NoError(t, err, "Failed to update streams")

								syncCmd := abstract.SyncCommand(*config, true)
								_, output, err = utils.ExecContainerCmd(ctx, c, syncCmd)
								require.NoError(t, err, fmt.Sprintf("Initial sync failed:\n%s", string(output)))
								t.Log(string(output))

								// TODO: Change implementation to insert multiple documents
								_, err = db.Collection("test_cdc").InsertOne(ctx, bson.M{"id": 1, "name": "olake_user_cdc"})
								require.NoError(t, err, "Failed to insert test doc")

								syncCmd = abstract.SyncCommand(*config, false)
								_, output, err = utils.ExecContainerCmd(ctx, c, syncCmd)
								require.NoError(t, err, fmt.Sprintf("CDC sync failed:\n%s", string(output)))
								t.Log(string(output))

								success, err := abstract.IsRPSAboveBenchmark(config.Driver, false)
								require.NoError(t, err, "Failed to check RPS", err)
								require.True(t, success, "Mongodb CDC performance below benchmark")
								t.Log("✅ SUCCESS: mongodb cdc")
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

func connectDatabase(ctx context.Context) (*mongo.Database, error) {
	var cfg Mongo
	if err := utils.UnmarshalFile("./testconfig/source.json", &cfg.config, false); err != nil {
		return nil, err
	}
	cfg.Setup(ctx)
	return cfg.client.Database("mongodb"), nil
}

func setupMongoDBForCDC(ctx context.Context, db *mongo.Database) error {
	usersCDC := db.Collection("users_cdc")
	_, err := usersCDC.DeleteMany(ctx, bson.M{})
	return err
}
