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
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestMongodbPerformance(t *testing.T) {
	ctx := context.Background()
	config := abstract.GetTestConfig("mongodb")
	namespace := "mongodb"
	mongoURI := fmt.Sprintf(
		"mongodb://%s:%s/?replicaSet=%s",
		abstract.GetEnv("MONGODB_HOST", "localhost"),
		abstract.GetEnv("MONGODB_PORT", "27017"),
		abstract.GetEnv("MONGODB_REPLICA_SET", "rs0"),
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
								_, output, err := utils.ExecContainerCmd(ctx, c, abstract.InstallCmd())
								require.NoError(t, err, fmt.Sprintf("Failed to install dependencies:\n%s", string(output)))
							})

							client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
							require.NoError(t, err, "Failed to connect to MongoDB")
							defer client.Disconnect(ctx)
							db := client.Database("mongodb")

							t.Run("backfill", func(t *testing.T) {
								err := setupMongoDBForBackfill(ctx, db)
								require.NoError(t, err, "Failed to setup database for backfill")

								discoverCmd := abstract.DiscoverCommand("mongodb", *config)
								_, output, err := utils.ExecContainerCmd(ctx, c, discoverCmd)
								require.NoError(t, err, fmt.Sprintf("Discover failed:\n%s", string(output)))
								t.Log(string(output))

								updateStreamsCmd := abstract.UpdateStreamsCmd(*config, namespace, "users")
								_, _, err = utils.ExecContainerCmd(ctx, c, updateStreamsCmd)
								require.NoError(t, err, "Failed to update streams:\n%s")

								syncCmd := abstract.SyncCommand("mongodb", true, *config)
								_, output, err = utils.ExecContainerCmd(ctx, c, syncCmd)
								require.NoError(t, err, fmt.Sprintf("Backfill sync failed:\n%s", string(output)))
								t.Log(string(output))

								success, err := abstract.IsRPSAboveBenchmark("mongodb", true)
								require.NoError(t, err, "Failed to check RPS", err)
								require.True(t, success, "MongoDB backfill performance below benchmark")
								t.Log("✅ SUCCESS: mongodb backfill")
							})

							t.Run("cdc", func(t *testing.T) {
								err := setupMongoDBForCDC(ctx, db)
								require.NoError(t, err, "Failed to setup database for CDC")

								discoverCmd := abstract.DiscoverCommand("mongodb", *config)
								_, output, err := utils.ExecContainerCmd(ctx, c, discoverCmd)
								require.NoError(t, err, fmt.Sprintf("Discover failed:\n%s", string(output)))
								t.Log(string(output))

								updateStreamsCmd := abstract.UpdateStreamsCmd(*config, namespace, "users_cdc")
								_, _, err = utils.ExecContainerCmd(ctx, c, updateStreamsCmd)
								require.NoError(t, err, "Failed to update streams")

								syncCmd := abstract.SyncCommand("mongodb", true, *config)
								_, output, err = utils.ExecContainerCmd(ctx, c, syncCmd)
								require.NoError(t, err, fmt.Sprintf("Initial sync failed:\n%s", string(output)))
								t.Log(string(output))

								_, err = db.Collection("users_cdc").InsertOne(ctx, bson.M{"id": 1, "name": "olake_user_cdc"})
								require.NoError(t, err, "Failed to insert test doc")

								syncCmd = abstract.SyncCommand("mongodb", false, *config)
								_, output, err = utils.ExecContainerCmd(ctx, c, syncCmd)
								require.NoError(t, err, fmt.Sprintf("CDC sync failed:\n%s", string(output)))
								t.Log(string(output))

								success, err := abstract.IsRPSAboveBenchmark("mongodb", false)
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

func setupMongoDBForBackfill(ctx context.Context, db *mongo.Database) error {
	users := db.Collection("users")
	_, err := users.DeleteMany(ctx, bson.M{})
	if err != nil {
		return err
	}
	_, err = users.InsertOne(ctx, bson.M{"id": 1, "name": "olake_user"})
	return err
}

func setupMongoDBForCDC(ctx context.Context, db *mongo.Database) error {
	usersCDC := db.Collection("users_cdc")
	_, err := usersCDC.DeleteMany(ctx, bson.M{})
	return err
}
