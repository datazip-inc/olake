package driver

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/utils"
	"github.com/docker/docker/api/types/container"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

const (
	currentTestTable      = "postgres_test_table_olake"
	sourceConfigPath      = "/test-olake/drivers/postgres/internal/testdata/source.json"
	streamsPath           = "/test-olake/drivers/postgres/internal/testdata/streams.json"
	destinationConfigPath = "/test-olake/drivers/postgres/internal/testdata/destination.json"
	statePath             = "/test-olake/drivers/postgres/internal/testdata/state.json"
	installCmd            = "apt-get update && apt-get install -y openjdk-17-jre-headless maven postgresql-client iproute2 dnsutils iputils-ping netcat-openbsd nodejs npm jq && npm install -g chalk-cli"
)

func TestPostgresIntegration(t *testing.T) {
	ctx := context.Background()

	cwd, err := os.Getwd()
	t.Logf("Host working directory: %s", cwd)
	require.NoErrorf(t, err, "Failed to get current working directory")
	projectRoot := filepath.Join(cwd, "../../..")
	t.Logf("Root Project directory: %s", projectRoot)
	testdataDir := filepath.Join(projectRoot, "drivers/postgres/internal/testdata")
	dummyStreamFilePath := filepath.Join(testdataDir, "test_streams.json")

	t.Run("Discover", func(t *testing.T) {
		req := testcontainers.ContainerRequest{
			Image: "golang:1.23.2",
			HostConfigModifier: func(hc *container.HostConfig) {
				hc.Binds = []string{
					fmt.Sprintf("%s:/test-olake:rw", projectRoot),
					fmt.Sprintf("%s:/test-olake/drivers/postgres/internal/testdata:rw", testdataDir),
				}
				hc.ExtraHosts = append(hc.ExtraHosts, "host.docker.internal:host-gateway")
			},
			ConfigModifier: func(config *container.Config) {
				config.WorkingDir = "/test-olake"
			},
			Env: map[string]string{
				"DEBUG": "false",
			},
			LifecycleHooks: []testcontainers.ContainerLifecycleHooks{
				{
					PostReadies: []testcontainers.ContainerHook{
						func(ctx context.Context, c testcontainers.Container) error {
							// 1. Install required tools
							if code, out, err := utils.ExecCommand(ctx, c, installCmd); err != nil || code != 0 {
								return fmt.Errorf("install failed (%d): %w\n%s", code, err, out)
							}

							// 2. Create test table and insert data
							db, err := sqlx.ConnectContext(ctx, "postgres",
								"postgres://postgres@localhost:5433/postgres?sslmode=disable",
							)
							require.NoError(t, err, "failed to connect to postgres")
							defer db.Close()
							ExecuteQuery(ctx, t, db, currentTestTable, "create")
							ExecuteQuery(ctx, t, db, currentTestTable, "clean")
							ExecuteQuery(ctx, t, db, currentTestTable, "add")

							// 3. Run discover command
							discoverCmd := fmt.Sprintf("/test-olake/build.sh driver-postgres discover --config %s", sourceConfigPath)
							if code, out, err := utils.ExecCommand(ctx, c, discoverCmd); err != nil || code != 0 {
								return fmt.Errorf("discover failed (%d): %w\n%s", code, err, string(out))
							}

							// 4. Verify streams.json file
							streamsJSON, err := os.ReadFile(dummyStreamFilePath)
							if err != nil {
								return fmt.Errorf("failed to read expected streams JSON: %w", err)
							}
							testStreamsCmd := fmt.Sprintf("cat %s", streamsPath)
							_, testStreamJSON, err := utils.ExecCommand(ctx, c, testStreamsCmd)
							if err != nil {
								return fmt.Errorf("failed to read actual streams JSON: %w", err)
							}
							if !utils.NormalizedEqual(string(streamsJSON), string(testStreamJSON)) {
								return fmt.Errorf("streams.json does not match expected test_streams.json\nExpected:\n%s\nGot:\n%s", string(streamsJSON), string(testStreamJSON))
							}
							t.Logf("Generated streams validated with test streams")

							// 5. Clean up
							ExecuteQuery(ctx, t, db, currentTestTable, "drop")
							t.Logf("Postgres discover test-container clean up")
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
		require.NoError(t, err, "Container startup failed")
		defer container.Terminate(ctx)
	})

	t.Run("Sync", func(t *testing.T) {
		req := testcontainers.ContainerRequest{
			Image: "golang:1.23.2",
			HostConfigModifier: func(hc *container.HostConfig) {
				hc.Binds = []string{
					fmt.Sprintf("%s:/test-olake:rw", projectRoot),
					fmt.Sprintf("%s:/test-olake/drivers/postgres/internal/testdata:rw", testdataDir),
				}
				hc.ExtraHosts = append(hc.ExtraHosts, "host.docker.internal:host-gateway")
			},
			ConfigModifier: func(config *container.Config) {
				config.WorkingDir = "/test-olake"
			},
			Env: map[string]string{
				"DEBUG": "false",
			},
			LifecycleHooks: []testcontainers.ContainerLifecycleHooks{
				{
					PostReadies: []testcontainers.ContainerHook{
						func(ctx context.Context, c testcontainers.Container) error {
							// 1. Install required tools
							if code, out, err := utils.ExecCommand(ctx, c, installCmd); err != nil || code != 0 {
								return fmt.Errorf("install failed (%d): %w\n%s", code, err, out)
							}

							// 2. Create test table and insert data
							db, err := sqlx.ConnectContext(ctx, "postgres",
								"postgres://postgres@localhost:5433/postgres?sslmode=disable",
							)
							require.NoError(t, err, "failed to connect to postgres")
							defer db.Close()
							ExecuteQuery(ctx, t, db, currentTestTable, "create")
							ExecuteQuery(ctx, t, db, currentTestTable, "clean")
							ExecuteQuery(ctx, t, db, currentTestTable, "add")

							streamUpdateCmd := fmt.Sprintf(
								`jq '.selected_streams.public[] .normalization = true' %s > /tmp/streams.json && mv /tmp/streams.json %s`,
								streamsPath, streamsPath,
							)
							if code, out, err := utils.ExecCommand(ctx, c, streamUpdateCmd); err != nil || code != 0 {
								return fmt.Errorf("failed to enable normalization in streams.json (%d): %w\n%s",
									code, err, out,
								)
							}
							t.Logf("Enabled normalization in %s", streamsPath)

							testCases := []struct {
								syncMode    string
								operation   string
								useState    bool
								opSymbol    string
								dummySchema map[string]interface{}
							}{
								{
									syncMode:    "Full-Refresh",
									operation:   "",
									useState:    false,
									opSymbol:    "r",
									dummySchema: ExpectedPostgresData,
								},
								{
									syncMode:    "CDC - insert",
									operation:   "insert",
									useState:    true,
									opSymbol:    "c",
									dummySchema: ExpectedPostgresData,
								},
								{
									syncMode:    "CDC - update",
									operation:   "update",
									useState:    true,
									opSymbol:    "u",
									dummySchema: ExpectedUpdatedPostgresData,
								},
								{
									syncMode:    "CDC - delete",
									operation:   "delete",
									useState:    true,
									opSymbol:    "d",
									dummySchema: nil,
								},
							}

							runSync := func(c testcontainers.Container, useState bool, operation, opSymbol string, schema map[string]interface{}) error {
								var cmd string
								if useState {
									if operation != "" {
										ExecuteQuery(ctx, t, db, currentTestTable, operation)
									}
									cmd = fmt.Sprintf("/test-olake/build.sh driver-postgres sync --config %s --catalog %s --destination %s --state %s", sourceConfigPath, streamsPath, destinationConfigPath, statePath)
								} else {
									cmd = fmt.Sprintf("/test-olake/build.sh driver-postgres sync --config %s --catalog %s --destination %s", sourceConfigPath, streamsPath, destinationConfigPath)
								}

								if code, out, err := utils.ExecCommand(ctx, c, cmd); err != nil || code != 0 {
									return fmt.Errorf("sync failed (%d): %w\n%s", code, err, out)
								}
								t.Logf("Sync successfull")
								abstract.VerifyIcebergSync(t, currentTestTable, PostgresSchema, schema, opSymbol, "postgres")
								return nil
							}

							// 3. Run Sync command and verify records in Iceberg
							for _, test := range testCases {
								t.Logf("Running test for: %s", test.syncMode)
								if err := runSync(c, test.useState, test.operation, test.opSymbol, test.dummySchema); err != nil {
									return err
								}
							}

							// 4. Clean up
							ExecuteQuery(ctx, t, db, currentTestTable, "drop")
							t.Logf("Postgres sync test-container clean up")
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
		require.NoError(t, err, "Container startup failed")
		defer container.Terminate(ctx)
	})

}
