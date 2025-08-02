package testutils

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/spark-connect-go/v35/spark/sql"
	"github.com/datazip-inc/olake/utils"
	"github.com/docker/docker/api/types/container"

	// load pq driver for SQL tests
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

const (
	// Database and connection constants
	icebergDatabase     = "olake_iceberg"
	sparkConnectAddress = "sc://localhost:15002"

	// Test file constants
	TestStreamsFile = "test_streams.json"
	StreamsFile     = "streams.json"
	StateFile       = "state.json"
	StatsFile       = "stats.json"

	// Command constants
	installCmd = "apt-get update && apt-get install -y openjdk-17-jre-headless maven default-mysql-client postgresql postgresql-client wget gnupg iproute2 dnsutils iputils-ping netcat-openbsd nodejs npm jq && wget -qO - https://www.mongodb.org/static/pgp/server-8.0.asc | apt-key add - && echo 'deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/debian bookworm/mongodb-org/8.0 main' | tee /etc/apt/sources.list.d/mongodb-org/8.0.list && apt-get update && apt-get install -y mongodb-mongosh && npm install -g chalk-cli"

	// Container and path variables
	TestWorkingDir     = "/test-olake"
	HostDockerInternal = "host.docker.internal:host-gateway"
)

// Test variables
var (
	TestContainerImage = getTestContainerImage()
)

type IntegrationTest struct {
	Driver             string
	ExpectedData       map[string]interface{}
	ExpectedUpdateData map[string]interface{}
	DataTypeSchema     map[string]string
	ExecuteQuery       func(ctx context.Context, t *testing.T, tableName, operation string)
}

func (cfg *IntegrationTest) TestIntegration(t *testing.T) {
	ctx := context.Background()
	cwd, err := os.Getwd()
	t.Logf("Host working directory: %s", cwd)
	require.NoErrorf(t, err, "Failed to get current working directory")
	projectRoot := filepath.Join(cwd, "../../..")
	t.Logf("Root Project directory: %s", projectRoot)
	testdataDir := filepath.Join(projectRoot, "drivers", cfg.Driver, "internal", "testdata")
	t.Logf("Test data directory: %s", testdataDir)
	dummyStreamFilePath := filepath.Join(testdataDir, TestStreamsFile)
	testStreamFilePath := filepath.Join(testdataDir, StreamsFile)
	currentTestTable := fmt.Sprintf("%s_test_table_olake", cfg.Driver)
	var (
		sourceConfigPath      = fmt.Sprintf("%s/drivers/%s/internal/testdata/source.json", TestWorkingDir, cfg.Driver)
		streamsPath           = fmt.Sprintf("%s/drivers/%s/internal/testdata/%s", TestWorkingDir, cfg.Driver, StreamsFile)
		destinationConfigPath = fmt.Sprintf("%s/drivers/%s/internal/testdata/destination.json", TestWorkingDir, cfg.Driver)
		statePath             = fmt.Sprintf("%s/drivers/%s/internal/testdata/%s", TestWorkingDir, cfg.Driver, StateFile)
	)

	t.Run("Discover", func(t *testing.T) {
		req := testcontainers.ContainerRequest{
			Image: TestContainerImage,
			HostConfigModifier: func(hc *container.HostConfig) {
				hc.Binds = []string{
					fmt.Sprintf("%s:/test-olake:rw", projectRoot),
					fmt.Sprintf("%s:/test-olake/drivers/%s/internal/testdata:rw", testdataDir, cfg.Driver),
				}
				hc.ExtraHosts = append(hc.ExtraHosts, "host.docker.internal:host-gateway")
			},
			ConfigModifier: func(config *container.Config) {
				config.WorkingDir = "/test-olake"
			},
			Env: map[string]string{
				"TELEMETRY_DISABLED": "true",
			},
			LifecycleHooks: []testcontainers.ContainerLifecycleHooks{
				{
					PostReadies: []testcontainers.ContainerHook{
						func(ctx context.Context, c testcontainers.Container) error {
							// 1. Install required tools if using the base Go image
							if TestContainerImage == "golang:1.23.2" {
								if code, out, err := utils.ExecCommand(ctx, c, installCmd); err != nil || code != 0 {
									return fmt.Errorf("install failed (%d): %s\n%s", code, err, out)
								}
							}

							// 2. Query on test table (tools already installed in custom image)
							cfg.ExecuteQuery(ctx, t, currentTestTable, "create")
							cfg.ExecuteQuery(ctx, t, currentTestTable, "clean")
							cfg.ExecuteQuery(ctx, t, currentTestTable, "add")

							// 3. Run discover command
							discoverCmd := fmt.Sprintf("/test-olake/build.sh driver-%s discover --config %s", cfg.Driver, sourceConfigPath)
							if code, out, err := utils.ExecCommand(ctx, c, discoverCmd); err != nil || code != 0 {
								return fmt.Errorf("discover failed (%d): %s\n%s", code, err, string(out))
							}

							// 4. Verify streams.json file
							streamsJSON, err := os.ReadFile(dummyStreamFilePath)
							if err != nil {
								return fmt.Errorf("failed to read expected streams JSON: %s", err)
							}
							testStreamsJSON, err := os.ReadFile(testStreamFilePath)
							if err != nil {
								return fmt.Errorf("failed to read actual streams JSON: %s", err)
							}
							if !utils.NormalizedEqual(string(streamsJSON), string(testStreamsJSON)) {
								return fmt.Errorf("streams.json does not match expected test_streams.json\nExpected:\n%s\nGot:\n%s", string(streamsJSON), string(testStreamsJSON))
							}
							t.Logf("Generated streams validated with test streams")

							// 5. Clean up
							cfg.ExecuteQuery(ctx, t, currentTestTable, "drop")
							t.Logf("%s discover test-container clean up", cfg.Driver)
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
		defer func() {
			if err := container.Terminate(ctx); err != nil {
				t.Logf("warning: failed to terminate container: %v", err)
			}
		}()
	})

	t.Run("Sync", func(t *testing.T) {
		req := testcontainers.ContainerRequest{
			Image: TestContainerImage,
			HostConfigModifier: func(hc *container.HostConfig) {
				hc.Binds = []string{
					fmt.Sprintf("%s:/test-olake:rw", projectRoot),
					fmt.Sprintf("%s:/test-olake/drivers/%s/internal/testdata:rw", testdataDir, cfg.Driver),
				}
				hc.ExtraHosts = append(hc.ExtraHosts, "host.docker.internal:host-gateway")
			},
			ConfigModifier: func(config *container.Config) {
				config.WorkingDir = "/test-olake"
			},
			Env: map[string]string{
				"TELEMETRY_DISABLED": "true",
			},
			LifecycleHooks: []testcontainers.ContainerLifecycleHooks{
				{
					PostReadies: []testcontainers.ContainerHook{
						func(ctx context.Context, c testcontainers.Container) error {
							// 1. Install required tools if using the base Go image
							if TestContainerImage == "golang:1.23.2" {
								if code, out, err := utils.ExecCommand(ctx, c, installCmd); err != nil || code != 0 {
									return fmt.Errorf("install failed (%d): %s\n%s", code, err, out)
								}
							}

							// 2. Query on test table (tools already installed in custom image)
							cfg.ExecuteQuery(ctx, t, currentTestTable, "create")
							cfg.ExecuteQuery(ctx, t, currentTestTable, "clean")
							cfg.ExecuteQuery(ctx, t, currentTestTable, "add")

							streamUpdateCmd := fmt.Sprintf(
								`jq '(.selected_streams[][] | .normalization) = true' %s > /tmp/streams.json && mv /tmp/streams.json %s`,
								streamsPath, streamsPath,
							)
							if code, out, err := utils.ExecCommand(ctx, c, streamUpdateCmd); err != nil || code != 0 {
								return fmt.Errorf("failed to enable normalization in streams.json (%d): %s\n%s",
									code, err, out,
								)
							}
							t.Logf("Enabled normalization in %s", streamsPath)

							// Clean up any existing Iceberg table from previous runs
							t.Logf("Cleaning up any existing Iceberg table: %s", currentTestTable)
							cleanupIcebergTable(t, currentTestTable)

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
									dummySchema: cfg.ExpectedData,
								},
								{
									syncMode:    "CDC - insert",
									operation:   "insert",
									useState:    true,
									opSymbol:    "c",
									dummySchema: cfg.ExpectedData,
								},
								{
									syncMode:    "CDC - update",
									operation:   "update",
									useState:    true,
									opSymbol:    "u",
									dummySchema: cfg.ExpectedUpdateData,
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
										cfg.ExecuteQuery(ctx, t, currentTestTable, operation)
									}
									cmd = fmt.Sprintf("/test-olake/build.sh driver-%s sync --config %s --catalog %s --destination %s --state %s", cfg.Driver, sourceConfigPath, streamsPath, destinationConfigPath, statePath)
								} else {
									cmd = fmt.Sprintf("/test-olake/build.sh driver-%s sync --config %s --catalog %s --destination %s", cfg.Driver, sourceConfigPath, streamsPath, destinationConfigPath)
								}
								t.Logf("Executing command: %s", cmd)
								if code, out, err := utils.ExecCommand(ctx, c, cmd); err != nil || code != 0 {
									return fmt.Errorf("sync failed (%d): %s\n%s", code, err, out)
								}
								t.Logf("Sync successful for %s driver", cfg.Driver)
								VerifyIcebergSync(t, currentTestTable, cfg.DataTypeSchema, schema, opSymbol, cfg.Driver)
								return nil
							}

							// 3. Run Sync command and verify records in Iceberg
							for _, test := range testCases {
								if err := runSync(c, test.useState, test.operation, test.opSymbol, test.dummySchema); err != nil {
									return err
								}
							}

							// 4. Clean up
							cfg.ExecuteQuery(ctx, t, currentTestTable, "drop")
							t.Logf("%s sync test-container clean up", cfg.Driver)
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
		defer func() {
			// Clean up test files (in case PostReadies hook fails)
			cleanupTestFiles(t, testdataDir)
			if err := container.Terminate(ctx); err != nil {
				t.Logf("warning: failed to terminate container: %v", err)
			}
		}()
	})
}

// cleanupTestFiles removes test-generated files that should not persist between test runs
func cleanupTestFiles(t *testing.T, testdataDir string) {
	t.Helper()

	// Files to clean up
	filesToRemove := []string{
		StateFile,
		StatsFile,
		StreamsFile,
	}

	for _, filename := range filesToRemove {
		filepath := filepath.Join(testdataDir, filename)
		if err := os.Remove(filepath); err != nil && !os.IsNotExist(err) {
			t.Logf("Warning: Failed to remove test file %s: %v", filename, err)
		}
	}
}

// verifyIcebergSync verifies that data was correctly synchronized to Iceberg
func VerifyIcebergSync(t *testing.T, tableName string, datatypeSchema map[string]string, schema map[string]interface{}, opSymbol, driver string) {
	t.Helper()
	ctx := context.Background()
	spark, err := sql.NewSessionBuilder().Remote(sparkConnectAddress).Build(ctx)
	require.NoError(t, err, "Failed to connect to Spark Connect server")
	defer func() {
		if stopErr := spark.Stop(); stopErr != nil {
			t.Errorf("Failed to stop Spark session: %v", stopErr)
		}
	}()

	selectQuery := fmt.Sprintf(
		"SELECT * FROM %s.%s.%s WHERE _op_type = '%s'",
		icebergDatabase, icebergDatabase, tableName, opSymbol,
	)
	t.Logf("Executing query: %s", selectQuery)

	selectQueryDf, err := spark.Sql(ctx, selectQuery)
	require.NoError(t, err, "Failed to select query from the table")

	selectRows, err := selectQueryDf.Collect(ctx)
	require.NoError(t, err, "Failed to collect data rows from Iceberg")
	require.NotEmpty(t, selectRows, "No rows returned for _op_type = '%s'", opSymbol)

	// delete row checked
	if opSymbol == "d" {
		deletedID := selectRows[0].Value("_olake_id")
		require.NotEmpty(t, deletedID, "Delete verification failed: _olake_id should not be empty")
		return
	}

	for rowIdx, row := range selectRows {
		icebergMap := make(map[string]interface{}, len(schema)+1)
		for _, col := range row.FieldNames() {
			icebergMap[col] = row.Value(col)
		}
		for key, expected := range schema {
			icebergValue, ok := icebergMap[key]
			require.Truef(t, ok, "Row %d: missing column %q in Iceberg result", rowIdx, key)
			require.Equal(t, icebergValue, expected, "Row %d: mismatch on %q: Iceberg has %#v, expected %#v", rowIdx, key, icebergValue, expected)
		}
	}
	t.Logf("Verified Iceberg synced data with respect to data synced from source[%s] found equal", driver)

	describeQuery := fmt.Sprintf("DESCRIBE TABLE %s.%s.%s", icebergDatabase, icebergDatabase, tableName)
	describeDf, err := spark.Sql(ctx, describeQuery)
	require.NoError(t, err, "Failed to describe Iceberg table")

	describeRows, err := describeDf.Collect(ctx)
	require.NoError(t, err, "Failed to collect describe data from Iceberg")
	icebergSchema := make(map[string]string)
	for _, row := range describeRows {
		colName := row.Value("col_name").(string)
		dataType := row.Value("data_type").(string)
		if !strings.HasPrefix(colName, "#") {
			icebergSchema[colName] = dataType
		}
	}

	for col, dbType := range datatypeSchema {
		iceType, found := icebergSchema[col]
		require.True(t, found, "Column %s not found in Iceberg schema", col)

		expectedIceType, mapped := GlobalTypeMapping[dbType]
		if !mapped {
			t.Logf("No mapping defined for driver type %s (column %s), skipping check", dbType, col)
			break
		}
		require.Equal(t, expectedIceType, iceType,
			"Data type mismatch for column %s: expected %s, got %s", col, expectedIceType, iceType)
	}
	t.Logf("Verified datatypes in Iceberg after sync")
}

// cleanupIcebergTable drops the specified Iceberg table if it exists
func cleanupIcebergTable(t *testing.T, tableName string) {
	t.Helper()
	ctx := context.Background()
	spark, err := sql.NewSessionBuilder().Remote(sparkConnectAddress).Build(ctx)
	if err != nil {
		t.Logf("Warning: Failed to connect to Spark Connect server for cleanup: %v", err)
		return
	}
	defer func() {
		if stopErr := spark.Stop(); stopErr != nil {
			t.Logf("Warning: Failed to stop Spark session during cleanup: %v", stopErr)
		}
	}()

	dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS %s.%s.%s", icebergDatabase, icebergDatabase, tableName)
	t.Logf("Executing cleanup query: %s", dropQuery)

	_, err = spark.Sql(ctx, dropQuery)
	if err != nil {
		t.Logf("Warning: Failed to drop Iceberg table %s: %v", tableName, err)
	} else {
		t.Logf("Successfully cleaned up Iceberg table: %s", tableName)
	}
}

func getTestContainerImage() string {
	//TODO: create a custom image for testing and push it to docker hub
	if img := os.Getenv("OLAKE_TEST_IMAGE"); img != "" {
		return img
	}
	return "golang:1.23.2"
}
