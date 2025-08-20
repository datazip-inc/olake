package testutils

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/spark-connect-go/v35/spark/sql"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/docker/docker/api/types/container"

	// load pq driver for SQL tests
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

const (
	icebergDatabase     = "olake_iceberg"
	sparkConnectAddress = "sc://localhost:15002"
	installCmd          = "apt-get update && apt-get install -y openjdk-17-jre-headless maven default-mysql-client postgresql postgresql-client wget gnupg iproute2 dnsutils iputils-ping netcat-openbsd nodejs npm jq && wget -qO - https://www.mongodb.org/static/pgp/server-8.0.asc | gpg --dearmor -o /usr/share/keyrings/mongodb-server-8.0.gpg && echo 'deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg ] https://repo.mongodb.org/apt/debian bookworm/mongodb-org/8.0 main' | tee /etc/apt/sources.list.d/mongodb-org-8.0.list && apt-get update && apt-get install -y mongodb-mongosh && npm install -g chalk-cli"
	SyncTimeout         = 10 * time.Minute
	BenchmarkThreshold  = 0.9
)

type IntegrationTest struct {
	TestConfig         *TestConfig
	ExpectedData       map[string]interface{}
	ExpectedUpdateData map[string]interface{}
	DataTypeSchema     map[string]string
	Namespace          string
	ExecuteQuery       func(ctx context.Context, t *testing.T, streams []string, operation string, fileConfig bool)
}

type PerformanceTest struct {
	TestConfig      *TestConfig
	Namespace       string
	BackfillStreams []string
	CDCStreams      []string
	ExecuteQuery    func(ctx context.Context, t *testing.T, streams []string, operation string, fileConfig bool)
}

type benchmarkStats struct {
	Backfill float64
	CDC      float64
}
type SyncSpeed struct {
	Speed string `json:"Speed"`
}
type TestConfig struct {
	Driver              string
	HostRootPath        string
	SourcePath          string
	CatalogPath         string
	DestinationPath     string
	StatePath           string
	StatsPath           string
	HostTestDataPath    string
	HostCatalogPath     string
	HostTestCatalogPath string
}

// this benchmark is for performance test which runs on a github runner
// for absolute benchmarks, please checkout olake docs: https://olake.io/docs/connectors/postgres/benchmarks
var benchmarks = map[constants.DriverType]benchmarkStats{
	constants.MySQL:    {Backfill: 15906.40, CDC: 15648.93},
	constants.Postgres: {Backfill: 12000, CDC: 1500},
	constants.Oracle:   {Backfill: 3500, CDC: 0},
	constants.MongoDB:  {Backfill: 0, CDC: 0},
}

// GetTestConfig returns the test config for the given driver
func GetTestConfig(driver string) *TestConfig {
	// pwd is olake/drivers/(driver)/internal
	pwd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	// root path is olake's root path
	rootPath := filepath.Join(pwd, "../../..")

	containerTestDataPath := "/test-olake/drivers/%s/internal/testdata/%s"
	hostTestDataPath := filepath.Join(rootPath, "drivers", "%s", "internal", "testdata", "%s")
	return &TestConfig{
		Driver:              driver,
		HostRootPath:        rootPath,
		HostTestDataPath:    fmt.Sprintf(hostTestDataPath, driver, ""),
		HostTestCatalogPath: fmt.Sprintf(hostTestDataPath, driver, "test_streams.json"),
		HostCatalogPath:     fmt.Sprintf(hostTestDataPath, driver, "streams.json"),
		SourcePath:          fmt.Sprintf(containerTestDataPath, driver, "source.json"),
		CatalogPath:         fmt.Sprintf(containerTestDataPath, driver, "streams.json"),
		DestinationPath:     fmt.Sprintf(containerTestDataPath, driver, "destination.json"),
		StatePath:           fmt.Sprintf(containerTestDataPath, driver, "state.json"),
		StatsPath:           fmt.Sprintf(containerTestDataPath, driver, "stats.json"),
	}
}

func syncCommand(config TestConfig, useState bool) string {
	baseCmd := fmt.Sprintf("/test-olake/build.sh driver-%s sync --config %s --catalog %s --destination %s", config.Driver, config.SourcePath, config.CatalogPath, config.DestinationPath)
	if useState {
		baseCmd = fmt.Sprintf("%s --state %s", baseCmd, config.StatePath)
	}
	return baseCmd
}

func discoverCommand(config TestConfig) string {
	return fmt.Sprintf("/test-olake/build.sh driver-%s discover --config %s", config.Driver, config.SourcePath)
}

// TODO: check if we can remove namespace from being passed as a parameter and use a common namespace for all drivers
func updateStreamsCommand(config TestConfig, namespace string, stream []string, isBackfill bool) string {
	if len(stream) == 0 {
		return ""
	}
	streamConditions := make([]string, len(stream))
	for i, s := range stream {
		streamConditions[i] = fmt.Sprintf(`.stream_name == "%s"`, s)
	}
	condition := strings.Join(streamConditions, " or ")
	tmpCatalog := fmt.Sprintf("/tmp/%s_%s_streams.json", config.Driver, utils.Ternary(isBackfill, "backfill", "cdc").(string))
	jqExpr := fmt.Sprintf(
		`jq '.selected_streams = { "%s": (.selected_streams["%s"] | map(select(%s) | .normalization = true)) }' %s > %s && mv %s %s`,
		namespace,
		namespace,
		condition,
		config.CatalogPath,
		tmpCatalog,
		tmpCatalog,
		config.CatalogPath,
	)
	return jqExpr
}

// to get backfill streams from cdc streams e.g. "demo_cdc" -> "demo"
func GetBackfillStreamsFromCDC(cdcStreams []string) []string {
	backfillStreams := []string{}
	for _, stream := range cdcStreams {
		backfillStreams = append(backfillStreams, strings.TrimSuffix(stream, "_cdc"))
	}
	return backfillStreams
}

func (cfg *IntegrationTest) TestIntegration(t *testing.T) {
	ctx := context.Background()

	t.Logf("Root Project directory: %s", cfg.TestConfig.HostRootPath)
	t.Logf("Test data directory: %s", cfg.TestConfig.HostTestDataPath)
	currentTestTable := fmt.Sprintf("%s_test_table_olake", cfg.TestConfig.Driver)

	t.Run("Discover", func(t *testing.T) {
		req := testcontainers.ContainerRequest{
			Image: "golang:1.23.2",
			HostConfigModifier: func(hc *container.HostConfig) {
				hc.Binds = []string{
					fmt.Sprintf("%s:/test-olake:rw", cfg.TestConfig.HostRootPath),
					fmt.Sprintf("%s:/test-olake/drivers/%s/internal/testdata:rw", cfg.TestConfig.HostTestDataPath, cfg.TestConfig.Driver),
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
							// 1. Install required tools
							if code, out, err := utils.ExecCommand(ctx, c, installCmd); err != nil || code != 0 {
								return fmt.Errorf("install failed (%d): %s\n%s", code, err, out)
							}

							// 2. Query on test table
							cfg.ExecuteQuery(ctx, t, []string{currentTestTable}, "create", false)
							cfg.ExecuteQuery(ctx, t, []string{currentTestTable}, "clean", false)
							cfg.ExecuteQuery(ctx, t, []string{currentTestTable}, "add", false)

							// 3. Run discover command
							discoverCmd := discoverCommand(*cfg.TestConfig)
							if code, out, err := utils.ExecCommand(ctx, c, discoverCmd); err != nil || code != 0 {
								return fmt.Errorf("discover failed (%d): %s\n%s", code, err, string(out))
							}

							// 4. Verify streams.json file
							streamsJSON, err := os.ReadFile(cfg.TestConfig.HostTestCatalogPath)
							if err != nil {
								return fmt.Errorf("failed to read expected streams JSON: %s", err)
							}
							testStreamsJSON, err := os.ReadFile(cfg.TestConfig.HostCatalogPath)
							if err != nil {
								return fmt.Errorf("failed to read actual streams JSON: %s", err)
							}
							if !utils.NormalizedEqual(string(streamsJSON), string(testStreamsJSON)) {
								return fmt.Errorf("streams.json does not match expected test_streams.json\nExpected:\n%s\nGot:\n%s", string(streamsJSON), string(testStreamsJSON))
							}
							t.Logf("Generated streams validated with test streams")

							// 5. Clean up
							cfg.ExecuteQuery(ctx, t, []string{currentTestTable}, "drop", false)
							t.Logf("%s discover test-container clean up", cfg.TestConfig.Driver)
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
			Image: "golang:1.23.2",
			HostConfigModifier: func(hc *container.HostConfig) {
				hc.Binds = []string{
					fmt.Sprintf("%s:/test-olake:rw", cfg.TestConfig.HostRootPath),
					fmt.Sprintf("%s:/test-olake/drivers/%s/internal/testdata:rw", cfg.TestConfig.HostTestDataPath, cfg.TestConfig.Driver),
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
							// 1. Install required tools
							if code, out, err := utils.ExecCommand(ctx, c, installCmd); err != nil || code != 0 {
								return fmt.Errorf("install failed (%d): %s\n%s", code, err, out)
							}

							// 2. Query on test table
							cfg.ExecuteQuery(ctx, t, []string{currentTestTable}, "create", false)
							cfg.ExecuteQuery(ctx, t, []string{currentTestTable}, "clean", false)
							cfg.ExecuteQuery(ctx, t, []string{currentTestTable}, "add", false)

							// streamUpdateCmd := fmt.Sprintf(
							// 	`jq '(.selected_streams[][] | .normalization) = true' %s > /tmp/streams.json && mv /tmp/streams.json %s`,
							// 	cfg.TestConfig.CatalogPath, cfg.TestConfig.CatalogPath,
							// )
							streamUpdateCmd := updateStreamsCommand(*cfg.TestConfig, cfg.Namespace, []string{currentTestTable}, true)
							if code, out, err := utils.ExecCommand(ctx, c, streamUpdateCmd); err != nil || code != 0 {
								return fmt.Errorf("failed to enable normalization in streams.json (%d): %s\n%s",
									code, err, out,
								)
							}

							t.Logf("Enabled normalization in %s", cfg.TestConfig.CatalogPath)

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
								cmd := syncCommand(*cfg.TestConfig, useState)
								if useState && operation != "" {
									cfg.ExecuteQuery(ctx, t, []string{currentTestTable}, operation, false)
								}

								if code, out, err := utils.ExecCommand(ctx, c, cmd); err != nil || code != 0 {
									return fmt.Errorf("sync failed (%d): %s\n%s", code, err, out)
								}
								t.Logf("Sync successful for %s driver", cfg.TestConfig.Driver)
								VerifyIcebergSync(t, currentTestTable, cfg.DataTypeSchema, schema, opSymbol, cfg.TestConfig.Driver)
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
							cfg.ExecuteQuery(ctx, t, []string{currentTestTable}, "drop", false)
							t.Logf("%s sync test-container clean up", cfg.TestConfig.Driver)
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

func (cfg *PerformanceTest) TestPerformance(t *testing.T) {
	ctx := context.Background()

	// checks if the current rps (from stats.json) is at least 90% of the benchmark rps
	isRPSAboveBenchmark := func(config TestConfig, isBackfill bool) (bool, error) {
		// get current RPS
		var stats SyncSpeed
		if err := utils.UnmarshalFile(filepath.Join(config.HostRootPath, fmt.Sprintf("drivers/%s/internal/testdata/%s", config.Driver, "stats.json")), &stats, false); err != nil {
			return false, err
		}
		rps, err := typeutils.ReformatFloat64(strings.Split(stats.Speed, " ")[0])
		if err != nil {
			return false, fmt.Errorf("failed to get RPS from stats: %s", err)
		}

		// get benchmark RPS
		benchmarkDriverStats := benchmarks[constants.DriverType(config.Driver)]
		benchmarkRPS := utils.Ternary(isBackfill, benchmarkDriverStats.Backfill, benchmarkDriverStats.CDC).(float64)

		t.Logf("CurrentRPS: %.2f, BenchmarkRPS: %.2f", rps, benchmarkRPS)
		if rps < BenchmarkThreshold*benchmarkRPS {
			return false, fmt.Errorf("❌ RPS is less than benchmark RPS")
		}
		return true, nil
	}

	syncWithTimeout := func(ctx context.Context, c testcontainers.Container, cmd string) ([]byte, error) {
		timedCtx, cancel := context.WithTimeout(ctx, SyncTimeout)
		defer cancel()
		code, output, err := utils.ExecCommand(timedCtx, c, cmd)
		// check if sync was canceled due to timeout (expected)
		if timedCtx.Err() == context.DeadlineExceeded {
			return output, nil
		}
		if err != nil || code != 0 {
			return output, fmt.Errorf("sync failed: %s", err)
		}
		return output, nil
	}

	t.Run("performance", func(t *testing.T) {
		req := testcontainers.ContainerRequest{
			Image: "golang:1.23.2",
			HostConfigModifier: func(hc *container.HostConfig) {
				hc.Binds = []string{
					fmt.Sprintf("%s:/test-olake:rw", cfg.TestConfig.HostRootPath),
				}
				hc.ExtraHosts = append(hc.ExtraHosts, "host.docker.internal:host-gateway")
				hc.NetworkMode = "host"
			},
			ConfigModifier: func(c *container.Config) {
				c.WorkingDir = "/test-olake"
			},
			Env: map[string]string{
				"TELEMETRY_DISABLED": "true",
			},
			LifecycleHooks: []testcontainers.ContainerLifecycleHooks{
				{
					PostReadies: []testcontainers.ContainerHook{
						func(ctx context.Context, c testcontainers.Container) error {
							if code, output, err := utils.ExecCommand(ctx, c, installCmd); err != nil || code != 0 {
								return fmt.Errorf("failed to install dependencies:\n%s", string(output))
							}

							t.Logf("(backfill) running performance test for %s", cfg.TestConfig.Driver)

							t.Log("(backfill) discover started")
							discoverCmd := discoverCommand(*cfg.TestConfig)
							if code, output, err := utils.ExecCommand(ctx, c, discoverCmd); err != nil || code != 0 {
								return fmt.Errorf("failed to perform discover:\n%s", string(output))
							}
							t.Log("(backfill) discover completed")

							updateStreamsCmd := updateStreamsCommand(*cfg.TestConfig, cfg.Namespace, cfg.BackfillStreams, true)
							if code, _, err := utils.ExecCommand(ctx, c, updateStreamsCmd); err != nil || code != 0 {
								return fmt.Errorf("failed to update streams: %s", err)
							}

							t.Log("(backfill) sync started")
							usePreChunkedState := cfg.TestConfig.Driver == string(constants.MySQL)
							syncCmd := syncCommand(*cfg.TestConfig, usePreChunkedState)
							if output, err := syncWithTimeout(ctx, c, syncCmd); err != nil {
								return fmt.Errorf("failed to perform sync:\n%s", string(output))
							}
							t.Log("(backfill) sync completed")

							checkRPS, err := isRPSAboveBenchmark(*cfg.TestConfig, true)
							if err != nil {
								return fmt.Errorf("failed to check RPS: %s", err)
							}
							require.True(t, checkRPS, fmt.Sprintf("%s backfill performance below benchmark", cfg.TestConfig.Driver))
							t.Logf("✅ SUCCESS: %s backfill", cfg.TestConfig.Driver)

							if len(cfg.CDCStreams) > 0 {
								t.Logf("(cdc) running performance test for %s", cfg.TestConfig.Driver)

								t.Log("(cdc) setup cdc started")
								cfg.ExecuteQuery(ctx, t, cfg.CDCStreams, "setup_cdc", true)
								t.Log("(cdc) setup cdc completed")

								t.Log("(cdc) discover started")
								discoverCmd := discoverCommand(*cfg.TestConfig)
								if code, output, err := utils.ExecCommand(ctx, c, discoverCmd); err != nil || code != 0 {
									return fmt.Errorf("failed to perform discover:\n%s", string(output))
								}
								t.Log("(cdc) discover completed")

								updateStreamsCmd := updateStreamsCommand(*cfg.TestConfig, cfg.Namespace, cfg.CDCStreams, false)
								if code, _, err := utils.ExecCommand(ctx, c, updateStreamsCmd); err != nil || code != 0 {
									return fmt.Errorf("failed to update streams: %s", err)
								}

								t.Log("(cdc) state creation started")
								syncCmd := syncCommand(*cfg.TestConfig, false)
								if code, output, err := utils.ExecCommand(ctx, c, syncCmd); err != nil || code != 0 {
									return fmt.Errorf("failed to perform initial sync:\n%s", string(output))
								}
								t.Log("(cdc) state creation completed")

								t.Log("(cdc) trigger cdc started")
								cfg.ExecuteQuery(ctx, t, cfg.CDCStreams, "bulk_cdc_data_insert", true)
								t.Log("(cdc) trigger cdc completed")

								t.Log("(cdc) sync started")
								syncCmd = syncCommand(*cfg.TestConfig, true)
								if output, err := syncWithTimeout(ctx, c, syncCmd); err != nil {
									return fmt.Errorf("failed to perform CDC sync:\n%s", string(output))
								}
								t.Log("(cdc) sync completed")

								checkRPS, err := isRPSAboveBenchmark(*cfg.TestConfig, false)
								if err != nil {
									return fmt.Errorf("failed to check RPS: %s", err)
								}
								require.True(t, checkRPS, fmt.Sprintf("%s CDC performance below benchmark", cfg.TestConfig.Driver))
								t.Logf("✅ SUCCESS: %s cdc", cfg.TestConfig.Driver)
							}
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
		require.NoError(t, err, "performance test failed: ", err)
		defer func() {
			if err := container.Terminate(ctx); err != nil {
				t.Logf("warning: failed to terminate container: %v", err)
			}
		}()
	})
}
