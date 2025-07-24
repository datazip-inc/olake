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
	installCmd          = "apt-get update && apt-get install -y openjdk-17-jre-headless maven default-mysql-client postgresql postgresql-client iproute2 dnsutils iputils-ping netcat-openbsd nodejs npm jq && npm install -g chalk-cli"
)

type IntegrationTest struct {
	Driver             string
	ExpectedData       map[string]interface{}
	ExpectedUpdateData map[string]interface{}
	DataTypeSchema     map[string]string
	ExecuteQuery       func(ctx context.Context, t *testing.T, tableName, operation string)
}

// TestConfig is used for performance test
type TestConfig struct {
	Driver          string
	HostRoot        string
	SourcePath      string
	CatalogPath     string
	DestinationPath string
	StatePath       string
	StatsPath       string
}

type PerformanceTest struct {
	TestConfig     *TestConfig
	Namespace      string
	BackfillStream string
	CDCStream      string
	ExecuteQuery   func(ctx context.Context, t *testing.T, operation string)
	SupportsCDC    bool
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
	dummyStreamFilePath := filepath.Join(testdataDir, "test_streams.json")
	testStreamFilePath := filepath.Join(testdataDir, "streams.json")
	currentTestTable := fmt.Sprintf("%s_test_table_olake", cfg.Driver)
	var (
		sourceConfigPath      = fmt.Sprintf("/test-olake/drivers/%s/internal/testdata/source.json", cfg.Driver)
		streamsPath           = fmt.Sprintf("/test-olake/drivers/%s/internal/testdata/streams.json", cfg.Driver)
		destinationConfigPath = fmt.Sprintf("/test-olake/drivers/%s/internal/testdata/destination.json", cfg.Driver)
		statePath             = fmt.Sprintf("/test-olake/drivers/%s/internal/testdata/state.json", cfg.Driver)
	)

	t.Run("Discover", func(t *testing.T) {
		req := testcontainers.ContainerRequest{
			Image: "golang:1.23.2",
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
							// 1. Install required tools
							if code, out, err := utils.ExecCommand(ctx, c, installCmd); err != nil || code != 0 {
								return fmt.Errorf("install failed (%d): %s\n%s", code, err, out)
							}

							// 2. Query on test table
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
			Image: "golang:1.23.2",
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
							// 1. Install required tools
							if code, out, err := utils.ExecCommand(ctx, c, installCmd); err != nil || code != 0 {
								return fmt.Errorf("install failed (%d): %s\n%s", code, err, out)
							}

							// 2. Query on test table
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

								if code, out, err := utils.ExecCommand(ctx, c, cmd); err != nil || code != 0 {
									return fmt.Errorf("sync failed (%d): %s\n%s", code, err, out)
								}
								t.Logf("Sync successful for %s driver", cfg.Driver)
								VerifyIcebergSync(t, currentTestTable, cfg.DataTypeSchema, schema, opSymbol, cfg.Driver)
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
		require.Equalf(t, "1", deletedID, "Delete verification failed: expected _olake_id = '1', got %s", deletedID)
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

func GetTestConfig(driver string) *TestConfig {
	pwd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	hostRoot := filepath.Join(pwd, "../../..")

	return &TestConfig{
		Driver:          driver,
		HostRoot:        hostRoot,
		SourcePath:      fmt.Sprintf("/test-olake/drivers/%s/internal/testconfig/source.json", driver),
		CatalogPath:     fmt.Sprintf("/test-olake/drivers/%s/internal/testconfig/streams.json", driver),
		DestinationPath: fmt.Sprintf("/test-olake/drivers/%s/internal/testconfig/destination.json", driver),
		StatePath:       fmt.Sprintf("/test-olake/drivers/%s/internal/testconfig/state.json", driver),
		StatsPath:       fmt.Sprintf("/test-olake/drivers/%s/internal/testconfig/stats.json", driver),
	}
}

func (cfg *PerformanceTest) TestPerformance(t *testing.T) {
	ctx := context.Background()

	isRPSAboveBenchmark := func(config TestConfig, isBackfill bool) (bool, error) {
		benchmarkFile := utils.Ternary(isBackfill, "benchmark.json", "benchmark_cdc.json").(string)

		var stats map[string]interface{}
		if err := utils.UnmarshalFile(filepath.Join(config.HostRoot, fmt.Sprintf("drivers/%s/internal/testconfig/%s", config.Driver, "stats.json")), &stats, false); err != nil {
			return false, err
		}

		getRPSFromStats := func(stats map[string]interface{}) (float64, error) {
			rps, err := typeutils.ReformatFloat64(strings.Split(stats["Speed"].(string), " ")[0])
			if err != nil {
				return 0, err
			}
			return rps.(float64), nil
		}

		rps, err := getRPSFromStats(stats)
		if err != nil {
			return false, err
		}

		var benchmarkStats map[string]interface{}
		if err := utils.UnmarshalFile(filepath.Join(config.HostRoot, fmt.Sprintf("drivers/%s/internal/testconfig/%s", config.Driver, benchmarkFile)), &benchmarkStats, false); err != nil {
			return false, err
		}

		benchmarkRps, err := getRPSFromStats(benchmarkStats)
		if err != nil {
			return false, err
		}

		fmt.Printf("CurrentRPS: %.2f, BenchmarkRPS: %.2f\n", rps, benchmarkRps)

		if rps < 0.9*benchmarkRps {
			return false, fmt.Errorf("❌ RPS is less than benchmark RPS")
		}

		return true, nil
	}

	discoverCommand := func(config TestConfig) string {
		return fmt.Sprintf("/test-olake/build.sh driver-%s discover --config %s", config.Driver, config.SourcePath)
	}

	syncCommand := func(config TestConfig, isBackfill bool) string {
		return fmt.Sprintf("/test-olake/build.sh driver-%s sync --config %s --catalog %s --destination %s %s", config.Driver, config.SourcePath, config.CatalogPath, config.DestinationPath, utils.Ternary(isBackfill, "", fmt.Sprintf("--state %s", config.StatePath)).(string))
	}

	updateStreamsCommand := func(config TestConfig, namespace string, stream string) string {
		if len(stream) == 0 {
			return ""
		}

		condition := fmt.Sprintf(`.stream_name == "%s"`, stream)

		jqExpr := fmt.Sprintf(
			`jq '.selected_streams = { "%s": (.selected_streams["%s"] | map(select(%s) | .normalization = true)) }' %s > /tmp/streams.json && mv /tmp/streams.json %s`,
			namespace,
			namespace,
			condition,
			config.CatalogPath,
			config.CatalogPath,
		)

		return jqExpr
	}

	t.Run("performance", func(t *testing.T) {
		req := testcontainers.ContainerRequest{
			Image: "golang:1.23.2",
			HostConfigModifier: func(hc *container.HostConfig) {
				hc.Binds = []string{
					fmt.Sprintf("%s:/test-olake:rw", cfg.TestConfig.HostRoot),
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
							_, output, err := utils.ExecCommand(ctx, c, installCmd)
							require.NoError(t, err, fmt.Sprintf("Failed to install dependencies:\n%s", string(output)))

							discoverCmd := discoverCommand(*cfg.TestConfig)
							_, output, err = utils.ExecCommand(ctx, c, discoverCmd)
							require.NoError(t, err, fmt.Sprintf("Failed to perform discover:\n%s", string(output)))
							t.Log(string(output))

							updateStreamsCmd := updateStreamsCommand(*cfg.TestConfig, cfg.Namespace, cfg.BackfillStream)
							_, _, err = utils.ExecCommand(ctx, c, updateStreamsCmd)
							require.NoError(t, err, "Failed to update streams")

							syncCmd := syncCommand(*cfg.TestConfig, true)
							_, output, err = utils.ExecCommand(ctx, c, syncCmd)
							require.NoError(t, err, fmt.Sprintf("Failed to perform sync:\n%s", string(output)))
							t.Log(string(output))

							checkRPS, err := isRPSAboveBenchmark(*cfg.TestConfig, true)
							require.NoError(t, err, "Failed to check RPS", err)
							require.True(t, checkRPS, fmt.Sprintf("%s backfill performance below benchmark", cfg.TestConfig.Driver))
							t.Logf("✅ SUCCESS: %s backfill", cfg.TestConfig.Driver)

							if cfg.SupportsCDC {
								cfg.ExecuteQuery(ctx, t, "setup_cdc")

								discoverCmd := discoverCommand(*cfg.TestConfig)
								_, output, err := utils.ExecCommand(ctx, c, discoverCmd)
								require.NoError(t, err, fmt.Sprintf("Failed to perform discover:\n%s", string(output)))
								t.Log(string(output))

								updateStreamsCmd := updateStreamsCommand(*cfg.TestConfig, cfg.Namespace, cfg.CDCStream)
								_, _, err = utils.ExecCommand(ctx, c, updateStreamsCmd)
								require.NoError(t, err, "Failed to update streams")

								syncCmd := syncCommand(*cfg.TestConfig, true)
								_, output, err = utils.ExecCommand(ctx, c, syncCmd)
								require.NoError(t, err, fmt.Sprintf("Failed to perform initial sync:\n%s", string(output)))
								t.Log(string(output))

								cfg.ExecuteQuery(ctx, t, "trigger_cdc")

								syncCmd = syncCommand(*cfg.TestConfig, false)
								_, output, err = utils.ExecCommand(ctx, c, syncCmd)
								require.NoError(t, err, fmt.Sprintf("Failed to perform CDC sync:\n%s", string(output)))
								t.Log(string(output))

								checkRPS, err := isRPSAboveBenchmark(*cfg.TestConfig, false)
								require.NoError(t, err, "Failed to check RPS", err)
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
		require.NoError(t, err, "Failed to start container")
		defer func() {
			if err := container.Terminate(ctx); err != nil {
				t.Logf("warning: failed to terminate container: %v", err)
			}
		}()
	})
}
