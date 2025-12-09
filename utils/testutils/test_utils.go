package testutils

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/apache/spark-connect-go/v35/spark/sql"
	"github.com/apache/spark-connect-go/v35/spark/sql/types"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/docker/docker/api/types/container"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	// load pq driver for SQL tests
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

const (
	icebergCatalog      = "olake_iceberg"
	sparkConnectAddress = "sc://localhost:15002"
	installCmd          = "apt-get update && apt-get install -y openjdk-17-jre-headless maven default-mysql-client postgresql postgresql-client wget gnupg iproute2 dnsutils iputils-ping netcat-openbsd nodejs npm jq && wget -qO - https://www.mongodb.org/static/pgp/server-8.0.asc | gpg --dearmor -o /usr/share/keyrings/mongodb-server-8.0.gpg && echo 'deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg ] https://repo.mongodb.org/apt/debian bookworm/mongodb-org/8.0 main' | tee /etc/apt/sources.list.d/mongodb-org-8.0.list && apt-get update && apt-get install -y mongodb-mongosh && npm install -g chalk-cli"
	SyncTimeout         = 10 * time.Minute
	BenchmarkThreshold  = 0.9
	OracleTestTable     = "ORACLE_TEST_TABLE_OLAKE"
)

type IntegrationTest struct {
	TestConfig                       *TestConfig
	ExpectedData                     map[string]interface{}
	ExpectedUpdatedData              map[string]interface{}
	DestinationDataTypeSchema        map[string]string
	UpdatedDestinationDataTypeSchema map[string]string
	Namespace                        string
	ExecuteQuery                     func(ctx context.Context, t *testing.T, streams []string, operation string, fileConfig bool)
	DestinationDB                    string
	CursorField                      string
	PartitionRegex                   string
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
	Driver                 string
	HostRootPath           string
	SourcePath             string
	CatalogPath            string
	IcebergDestinationPath string
	ParquetDestinationPath string
	StatePath              string
	StatsPath              string
	HostTestDataPath       string
	HostCatalogPath        string
	HostTestCatalogPath    string
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
		Driver:                 driver,
		HostRootPath:           rootPath,
		HostTestDataPath:       fmt.Sprintf(hostTestDataPath, driver, ""),
		HostTestCatalogPath:    fmt.Sprintf(hostTestDataPath, driver, "test_streams.json"),
		HostCatalogPath:        fmt.Sprintf(hostTestDataPath, driver, "streams.json"),
		SourcePath:             fmt.Sprintf(containerTestDataPath, driver, "source.json"),
		CatalogPath:            fmt.Sprintf(containerTestDataPath, driver, "streams.json"),
		IcebergDestinationPath: fmt.Sprintf(containerTestDataPath, driver, "iceberg_destination.json"),
		ParquetDestinationPath: fmt.Sprintf(containerTestDataPath, driver, "parquet_destination.json"),
		StatePath:              fmt.Sprintf(containerTestDataPath, driver, "state.json"),
		StatsPath:              fmt.Sprintf(containerTestDataPath, driver, "stats.json"),
	}
}

func syncCommand(config TestConfig, useState bool, destinationType string, flags ...string) string {
	baseCmd := fmt.Sprintf("/test-olake/build.sh driver-%s sync --config %s --catalog %s", config.Driver, config.SourcePath, config.CatalogPath)

	switch destinationType {
	case "iceberg":
		baseCmd = fmt.Sprintf("%s --destination %s", baseCmd, config.IcebergDestinationPath)
	case "parquet":
		baseCmd = fmt.Sprintf("%s --destination %s", baseCmd, config.ParquetDestinationPath)
	}

	if useState {
		baseCmd = fmt.Sprintf("%s --state %s", baseCmd, config.StatePath)
	}

	if len(flags) > 0 {
		baseCmd = fmt.Sprintf("%s %s", baseCmd, strings.Join(flags, " "))
	}
	return baseCmd
}

// pass flags as `--flag1, flag1 value, --flag2, flag2 value...`
func discoverCommand(config TestConfig, flags ...string) string {
	baseCmd := fmt.Sprintf("/test-olake/build.sh driver-%s discover --config %s", config.Driver, config.SourcePath)
	if len(flags) > 0 {
		baseCmd = fmt.Sprintf("%s %s", baseCmd, strings.Join(flags, " "))
	}
	return baseCmd
}

// update normalization=true for selected streams under selected_streams.<namespace> by name
func updateSelectedStreamsCommand(config TestConfig, namespace, partitionRegex string, stream []string, isBackfill bool) string {
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
		`jq '.selected_streams = { "%s": (.selected_streams["%s"] | map(select(%s) | .normalization = true | .partition_regex = "%s")) }' %s > %s && mv %s %s`,
		namespace,
		namespace,
		condition,
		partitionRegex,
		config.CatalogPath,
		tmpCatalog,
		tmpCatalog,
		config.CatalogPath,
	)
	return jqExpr
}

// set sync_mode and cursor_field for a specific stream object in streams[] by namespace+name
func updateStreamConfigCommand(config TestConfig, namespace, streamName, syncMode, cursorField string) string {
	tmpCatalog := fmt.Sprintf("/tmp/%s_set_mode_streams.json", config.Driver)
	// map/select pattern updates nested array members
	return fmt.Sprintf(
		`jq --arg ns "%s" --arg name "%s" --arg mode "%s" --arg cursor "%s" '.streams = (.streams | map(if .stream.namespace == $ns and .stream.name == $name then (.stream.sync_mode = $mode | .stream.cursor_field = $cursor) else . end))' %s > %s && mv %s %s`,
		namespace, streamName, syncMode, cursorField,
		config.CatalogPath, tmpCatalog, tmpCatalog, config.CatalogPath,
	)
}

// reset state file so incremental can perform initial load (equivalent to full load on first run)
func resetStateFileCommand(config TestConfig) string {
	// Ensure the state is clean irrespective of previous CDC run
	return fmt.Sprintf(`rm -f %s; echo '{}' > %s`, config.StatePath, config.StatePath)
}

// to get backfill streams from cdc streams e.g. "demo_cdc" -> "demo"
func GetBackfillStreamsFromCDC(cdcStreams []string) []string {
	backfillStreams := []string{}
	for _, stream := range cdcStreams {
		backfillStreams = append(backfillStreams, strings.TrimSuffix(stream, "_cdc"))
	}
	return backfillStreams
}

// reset table and add back data to the table
func (cfg *IntegrationTest) resetTable(ctx context.Context, t *testing.T, testTable string) error {
	cfg.ExecuteQuery(ctx, t, []string{testTable}, "drop", false)
	cfg.ExecuteQuery(ctx, t, []string{testTable}, "create", false)
	cfg.ExecuteQuery(ctx, t, []string{testTable}, "add", false)
	return nil
}

// DeleteParquetFiles deletes only .parquet files directly in the table folder in MinIO
func DeleteParquetFiles(t *testing.T, parquetDB, tableName string) error {
	t.Helper()
	bucketName := "warehouse"
	parquetPath := fmt.Sprintf("%s/%s/", parquetDB, tableName)

	t.Logf("Cleaning up .parquet files in: s3a://%s/%s", bucketName, parquetPath)

	minioClient, err := minio.New("localhost:9000", &minio.Options{
		Creds:  credentials.NewStaticV4("admin", "password", ""),
		Secure: false,
	})
	if err != nil {
		return fmt.Errorf("failed to create MinIO client: %w", err)
	}

	ctx := context.Background()

	objectsCh := minioClient.ListObjects(ctx, bucketName, minio.ListObjectsOptions{
		Prefix:    parquetPath,
		Recursive: false,
	})

	deletedCount := 0

	for object := range objectsCh {
		if object.Err != nil {
			return fmt.Errorf("error listing objects: %w", object.Err)
		}

		if strings.HasSuffix(object.Key, ".parquet") {
			fileName := strings.TrimPrefix(object.Key, parquetPath)
			t.Logf("Deleting: %s", fileName)

			err := minioClient.RemoveObject(ctx, bucketName, object.Key, minio.RemoveObjectOptions{})
			if err != nil {
				return fmt.Errorf("failed to delete %s: %w", object.Key, err)
			}
			deletedCount++
		}
	}

	t.Logf("--- Cleanup Complete: Deleted %d files ---", deletedCount)
	return nil
}

// syncTestCase represents a test case for sync operations
type syncTestCase struct {
	name      string
	operation string
	useState  bool
	opSymbol  string
	expected  map[string]interface{}
}

// runSyncAndVerify executes a sync command and verifies the results in Iceberg
func (cfg *IntegrationTest) runSyncAndVerify(
	ctx context.Context,
	t *testing.T,
	c testcontainers.Container,
	testTable string,
	useState bool,
	destinationType string,
	operation string,
	opSymbol string,
	schema map[string]interface{},
) error {
	destDBPrefix := fmt.Sprintf("integration_%s", cfg.TestConfig.Driver)
	cmd := syncCommand(*cfg.TestConfig, useState, destinationType, "--destination-database-prefix", destDBPrefix)

	// Execute operation before sync if needed
	if useState && operation != "" {
		cfg.ExecuteQuery(ctx, t, []string{testTable}, operation, false)
	}

	// Run sync command
	code, out, err := utils.ExecCommand(ctx, c, cmd)
	if err != nil || code != 0 {
		return fmt.Errorf("sync failed (%d): %s\n%s", code, err, out)
	}

	t.Logf("Sync successful for %s driver", cfg.TestConfig.Driver)

	// Use evolved schema only for CDC "update" operation (where schema evolution is expected)
	// Incremental "insert" uses opSymbol "u" but doesn't have schema evolution
	evolvedSchema := operation == "update"

	switch destinationType {
	case "iceberg":
		{
			if evolvedSchema {
				VerifyIcebergSync(t, strings.ToLower(testTable), cfg.DestinationDB, cfg.UpdatedDestinationDataTypeSchema, schema, opSymbol, cfg.PartitionRegex, cfg.TestConfig.Driver)
			} else {
				VerifyIcebergSync(t, strings.ToLower(testTable), cfg.DestinationDB, cfg.DestinationDataTypeSchema, schema, opSymbol, cfg.PartitionRegex, cfg.TestConfig.Driver)
			}
		}
	case "parquet":
		{
			if evolvedSchema {
				VerifyParquetSync(t, strings.ToLower(testTable), cfg.DestinationDB, cfg.UpdatedDestinationDataTypeSchema, schema, opSymbol, cfg.TestConfig.Driver)
			} else {
				VerifyParquetSync(t, strings.ToLower(testTable), cfg.DestinationDB, cfg.DestinationDataTypeSchema, schema, opSymbol, cfg.TestConfig.Driver)
			}
		}
	}

	return nil
}

// testIcebergFullLoadAndCDC tests Full load and CDC operations
func (cfg *IntegrationTest) testIcebergFullLoadAndCDC(
	ctx context.Context,
	t *testing.T,
	c testcontainers.Container,
	testTable string,
) error {
	t.Log("Starting Phase A: Full load + CDC tests")

	testCases := []syncTestCase{
		{
			name:      "Full-Refresh",
			operation: "",
			useState:  false,
			opSymbol:  "r",
			expected:  cfg.ExpectedData,
		},
		{
			name:      "CDC - insert",
			operation: "insert",
			useState:  true,
			opSymbol:  "c",
			expected:  cfg.ExpectedData,
		},
		{
			name:      "CDC - update",
			operation: "update",
			useState:  true,
			opSymbol:  "u",
			expected:  cfg.ExpectedUpdatedData,
		},
		{
			name:      "CDC - delete",
			operation: "delete",
			useState:  true,
			opSymbol:  "d",
			expected:  nil,
		},
	}

	// Run each test case
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// schema evolution
			if tc.operation == "update" {
				if cfg.TestConfig.Driver != "mongodb" {
					cfg.ExecuteQuery(ctx, t, []string{testTable}, "evolve-schema", false)
				}
			}

			if err := cfg.runSyncAndVerify(
				ctx,
				t,
				c,
				testTable,
				tc.useState,
				"iceberg",
				tc.operation,
				tc.opSymbol,
				tc.expected,
			); err != nil {
				t.Fatalf("%s test failed: %v", tc.name, err)
			}
		})
	}

	t.Log("Phase A: Full load + CDC tests completed successfully")

	// Drop the Iceberg table after all tests are finished
	dropIcebergTable(t, testTable, cfg.DestinationDB)
	t.Logf("Dropped Iceberg table: %s", testTable)

	return nil
}

// testIcebergFullLoadAndCDC tests Full load and CDC operations
func (cfg *IntegrationTest) testParquetFullLoadAndCDC(
	ctx context.Context,
	t *testing.T,
	c testcontainers.Container,
	testTable string,
) error {
	t.Log("Starting Phase B: Parquet Full load + CDC tests")

	if err := cfg.resetTable(ctx, t, testTable); err != nil {
		return fmt.Errorf("failed to reset table: %w", err)
	}

	testCases := []syncTestCase{
		{
			name:      "Full-Refresh",
			operation: "",
			useState:  false,
			opSymbol:  "r",
			expected:  cfg.ExpectedData,
		},
		{
			name:      "CDC - insert",
			operation: "insert",
			useState:  true,
			opSymbol:  "c",
			expected:  cfg.ExpectedData,
		},
		{
			name:      "CDC - update",
			operation: "update",
			useState:  true,
			opSymbol:  "u",
			expected:  cfg.ExpectedUpdatedData,
		},
		{
			name:      "CDC - delete",
			operation: "delete",
			useState:  true,
			opSymbol:  "d",
			expected:  nil,
		},
	}

	// Run each test case
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// schema evolution
			if tc.operation == "update" {
				if cfg.TestConfig.Driver != "mongodb" {
					cfg.ExecuteQuery(ctx, t, []string{testTable}, "evolve-schema", false)
				}
			}

			// Delete parquet files before next operation to avoid error due to schema changes
			if err := DeleteParquetFiles(t, cfg.DestinationDB, testTable); err != nil {
				t.Fatalf("Failed to delete parquet files before %s: %v", tc.name, err)
			}

			if err := cfg.runSyncAndVerify(
				ctx,
				t,
				c,
				testTable,
				tc.useState,
				"parquet",
				tc.operation,
				tc.opSymbol,
				tc.expected,
			); err != nil {
				t.Fatalf("%s test failed: %v", tc.name, err)
			}
		})
	}

	t.Log("Phase B: Parquet Full load + CDC tests completed successfully")
	return nil
}

// TODO: add incremntal test for string time, timestamp with timezone, datetime, float, int as cursor field
// testIcebergFullLoadAndIncremental tests Full load and Incremental operations
func (cfg *IntegrationTest) testIcebergFullLoadAndIncremental(
	ctx context.Context,
	t *testing.T,
	c testcontainers.Container,
	testTable string,
) error {
	t.Log("Starting Phase C: Full load + Incremental tests")

	if err := cfg.resetTable(ctx, t, testTable); err != nil {
		return fmt.Errorf("failed to reset table: %w", err)
	}

	// Patch streams.json: set sync_mode = incremental, cursor_field = "id"
	incPatch := updateStreamConfigCommand(*cfg.TestConfig, cfg.Namespace, testTable, "incremental", cfg.CursorField)
	code, out, err := utils.ExecCommand(ctx, c, incPatch)
	if err != nil || code != 0 {
		return fmt.Errorf("failed to patch streams.json for incremental (%d): %s\n%s", code, err, out)
	}

	// Reset state so initial incremental behaves like a first full incremental load
	resetState := resetStateFileCommand(*cfg.TestConfig)
	code, out, err = utils.ExecCommand(ctx, c, resetState)
	if err != nil || code != 0 {
		return fmt.Errorf("failed to reset state for incremental (%d): %s\n%s", code, err, out)
	}

	// Test cases for incremental sync
	incrementalTestCases := []syncTestCase{
		{
			name:      "Full-Refresh",
			operation: "",
			useState:  false,
			opSymbol:  "r",
			expected:  cfg.ExpectedData,
		},
		{
			name:      "Incremental",
			operation: "update",
			useState:  true,
			opSymbol:  "u",
			expected:  cfg.ExpectedUpdatedData,
		},
	}

	// Run each incremental test case
	for _, tc := range incrementalTestCases {
		t.Run(tc.name, func(t *testing.T) {
			if err := cfg.runSyncAndVerify(
				ctx,
				t,
				c,
				testTable,
				tc.useState,
				"iceberg",
				tc.operation,
				tc.opSymbol,
				tc.expected,
			); err != nil {
				t.Fatalf("Incremental test %s failed: %v", tc.name, err)
			}
		})
	}

	t.Log("Phase C: Full load + Incremental tests completed successfully")
	return nil
}

// testParquetFullLoadAndIncremental tests Full load and Incremental operations for Parquet
func (cfg *IntegrationTest) testParquetFullLoadAndIncremental(
	ctx context.Context,
	t *testing.T,
	c testcontainers.Container,
	testTable string,
) error {
	t.Log("Starting Phase D: Parquet Full load + Incremental tests")

	if err := cfg.resetTable(ctx, t, testTable); err != nil {
		return fmt.Errorf("failed to reset table: %w", err)
	}

	// Patch streams.json: set sync_mode = incremental, cursor_field = "id"
	incPatch := updateStreamConfigCommand(*cfg.TestConfig, cfg.Namespace, testTable, "incremental", cfg.CursorField)
	code, out, err := utils.ExecCommand(ctx, c, incPatch)
	if err != nil || code != 0 {
		return fmt.Errorf("failed to patch streams.json for incremental (%d): %s\n%s", code, err, out)
	}

	// Reset state so initial incremental behaves like a first full incremental load
	resetState := resetStateFileCommand(*cfg.TestConfig)
	code, out, err = utils.ExecCommand(ctx, c, resetState)
	if err != nil || code != 0 {
		return fmt.Errorf("failed to reset state for incremental (%d): %s\n%s", code, err, out)
	}

	// Test cases for incremental sync
	incrementalTestCases := []syncTestCase{
		{
			name:      "Full-Refresh",
			operation: "",
			useState:  false,
			opSymbol:  "r",
			expected:  cfg.ExpectedData,
		},
		{
			name:      "Incremental",
			operation: "update",
			useState:  true,
			opSymbol:  "u",
			expected:  cfg.ExpectedUpdatedData,
		},
	}

	// Run each incremental test case
	for _, tc := range incrementalTestCases {
		t.Run(tc.name, func(t *testing.T) {
			// Delete parquet files before next operation to avoid error due to schema changes
			if err := DeleteParquetFiles(t, cfg.DestinationDB, testTable); err != nil {
				t.Fatalf("Failed to delete parquet files before %s: %v", tc.name, err)
			}

			if err := cfg.runSyncAndVerify(
				ctx,
				t,
				c,
				testTable,
				tc.useState,
				"parquet",
				tc.operation,
				tc.opSymbol,
				tc.expected,
			); err != nil {
				t.Fatalf("Incremental test %s failed: %v", tc.name, err)
			}
		})
	}

	t.Log("Phase D: Parquet Full load + Incremental tests completed successfully")
	return nil
}

func (cfg *IntegrationTest) TestIntegration(t *testing.T) {
	ctx := context.Background()

	t.Logf("Root Project directory: %s", cfg.TestConfig.HostRootPath)
	t.Logf("Test data directory: %s", cfg.TestConfig.HostTestDataPath)
	currentTestTable := utils.Ternary(cfg.TestConfig.Driver == string(constants.Oracle), OracleTestTable, fmt.Sprintf("%s_test_table_olake", cfg.TestConfig.Driver)).(string)

	t.Run("Discover", func(t *testing.T) {
		req := testcontainers.ContainerRequest{
			Image: "golang:1.24.0",
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
			Image: "golang:1.24.0",
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
							streamUpdateCmd := updateSelectedStreamsCommand(*cfg.TestConfig, cfg.Namespace, cfg.PartitionRegex, []string{currentTestTable}, true)
							if code, out, err := utils.ExecCommand(ctx, c, streamUpdateCmd); err != nil || code != 0 {
								return fmt.Errorf("failed to enable normalization and partition regex in streams.json (%d): %s\n%s",
									code, err, out,
								)
							}

							t.Logf("Enabled normalization and added partition regex in %s", cfg.TestConfig.CatalogPath)

							if !slices.Contains(constants.SkipCDCDrivers, constants.DriverType(cfg.TestConfig.Driver)) {
								t.Run("Iceberg Full load + CDC tests", func(t *testing.T) {
									if err := cfg.testIcebergFullLoadAndCDC(ctx, t, c, currentTestTable); err != nil {
										t.Fatalf("Iceberg Full load + CDC tests failed: %v", err)
									}
								})

								t.Run("Parquet Full load + CDC tests", func(t *testing.T) {
									if err := cfg.testParquetFullLoadAndCDC(ctx, t, c, currentTestTable); err != nil {
										t.Fatalf("Parquet Full load + CDC tests failed: %v", err)
									}
								})
							}

							t.Run("Iceberg Full load + Incremental tests", func(t *testing.T) {
								if err := cfg.testIcebergFullLoadAndIncremental(ctx, t, c, currentTestTable); err != nil {
									t.Fatalf("Iceberg Full load + Incremental tests failed: %v", err)
								}
							})

							t.Run("Parquet Full load + Incremental tests", func(t *testing.T) {
								if err := cfg.testParquetFullLoadAndIncremental(ctx, t, c, currentTestTable); err != nil {
									t.Fatalf("Parquet Full load + Incremental tests failed: %v", err)
								}
							})

							// 5. Clean up
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

// dropIcebergTable drops an Iceberg table using Spark SQL
func dropIcebergTable(t *testing.T, tableName, icebergDB string) {
	t.Helper()
	ctx := context.Background()
	spark, err := sql.NewSessionBuilder().Remote(sparkConnectAddress).Build(ctx)
	if err != nil {
		t.Logf("Failed to connect to Spark Connect server for dropping table: %v", err)
		return
	}
	defer func() {
		if stopErr := spark.Stop(); stopErr != nil {
			t.Logf("Failed to stop Spark session: %v", stopErr)
		}
	}()

	fullTableName := fmt.Sprintf("%s.%s.%s", icebergCatalog, icebergDB, tableName)
	dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS %s", fullTableName)
	t.Logf("Dropping Iceberg table: %s", dropQuery)

	_, err = spark.Sql(ctx, dropQuery)
	if err != nil {
		t.Logf("Failed to drop Iceberg table %s: %v", fullTableName, err)
		return
	}
	t.Logf("Successfully dropped Iceberg table: %s", fullTableName)
}

// TODO: Refactor parsing logic into a reusable utility functions
// verifyIcebergSync verifies that data was correctly synchronized to Iceberg
func VerifyIcebergSync(t *testing.T, tableName, icebergDB string, datatypeSchema map[string]string, schema map[string]interface{}, opSymbol, partitionRegex, driver string) {
	t.Helper()
	ctx := context.Background()
	spark, err := sql.NewSessionBuilder().Remote(sparkConnectAddress).Build(ctx)
	require.NoError(t, err, "Failed to connect to Spark Connect server")
	defer func() {
		if stopErr := spark.Stop(); stopErr != nil {
			t.Errorf("Failed to stop Spark session: %v", stopErr)
		}
	}()

	fullTableName := fmt.Sprintf("%s.%s.%s", icebergCatalog, icebergDB, tableName)
	selectQuery := fmt.Sprintf(
		"SELECT * FROM %s WHERE _op_type = '%s'",
		fullTableName, opSymbol,
	)
	t.Logf("Executing query: %s", selectQuery)

	var selectRows []types.Row
	var queryErr error
	maxRetries := 5
	retryDelay := 2 * time.Second

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(retryDelay)
		}
		var selectQueryDf sql.DataFrame
		// This is to check if the table exists in destination, as race condition might cause table to not be created yet
		selectQueryDf, queryErr = spark.Sql(ctx, selectQuery)
		if queryErr != nil {
			t.Logf("Query attempt %d failed: %v", attempt+1, queryErr)
			continue
		}

		// To ensure stale data is not being used for verification
		selectRows, queryErr = selectQueryDf.Collect(ctx)
		if queryErr != nil {
			t.Logf("Query attempt %d failed (Collect error): %v", attempt+1, queryErr)
			continue
		}
		if len(selectRows) > 0 {
			queryErr = nil
			break
		}

		// for every type of operation, op symbol will be different, using that to ensure data is not stale
		queryErr = fmt.Errorf("stale data: query succeeded but returned 0 rows for _op_type = '%s'", opSymbol)
		t.Logf("Query attempt %d/%d failed: %v", attempt+1, maxRetries, queryErr)
	}

	require.NoError(t, queryErr, "Failed to collect data rows from Iceberg after %d attempts: %v", maxRetries, queryErr)
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
			require.Equal(t, expected, icebergValue, "Row %d: mismatch on %q: Iceberg has %#v, expected %#v", rowIdx, key, icebergValue, expected)
		}
	}
	t.Logf("Verified Iceberg synced data with respect to data synced from source[%s] found equal", driver)

	describeQuery := fmt.Sprintf("DESCRIBE TABLE %s", fullTableName)
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
			t.Errorf("No mapping defined for driver type %s (column %s)", dbType, col)
		}
		require.Equal(t, expectedIceType, iceType,
			"Data type mismatch for column %s: expected %s, got %s", col, expectedIceType, iceType)
	}
	t.Logf("Verified datatypes in Iceberg after sync")

	// Partition verification using only metadata tables
	if partitionRegex == "" {
		t.Log("No partitionRegex provided, skipping partition verification")
		return
	}
	// Extract partition columns from describe rows
	partitionCols := extractFirstPartitionColFromRows(describeRows)
	require.NotEmpty(t, partitionCols, "Partition columns not found in Iceberg metadata")

	// Parse expected partition columns from pattern like "/{col,identity}"
	// Supports multiple entries like "/{col1,identity}" by taking the first token as the source column
	clean := strings.TrimPrefix(partitionRegex, "/{")
	clean = strings.TrimSuffix(clean, "}")
	toks := strings.Split(clean, ",")
	expectedCol := strings.TrimSpace(toks[0])
	require.Equal(t, expectedCol, partitionCols, "Partition column does not match expected '%s'", expectedCol)
	t.Logf("Verified partition column: %s", expectedCol)
}

// VerifyParquetSync verifies that data was correctly synchronized to Parquet files in MinIO
func VerifyParquetSync(t *testing.T, tableName, parquetDB string, datatypeSchema map[string]string, schema map[string]interface{}, opSymbol, driver string) {
	t.Helper()
	ctx := context.Background()
	spark, err := sql.NewSessionBuilder().Remote(sparkConnectAddress).Build(ctx)
	require.NoError(t, err, "Failed to connect to Spark Connect server")
	defer func() {
		if stopErr := spark.Stop(); stopErr != nil {
			t.Errorf("Failed to stop Spark session: %v", stopErr)
		}
	}()

	parquetPath := fmt.Sprintf("s3a://warehouse/%s/%s", parquetDB, tableName)
	viewName := fmt.Sprintf("`%s_view_%d`", tableName, time.Now().UnixNano())

	// create a temporary view for parquet files, allows to run describe query
	createViewQuery := fmt.Sprintf(
		"CREATE OR REPLACE TEMP VIEW %s AS SELECT * FROM parquet.`%s/*.parquet`",
		viewName, parquetPath,
	)
	_, err = spark.Sql(ctx, createViewQuery)
	require.NoError(t, err, "Failed to create temporary view for Parquet files")

	defer func() {
		dropViewQuery := fmt.Sprintf("DROP VIEW IF EXISTS %s", viewName)
		t.Logf("Dropping temporary view: %s", dropViewQuery)
		_, _ = spark.Sql(ctx, dropViewQuery)
	}()

	selectQuery := fmt.Sprintf(
		"SELECT * FROM %s WHERE `_op_type` = '%s'",
		viewName, opSymbol,
	)
	t.Logf("Executing Parquet query: %s", selectQuery)

	df, err := spark.Sql(ctx, selectQuery)
	require.NoError(t, err, "Failed to run select query on Parquet files")

	rows, err := df.Collect(ctx)
	require.NoError(t, err, "Failed to collect rows from Parquet query")
	require.NotEmpty(t, rows, "No rows returned for _op_type = '%s'", opSymbol)

	if opSymbol == "d" {
		deletedID := rows[0].Value("_olake_id")
		require.NotEmpty(t, deletedID, "Delete verification failed: _olake_id should not be empty")
		return
	}

	for rowIdx, row := range rows {
		parquetMap := make(map[string]interface{}, len(schema)+1)
		for _, col := range row.FieldNames() {
			parquetMap[col] = row.Value(col)
		}
		for key, expected := range schema {
			val, ok := parquetMap[key]
			require.Truef(t, ok, "Row %d: missing column %q in Parquet result", rowIdx, key)
			require.Equal(t, expected, val,
				"Row %d: mismatch on %q: Parquet has %#v, expected %#v", rowIdx, key, val, expected)
		}
	}
	t.Logf("Verified Parquet synced data with respect to data synced from source[%s] found equal", driver)

	describeQuery := fmt.Sprintf("DESCRIBE TABLE %s", viewName)
	descDF, err := spark.Sql(ctx, describeQuery)
	require.NoError(t, err, "Failed to describe Parquet view")

	descRows, err := descDF.Collect(ctx)
	require.NoError(t, err, "Failed to collect schema info from Parquet view")

	parquetSchema := make(map[string]string)
	for _, row := range descRows {
		colName := row.Value("col_name").(string)
		dataType := row.Value("data_type").(string)
		if !strings.HasPrefix(colName, "#") {
			parquetSchema[colName] = dataType
		}
	}

	for col, dbType := range datatypeSchema {
		pqType, found := parquetSchema[col]
		require.True(t, found, "Column %s not found in Parquet schema", col)

		expectedType, mapped := GlobalTypeMapping[dbType]
		if !mapped {
			t.Errorf("No mapping defined for driver type %s (column %s)", dbType, col)
		}
		require.Equal(t, expectedType, pqType,
			"Data type mismatch for column %s: expected %s, got %s", col, expectedType, pqType)
	}
	t.Logf("Verified datatypes in Parquet after sync")
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
			killCmd := "pkill -9 -f 'olake.*sync' || true"
			_, _, _ = utils.ExecCommand(ctx, c, killCmd)
			return output, nil
		}
		if err != nil || code != 0 {
			return output, fmt.Errorf("sync failed: %s", err)
		}
		return output, nil
	}

	t.Run("performance", func(t *testing.T) {
		req := testcontainers.ContainerRequest{
			Image: "golang:1.24.0",
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

							destDBPrefix := fmt.Sprintf("performance_%s", cfg.TestConfig.Driver)

							t.Log("(backfill) discover started")
							discoverCmd := discoverCommand(*cfg.TestConfig, "--destination-database-prefix", destDBPrefix)
							if code, output, err := utils.ExecCommand(ctx, c, discoverCmd); err != nil || code != 0 {
								return fmt.Errorf("failed to perform discover:\n%s", string(output))
							}
							t.Log("(backfill) discover completed")

							updateStreamsCmd := updateSelectedStreamsCommand(*cfg.TestConfig, cfg.Namespace, "", cfg.BackfillStreams, true)
							if code, _, err := utils.ExecCommand(ctx, c, updateStreamsCmd); err != nil || code != 0 {
								return fmt.Errorf("failed to update streams: %s", err)
							}

							t.Log("(backfill) sync started")
							usePreChunkedState := cfg.TestConfig.Driver == string(constants.MySQL)
							syncCmd := syncCommand(*cfg.TestConfig, usePreChunkedState, "iceberg", "--destination-database-prefix", destDBPrefix)
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
								discoverCmd := discoverCommand(*cfg.TestConfig, "--destination-database-prefix", destDBPrefix)
								if code, output, err := utils.ExecCommand(ctx, c, discoverCmd); err != nil || code != 0 {
									return fmt.Errorf("failed to perform discover:\n%s", string(output))
								}
								t.Log("(cdc) discover completed")

								updateStreamsCmd := updateSelectedStreamsCommand(*cfg.TestConfig, cfg.Namespace, "", cfg.CDCStreams, false)
								if code, _, err := utils.ExecCommand(ctx, c, updateStreamsCmd); err != nil || code != 0 {
									return fmt.Errorf("failed to update streams: %s", err)
								}

								t.Log("(cdc) state creation started")
								syncCmd := syncCommand(*cfg.TestConfig, false, "iceberg", "--destination-database-prefix", destDBPrefix)
								if code, output, err := utils.ExecCommand(ctx, c, syncCmd); err != nil || code != 0 {
									return fmt.Errorf("failed to perform initial sync:\n%s", string(output))
								}
								t.Log("(cdc) state creation completed")

								t.Log("(cdc) trigger cdc started")
								cfg.ExecuteQuery(ctx, t, cfg.CDCStreams, "bulk_cdc_data_insert", true)
								t.Log("(cdc) trigger cdc completed")

								t.Log("(cdc) sync started")
								syncCmd = syncCommand(*cfg.TestConfig, true, "iceberg", "--destination-database-prefix", destDBPrefix)
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

// extractFirstPartitionColFromRows extracts the first partition column from DESCRIBE EXTENDED rows
func extractFirstPartitionColFromRows(rows []types.Row) string {
	inPartitionSection := false

	for _, row := range rows {
		// Convert []any -> []string
		vals := row.Values()
		parts := make([]string, len(vals))
		for i, v := range vals {
			if v == nil {
				parts[i] = ""
			} else {
				parts[i] = fmt.Sprint(v) // safe string conversion
			}
		}
		line := strings.TrimSpace(strings.Join(parts, " "))
		if line == "" {
			continue
		}

		if strings.HasPrefix(line, "# Partition Information") {
			inPartitionSection = true
			continue
		}

		if inPartitionSection {
			if strings.HasPrefix(line, "# col_name") {
				continue
			}

			if strings.HasPrefix(line, "#") {
				break
			}

			fields := strings.Fields(line)
			if len(fields) > 0 {
				return fields[0] // return the first partition col
			}
		}
	}

	return ""
}
