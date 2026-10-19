package testutils

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/datazip-inc/olake/tests/testutils/constants"
)

const (
	SyncTimeout = 10 * time.Minute

	KeepTestDataEnvVar = "OLAKE_TEST_KEEP_DATA"
)

// ExecuteQueryFn drives the driver's source: the suites call it with an operation name, and the
// driver's own implementation knows what that means for its source.
type ExecuteQueryFn func(ctx context.Context, t *testing.T, cfg *TestConfig, operation string)

// TestConfig holds the configuration for a single test suite run per driver
type TestConfig struct {
	Driver     string
	DataFormat string

	// Suite is the unique identifier for the test suite running.
	// This is used to isolate test data and resources for concurrent test runs.
	Suite string

	// ImagePlatform overrides the platform the driver image runs under, for images that exist
	// only for amd64 and run emulated elsewhere.
	ImagePlatform string

	// DriverVersion identifies the driver revision to run the test against.
	// It can be a released version or a commit ID. Defaults to the local codebase.
	DriverVersion string

	// OlakeRootPath is the repo the tests run from, the directory `make docker.<driver>.build`
	// runs in and the committed fixtures are read from. Resolved by setupWorkingDir.
	OlakeRootPath string

	// TestWorkingDir is this config's private /tmp working dir, the folder where we run olake
	// commands and expect the generated files. Every file the suite reads or writes lives in it and
	// is addressed by name through GetFilePath, so there is no path to keep a field for.
	TestWorkingDir string

	// SeedExcludedColumns names the columns this suite must leave out of its seed data entirely --
	// columns the binary under test cannot sync at any price. The backward-compatibility runner
	// fills it in per baseline from its rules; every other suite leaves it empty, so a driver's
	// ExecuteQuery reads it and seeds everything by default.
	SeedExcludedColumns []string `json:"-"`

	// SourceBaseConfig is the working copy of source.json, parsed: the suite's own credentials,
	// database and prefixes, after isolateSuiteConfigs renamed what it isolates. ExecuteQuery connects with
	// it, so the harness and olake always drive the same source.
	SourceBaseConfig SourceConfig `json:"-"`

	// DestinationDB is the namespace this suite's syncs write to, read out of the catalog once
	// IsolateDestinationDB has suffixed it with the suite.
	DestinationDB string

	SkipSchemaEvolution bool

	// Driver shape: the same for every suite this driver runs, so it is declared once here
	// rather than per test.
	Namespace       string
	ExecuteQuery    ExecuteQueryFn `json:"-"`
	CursorField     string
	PartitionRegex  string
	FilterConfig    string
	ColumnToExclude string
	PrimaryKey      string
}

type TestConfigOption func(*TestConfig)

// NewTestConfig builds a driver's config from what a suite cannot derive for itself: the source it
// reads and the namespace it drives. dataFormat names the driver's testdata subdirectory, for the
// drivers that have one.
func NewTestConfig(t *testing.T, driver constants.DriverType, namespace string, executeQuery ExecuteQueryFn, opts ...TestConfigOption) (*TestConfig, error) {
	t.Helper()
	cfg := &TestConfig{
		Driver:       string(driver),
		Namespace:    namespace,
		ExecuteQuery: executeQuery,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	err := cfg.setup(t)
	if err != nil {
		return nil, err
	}

	return cfg, nil
}

func WithDriverVersion(version string) TestConfigOption {
	return func(c *TestConfig) {
		c.DriverVersion = version
	}
}

func WithImagePlatform(platform string) TestConfigOption {
	return func(c *TestConfig) {
		c.ImagePlatform = platform
	}
}

func WithDataFormat(dataFormat string) TestConfigOption {
	return func(c *TestConfig) {
		c.DataFormat = dataFormat
	}
}

func (c *TestConfig) generateSuiteName(t *testing.T) {
	t.Helper()
	nonSuiteChars := regexp.MustCompile(`[^a-z0-9]+`)
	suite := strings.ToLower(t.Name())
	suite = strings.TrimPrefix(suite, "test")
	suite = strings.TrimPrefix(suite, string(c.Driver))
	c.Suite = strings.Trim(nonSuiteChars.ReplaceAllString(suite, "_"), "_")
}

// setup initializes the derived fields of the TestConfig and does the steup for configuring the test isolation like isolated configs
func (c *TestConfig) setup(t *testing.T) error {
	t.Helper()

	c.generateSuiteName(t)
	c.addTimingLogsMiddleware()

	if err := c.setupWorkingDir(t); err != nil {
		return err
	}
	if err := c.pullOrBuildDriverImage(t); err != nil {
		return err
	}
	if err := c.isolateSuiteConfigs(); err != nil {
		return err
	}

	sourceConfig, err := ReadSourceConfig(c.GetFilePath("source.json"))
	if err != nil {
		return fmt.Errorf("failed to read the source config of driver %q suite %q: %s", c.Driver, c.Suite, err)
	}
	c.SourceBaseConfig = sourceConfig
	t.Logf("test config: %s", c)

	return nil
}

func (c *TestConfig) String() string {
	config, _ := json.MarshalIndent(c, "", "  ")
	return string(config)
}

// pullOrBuildDriverImage just sets the driver image in case  builds the driver image against current codebase
func (c *TestConfig) pullOrBuildDriverImage(t *testing.T) (err error) {
	if c.DriverVersion == "" {
		c.DriverVersion = GetCurrentDriverVersion()
	}
	return c.resolveImage(t)
}

// resolveImage turns a version string into an image ref present on the local daemon:
//
//	"local"                            -> built from current code
//	"latest", "v0.6.5"                 -> olakego/source-<driver>:<spec>, pulled
//	"9f3c1ab", "sha:9f3c1ab"           -> built from a detached worktree at that commit
func (c *TestConfig) resolveImage(t *testing.T) error {
	if c.DriverVersion == defaultDriverVersion {
		return buildDriverImage(t, c)
	}
	// A commit id is abbreviated on the way in, and that short form becomes the image tag.
	if commitID, ok := ResolveToCommit(c.OlakeRootPath, c.DriverVersion); ok {
		c.DriverVersion = commitID
		return buildImageFromCommit(t, c, commitID)
	}

	return ensureImagePresent(t, c.GetDriverImage(), c.ImagePlatform)
}

func (c *TestConfig) addTimingLogsMiddleware() {
	executeFunc := c.ExecuteQuery
	c.ExecuteQuery = func(ctx context.Context, t *testing.T, cfg *TestConfig, operation string) {
		defer TrackPhaseTiming(t, c.Driver, fmt.Sprintf("query %q", operation))()
		executeFunc(ctx, t, cfg, operation)
	}
}

// SetExecuteQuery replaces the config's query function, timed like the one NewTestConfig was
// given, for a suite that can only build its function once the config exists.
func (c *TestConfig) SetExecuteQuery(executeQuery ExecuteQueryFn) {
	c.ExecuteQuery = executeQuery
	c.addTimingLogsMiddleware()
}

// UniqueID identifies this run among every suite that can be running beside it: the driver and the
// suite itself. Everything a suite must not share is named after it.
func (c *TestConfig) UniqueID() string {
	return Combine(c.withSuite(c.Driver))
}

func (c *TestConfig) withSuite(base string) string {
	return Combine(base, c.Suite)
}

// TestTableName is the source table a suite drives. The suite suffix is what keeps concurrent
// suites off each other's table -- without it they race the same DROP/CREATE.
func (c *TestConfig) GetTableName() string {
	return Combine("test_table_olake", c.Suite)
}

func (c *TestConfig) GetDriverImage() string {
	return fmt.Sprintf("olakego/source-%s:%s", c.Driver, c.DriverVersion)
}

// GetFilePath addresses a file in the suite's working directory by name -- the configs, the
// catalog, state and stats all live there, and the container reads them under the same names.
func (c *TestConfig) GetFilePath(fileName string) string {
	return filepath.Join(c.TestWorkingDir, fileName)
}

// GetFixturePath addresses a committed fixture in the driver's testdata directory, for the one
// thing a run must outlive its working directory: the benchmark history the perf suite appends to.
// Everything else a suite reads is the working copy setupWorkingDir made, via GetFilePath.
func (c *TestConfig) GetFixturePath(fileName string, dataFormat ...string) string {
	return filepath.Join(c.OlakeRootPath, "tests", c.Driver, "testdata", filepath.Join(dataFormat...), fileName)
}

// setupWorkingDir gives the suite a private working directory holding its own copy of every config
// the driver container reads, so the repo fixtures stay read-only and concurrent suites never share
// a writable file. The shared fixtures land first and the driver's own overwrite them by name, so a
// driver overrides a common config just by committing a file of the same name.
func (c *TestConfig) setupWorkingDir(t *testing.T) (err error) {
	c.TestWorkingDir = t.TempDir()

	c.OlakeRootPath, err = RepoRoot()
	if err != nil {
		return fmt.Errorf("failed to determine the repo root; the tests run from a git checkout: %s", err)
	}

	// static committed files(called fixtures) are copied into the working dir.
	// It contains the source and destination configs, the catalog and the initial state
	commonFixturesDir := filepath.Join(c.OlakeRootPath, "tests/testdata")
	driverFixturesDir := filepath.Join(c.OlakeRootPath, "tests", c.Driver, "testdata", c.DataFormat)
	for _, fixtures := range []string{commonFixturesDir, driverFixturesDir} {
		if err := CopyDirFiles(fixtures, c.TestWorkingDir); err != nil {
			return fmt.Errorf("failed to copy the fixtures of %s into %s: %s", fixtures, c.TestWorkingDir, err)
		}
	}
	return nil
}

// isolateSuiteConfigs derives every config the driver container reads from its committed base, so the base
// files stay untouched, and retargets the copies at the names this suite owns.
func (c *TestConfig) isolateSuiteConfigs() error {
	enableArrowWrites := func(destinationConf map[string]interface{}) error {
		writer, ok := destinationConf["writer"].(map[string]interface{})
		if !ok {
			return fmt.Errorf("no writer object in iceberg_destination.json")
		}
		writer["arrow_writes"] = true

		return nil
	}

	err := CopyJSONWithEdit(c.GetFilePath("iceberg_destination.json"), c.GetFilePath("iceberg_destination_arrow.json"), enableArrowWrites)
	if err != nil {
		return fmt.Errorf("failed to derive the arrow destination config of driver %q suite %q: %s", c.Driver, c.Suite, err)
	}

	if err := c.getOrRenderConfig("source.template.json", "source.json"); err != nil {
		return fmt.Errorf("failed to isolate the source config of driver %q for suite %q: %s", c.Driver, c.Suite, err)
	}

	if err := c.RenderConfig("streams.template.json", "streams.json"); err != nil {
		return fmt.Errorf("failed to retarget the catalog of driver %q at suite %q table %s: %s", c.Driver, c.Suite, c.GetTableName(), err)
	}
	return nil
}

// getOrRenderConfig checks if we need to get the configFile provided by the caller or render it from committed template.
// NOTE: Currently all the tests use templates except PerformanceTests which provides the configs directly.
func (c *TestConfig) getOrRenderConfig(template, configPath string) error {
	_, err := os.Stat(c.GetFilePath(configPath))
	if errors.Is(err, os.ErrNotExist) {
		return c.RenderConfig(template, configPath)
	} else if err != nil {
		return err
	}

	return nil
}

// RenderConfig expands the placeholders of the committed template and writes the result to outputFile
func (c *TestConfig) RenderConfig(template, outputFile string) error {
	raw, err := os.ReadFile(c.GetFilePath(template))
	if err != nil {
		return fmt.Errorf("failed to read %s: %s", template, err)
	}
	expanded, err := c.expandPlaceholders(raw)
	if err != nil {
		return fmt.Errorf("failed to expand %s: %s", template, err)
	}
	return WriteHostFile(c.GetFilePath(outputFile), expanded)
}

// placeholder matches the ${name} form alone: the source configs carry credentials, and a secret
// holding a bare $ has to survive rendering untouched.
var placeholder = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// expandPlaceholders substitutes the ${suite} a committed config spells its per-suite names with --
// ${SUITE} for the drivers whose identifiers are uppercase -- so the file reads as what it renders.
func (c *TestConfig) expandPlaceholders(raw []byte) ([]byte, error) {
	var unknown []string
	expanded := placeholder.ReplaceAllFunc(raw, func(match []byte) []byte {
		switch name := string(placeholder.FindSubmatch(match)[1]); name {
		case "suite":
			return []byte(c.Suite)
		case "SUITE":
			return []byte(strings.ToUpper(c.Suite))
		default:
			unknown = append(unknown, name)
			return match
		}
	})
	if len(unknown) > 0 {
		return nil, fmt.Errorf("undefined placeholder(s) %s: a config can only carry ${suite} and ${SUITE}", strings.Join(unknown, ", "))
	}
	return expanded, nil
}

// The helpers below edit the driver's config/catalog files on the host; the container sees the
// changes through the /testdata mount.

// EditJSONFile reads path, applies edit to the decoded document, and writes it back.
func EditJSONFile(path string, edit func(doc map[string]interface{}) error) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read %s: %s", path, err)
	}
	doc, err := ParseJSONDoc(raw)
	if err != nil {
		return fmt.Errorf("failed to parse %s: %s", path, err)
	}
	if err := edit(doc); err != nil {
		return err
	}
	out, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal %s: %s", path, err)
	}
	return WriteHostFile(path, out)
}

// WriteHostFile writes to the shared /testdata mount, unlinking first. DockerRunArgs runs the
// container as the test user, but as root when it cannot look the user up. On Linux the test user
// can replace a root-owned file a previous run left behind, but cannot truncate it.
func WriteHostFile(path string, data []byte) error {
	_ = os.Remove(path)
	return os.WriteFile(path, data, 0600)
}

// NormalizeStreamName uppercases the stream name for drivers whose catalogs store
// uppercase identifiers (e.g. Oracle).
func NormalizeStreamName(driver, streamName string) string {
	return Ternary(slices.Contains(constants.UppercaseStreamDrivers, constants.DriverType(driver)), strings.ToUpper(streamName), streamName).(string)
}

// IsolateDestinationDB suffixes every stream's destination_database with the suite and records it as
// DestinationDB: the driver writes a baked destination_database verbatim, ignoring the prefix flag.
func (c *TestConfig) IsolateDestinationDB() error {
	return EditJSONFile(c.GetFilePath("streams.json"), func(doc map[string]interface{}) error {
		entries, _ := doc["streams"].([]interface{})
		for _, entry := range entries {
			wrapper, ok := entry.(map[string]interface{})
			if !ok {
				continue
			}
			stream, ok := wrapper["stream"].(map[string]interface{})
			if !ok {
				continue
			}
			destinationDB, ok := stream["destination_database"].(string)
			if !ok || destinationDB == "" {
				continue
			}
			destinationDB = c.withSuite(destinationDB)
			stream["destination_database"] = destinationDB
			c.DestinationDB = strings.ReplaceAll(destinationDB, ":", "_")
		}
		return nil
	})
}

// UpdateSelectedStreams rewrites selected_streams so only the given streams stay selected, with
// normalization enabled and the partition regex, filter config and excluded columns applied.
func UpdateSelectedStreams(config *TestConfig, namespace, partitionRegex, filterConfig string, streams []string, excludedColumns []string) error {
	if len(streams) == 0 {
		return nil
	}
	selectedNames := make(map[string]bool, len(streams))
	for _, s := range streams {
		selectedNames[NormalizeStreamName(config.Driver, s)] = true
	}

	var filter interface{} = map[string]interface{}{}
	if filterConfig != "" {
		if err := json.Unmarshal([]byte(filterConfig), &filter); err != nil {
			return fmt.Errorf("failed to parse filter config: %s", err)
		}
	}

	return EditJSONFile(config.GetFilePath("streams.json"), func(doc map[string]interface{}) error {
		selected, _ := doc["selected_streams"].(map[string]interface{})
		nsStreams, _ := selected[namespace].([]interface{})
		kept := make([]interface{}, 0, len(nsStreams))
		for _, raw := range nsStreams {
			stream, ok := raw.(map[string]interface{})
			if !ok || !selectedNames[fmt.Sprint(stream["stream_name"])] {
				continue
			}
			delete(selectedNames, fmt.Sprint(stream["stream_name"]))
			stream["normalization"] = true
			stream["partition_regex"] = partitionRegex
			stream["filter_config"] = filter
			for _, excluded := range excludedColumns {
				if excluded == "" {
					continue
				}
				selectedColumns, ok := stream["selected_columns"].(map[string]interface{})
				if !ok {
					continue
				}
				columns, ok := selectedColumns["columns"].([]interface{})
				if !ok {
					continue
				}
				remaining := make([]interface{}, 0, len(columns))
				for _, col := range columns {
					if fmt.Sprint(col) != excluded {
						remaining = append(remaining, col)
					}
				}
				selectedColumns["columns"] = remaining
			}
			kept = append(kept, stream)
		}
		if len(selectedNames) > 0 {
			return fmt.Errorf("streams %v not found in namespace %q", slices.Sorted(maps.Keys(selectedNames)), namespace)
		}
		doc["selected_streams"] = map[string]interface{}{namespace: kept}
		return nil
	})
}

func CopyFile(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return fmt.Errorf("failed to read %s: %s", src, err)
	}
	return WriteHostFile(dst, data)
}

// SaveStateFile copies state.json to the checkpoint state file.
func SaveStateFile(config *TestConfig) error {
	return CopyFile(config.GetFilePath("state.json"), config.GetFilePath("state_checkpoint.json"))
}

// RestoreStateFile replaces state.json with the previously saved checkpoint backup.
func RestoreStateFile(config *TestConfig) error {
	return CopyFile(config.GetFilePath("state_checkpoint.json"), config.GetFilePath("state.json"))
}

// syncTestCase represents a test case for sync operations
// RenderOlakeFailure formats a failed sync's exit, translating the one code worth translating: 137 is
// SIGKILL, which in this harness almost always means the --memory cap or the docker VM OOM-killed
func RenderOlakeFailure(code int, err error, out []byte) error {
	hint := ""
	if code == 137 {
		hint = " [exit 137 = SIGKILL: the container was OOM-killed -- see the --memory cap and the docker VM's total memory]"
	}
	if err != nil {
		return fmt.Errorf("sync failed (%d)%s: %s\n%s", code, hint, err, out)
	}
	return fmt.Errorf("sync failed (%d)%s\n%s", code, hint, out)
}

// runSyncAndVerify executes a sync command and verifies the results in Iceberg
func (cfg *IntegrationTest) runSyncAndVerify(
	ctx context.Context,
	t *testing.T,
	testTable string,
	useState bool,
	destinationType string,
	operation string,
	opSymbol string,
	schema map[string]interface{},
	isCDC bool,
) error {
	destDBPrefix := destinationDBPrefix(cfg.TestConfig)
	cmd := syncArgs(*cfg.TestConfig, useState, destinationType, "--destination-database-prefix", destDBPrefix)

	// Execute operation before sync if needed
	if useState && operation != "" {
		cfg.ExecuteQuery(ctx, t, cfg.TestConfig, operation)
		// SQL Server CDC is asynchronous: the capture job only picks up the DML above on its next
		// transaction-log scan, and the sync's change window ends at the job's processed max LSN
		// (sys.fn_cdc_get_max_lsn), so syncing too early would see no changes. Wait for the capture
		// job to advance past the DML. Incremental runs read the table directly and need no wait.
		if isCDC && cfg.TestConfig.Driver == "mssql" {
			cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "wait-cdc-catchup")
		}
	}

	// Run sync against the driver image
	code, out, err := runOlake(ctx, t, cfg.TestConfig, cmd...)
	if err != nil || code != 0 {
		return fmt.Errorf("sync failed (%d): %s\n%s", code, err, out)
	}

	t.Logf("Sync successful for %s driver", cfg.TestConfig.Driver)

	// Use evolved schema only for CDC "update" operation (where schema evolution is expected)
	// Incremental "insert" uses opSymbol "u" but doesn't have schema evolution
	evolvedSchema := operation == "update" || operation == "upsert_update" || operation == "upsert_column_update" || operation == "upsert_update_repartition" || operation == "upsert_update_partition_1"

	// Verification reads the destination back through Spark Connect (with retries), a real slice of
	// sync wall-clock; time it as its own phase.
	defer trackPhaseTiming(t, cfg.TestConfig.Driver, destinationType+" verify")()

	switch destinationType {
	case "iceberg":
		{
			if evolvedSchema {
				VerifyIcebergSync(t, testTable, cfg.DestinationDB, cfg.UpdatedDestinationDataTypeSchema, cfg.DefaultCDCColumnsSchema, schema, opSymbol, cfg.PartitionRegex, cfg.TestConfig.Driver, isCDC, cfg.ColumnToExclude)
			} else {
				VerifyIcebergSync(t, testTable, cfg.DestinationDB, cfg.DestinationDataTypeSchema, cfg.DefaultCDCColumnsSchema, schema, opSymbol, cfg.PartitionRegex, cfg.TestConfig.Driver, isCDC, cfg.ColumnToExclude)
			}
		}
	case "parquet":
		{
			if evolvedSchema {
				VerifyParquetSync(t, testTable, cfg.DestinationDB, cfg.UpdatedDestinationDataTypeSchema, cfg.DefaultCDCColumnsSchema, schema, opSymbol, cfg.TestConfig.Driver, isCDC, cfg.ColumnToExclude)
			} else {
				VerifyParquetSync(t, testTable, cfg.DestinationDB, cfg.DestinationDataTypeSchema, cfg.DefaultCDCColumnsSchema, schema, opSymbol, cfg.TestConfig.Driver, isCDC, cfg.ColumnToExclude)
			}
		}
	}

	return nil
}

func (cfg *IntegrationTest) testIcebergWriter(
	ctx context.Context,
	t *testing.T,
	testTable string,
	useArrowWriter bool,
	testFunc func(context.Context, *testing.T, string) error,
) error {
	// Writer variants are separate config files, so no suite ever edits one in place.
	file := "iceberg_destination.json"
	if useArrowWriter {
		file = "iceberg_destination_arrow.json"
	}
	cfg.TestConfig.IcebergDestinationPath = path.Join(containerTestDataDir, file)

	return testFunc(ctx, t, testTable)
}

// testIcebergFullLoadAndCDC tests Full load and CDC operations
func (cfg *IntegrationTest) testIcebergFullLoadAndCDC(
	ctx context.Context,
	t *testing.T,
	testTable string,
) error {
	t.Log("Starting Iceberg Full load + CDC tests")

	if err := cfg.resetTable(ctx, t); err != nil {
		return fmt.Errorf("failed to reset table: %w", err)
	}

	dbTestCases := []syncTestCase{
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

	kafkaTestCases := []syncTestCase{
		{
			name:      "CDC - strict - insert",
			operation: "",
			useState:  false,
			opSymbol:  "c",
			expected:  cfg.ExpectedData,
		},
		{
			name:      "CDC - strict - update",
			operation: "update",
			useState:  true,
			opSymbol:  "c",
			expected:  cfg.ExpectedUpdatedData,
		},
	}

	testCases := Ternary(cfg.TestConfig.Driver == string(constants.Kafka), kafkaTestCases, dbTestCases).([]syncTestCase)
	if len(cfg.DedupKeys) > 0 {
		updateOp := "upsert_update"
		if cfg.UpsertUpdateOp != "" {
			updateOp = cfg.UpsertUpdateOp
		}
		testCases = []syncTestCase{
			{
				name:      "CDC - strict - insert",
				operation: "",
				useState:  false,
				opSymbol:  "c",
				expected:  cfg.ExpectedData,
			},
			{
				name:      "CDC - strict - update",
				operation: updateOp,
				useState:  true,
				opSymbol:  "u",
				expected:  cfg.ExpectedUpdatedData,
			},
		}
		if !cfg.SkipTombstone {
			testCases = append(testCases, syncTestCase{
				name:      "CDC - strict - delete",
				operation: "upsert_tombstone",
				useState:  true,
				opSymbol:  "d",
				expected:  nil,
			})
		}
	}

	// Run each test case
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// schema evolution
			if tc.operation == "update" {
				if cfg.TestConfig.Driver != "mongodb" && cfg.TestConfig.Driver != "mssql" && cfg.TestConfig.Driver != "kafka" {
					cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "evolve-schema")
				}
			}

			if err := cfg.runSyncAndVerify(
				ctx,
				t,
				testTable,
				tc.useState,
				"iceberg",
				tc.operation,
				tc.opSymbol,
				tc.expected,
				tc.name != "Full-Refresh",
			); err != nil {
				t.Fatalf("%s test failed: %v", tc.name, err)
			}
		})
	}

	t.Log("Iceberg Full load + CDC tests completed successfully")

	// Drop the Iceberg table after all tests are finished
	dropIcebergTable(t, testTable, cfg.DestinationDB)
	t.Logf("Dropped Iceberg table: %s", testTable)

	return nil
}

// testIcebergFullLoadAndCDC tests Full load and CDC operations
func (cfg *IntegrationTest) testParquetFullLoadAndCDC(
	ctx context.Context,
	t *testing.T,
	testTable string,
) error {
	t.Log("Starting Parquet Full load + CDC tests")

	if err := cfg.resetTable(ctx, t); err != nil {
		return fmt.Errorf("failed to reset table: %s", err)
	}
	if err := deleteParquetTable(t, cfg.DestinationDB, testTable); err != nil {
		return fmt.Errorf("failed to reset parquet table: %s", err)
	}

	dbTestCases := []syncTestCase{
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

	kafkaTestCases := []syncTestCase{
		{
			name:      "CDC - strict - insert",
			operation: "",
			useState:  false,
			opSymbol:  "c",
			expected:  cfg.ExpectedData,
		},
		{
			name:      "CDC - strict - update",
			operation: "update",
			useState:  true,
			opSymbol:  "c",
			expected:  cfg.ExpectedUpdatedData,
		},
	}

	testCases := Ternary(cfg.TestConfig.Driver == string(constants.Kafka), kafkaTestCases, dbTestCases).([]syncTestCase)
	if len(cfg.DedupKeys) > 0 {
		updateOp := "upsert_update"
		if cfg.UpsertUpdateOp != "" {
			updateOp = cfg.UpsertUpdateOp
		}
		testCases = []syncTestCase{
			{
				name:      "CDC - strict - insert",
				operation: "",
				useState:  false,
				opSymbol:  "u",
				expected:  cfg.ExpectedData,
			},
			{
				name:      "CDC - strict - update",
				operation: updateOp,
				useState:  true,
				opSymbol:  "u",
				expected:  cfg.ExpectedUpdatedData,
			},
		}
		if !cfg.SkipTombstone {
			testCases = append(testCases, syncTestCase{
				name:      "CDC - strict - delete",
				operation: "upsert_tombstone",
				useState:  true,
				opSymbol:  "d",
				expected:  nil,
			})
		}
	}

	// Run each test case
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// schema evolution
			if tc.operation == "update" {
				if cfg.TestConfig.Driver != "mongodb" && cfg.TestConfig.Driver != "mssql" && cfg.TestConfig.Driver != "kafka" {
					cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "evolve-schema")
				}
			}

			// Delete parquet files before next operation to avoid error due to schema changes
			if err := DeleteParquetFiles(t, cfg.DestinationDB, testTable); err != nil {
				t.Fatalf("Failed to delete parquet files before %s: %v", tc.name, err)
			}

			if err := cfg.runSyncAndVerify(
				ctx,
				t,
				testTable,
				tc.useState,
				"parquet",
				tc.operation,
				tc.opSymbol,
				tc.expected,
				tc.name != "Full-Refresh",
			); err != nil {
				t.Fatalf("%s test failed: %v", tc.name, err)
			}
		})
	}

	t.Log("Parquet Full load + CDC tests completed successfully")
	return nil
}

// TODO: add incremntal test for string time, timestamp with timezone, datetime, float, int as cursor field
// testIcebergFullLoadAndIncremental tests Full load and Incremental operations
func (cfg *IntegrationTest) testIcebergFullLoadAndIncremental(
	ctx context.Context,
	t *testing.T,
	testTable string,
) error {
	t.Log("Starting Iceberg Full load + Incremental tests")

	if err := cfg.resetTable(ctx, t); err != nil {
		return fmt.Errorf("failed to reset table: %s", err)
	}

	// Patch streams.json: set sync_mode = incremental, cursor_field = "id"
	if err := updateStreamConfig(cfg.TestConfig, cfg.Namespace, testTable, "incremental", cfg.CursorField); err != nil {
		return fmt.Errorf("failed to patch streams.json for incremental: %s", err)
	}

	// Reset state so initial incremental behaves like a first full incremental load
	if err := resetStateFile(cfg.TestConfig); err != nil {
		return fmt.Errorf("failed to reset state for incremental: %s", err)
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
			name:      "Incremental - insert",
			operation: "insert",
			useState:  true,
			opSymbol:  "u",
			expected:  cfg.ExpectedData,
		},
		{
			name:      "Incremental - update",
			operation: "update",
			useState:  true,
			opSymbol:  "u",
			expected:  cfg.ExpectedUpdatedData,
		},
	}

	// Run each incremental test case
	for _, tc := range incrementalTestCases {
		t.Run(tc.name, func(t *testing.T) {
			// schema evolution
			if tc.operation == "update" {
				if cfg.TestConfig.Driver != string(constants.MongoDB) && cfg.TestConfig.Driver != "mssql" {
					cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "evolve-schema")
				}
			}

			// drop iceberg table before sync
			dropIcebergTable(t, testTable, cfg.DestinationDB)
			t.Logf("Dropped Iceberg table: %s", testTable)

			if err := cfg.runSyncAndVerify(
				ctx,
				t,
				testTable,
				tc.useState,
				"iceberg",
				tc.operation,
				tc.opSymbol,
				tc.expected,
				false,
			); err != nil {
				t.Fatalf("Incremental test %s failed: %v", tc.name, err)
			}
		})
	}

	t.Log("Iceberg Full load + Incremental tests completed successfully")

	// Drop the Iceberg table after all tests are finished, so the incremental
	// cursor state left in the table's olake_2pc property is not read back as
	// CDC state by a later run.
	dropIcebergTable(t, testTable, cfg.DestinationDB)
	t.Logf("Dropped Iceberg table: %s", testTable)

	return nil
}

// testParquetFullLoadAndIncremental tests Full load and Incremental operations for Parquet
func (cfg *IntegrationTest) testParquetFullLoadAndIncremental(
	ctx context.Context,
	t *testing.T,
	testTable string,
) error {
	t.Log("Starting Parquet Full load + Incremental tests")

	if err := cfg.resetTable(ctx, t); err != nil {
		return fmt.Errorf("failed to reset table: %s", err)
	}
	if err := deleteParquetTable(t, cfg.DestinationDB, testTable); err != nil {
		return fmt.Errorf("failed to reset parquet table: %s", err)
	}

	// Patch streams.json: set sync_mode = incremental, cursor_field = "id"
	if err := updateStreamConfig(cfg.TestConfig, cfg.Namespace, testTable, "incremental", cfg.CursorField); err != nil {
		return fmt.Errorf("failed to patch streams.json for incremental: %s", err)
	}

	// Reset state so initial incremental behaves like a first full incremental load
	if err := resetStateFile(cfg.TestConfig); err != nil {
		return fmt.Errorf("failed to reset state for incremental: %s", err)
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
			name:      "Incremental - insert",
			operation: "insert",
			useState:  true,
			opSymbol:  "u",
			expected:  cfg.ExpectedData,
		},
		{
			name:      "Incremental - update",
			operation: "update",
			useState:  true,
			opSymbol:  "u",
			expected:  cfg.ExpectedUpdatedData,
		},
	}

	// Run each incremental test case
	for _, tc := range incrementalTestCases {
		t.Run(tc.name, func(t *testing.T) {
			// schema evolution
			if tc.operation == "update" {
				if cfg.TestConfig.Driver != string(constants.MongoDB) && cfg.TestConfig.Driver != "mssql" {
					cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "evolve-schema")
				}
			}

			// Delete parquet files before next operation to avoid error due to schema changes
			if err := DeleteParquetFiles(t, cfg.DestinationDB, testTable); err != nil {
				t.Fatalf("Failed to delete parquet files before %s: %v", tc.name, err)
			}

			if err := cfg.runSyncAndVerify(
				ctx,
				t,
				testTable,
				tc.useState,
				"parquet",
				tc.operation,
				tc.opSymbol,
				tc.expected,
				false,
			); err != nil {
				t.Fatalf("Incremental test %s failed: %v", tc.name, err)
			}
		})
	}

	t.Log("Parquet Full load + Incremental tests completed successfully")
	return nil
}

// testIceberg2PCCDCRecovery tests 2PC (Two-Phase Commit) failure recovery for CDC mode using
// the Iceberg destination. It simulates a state-save failure mid-sync: saves a pre-insert
// checkpoint, performs a CDC insert, then restores to the checkpoint and inserts a second
// record (insert_2pc) to verify the driver correctly recovers without duplicating rows.
func (cfg *IntegrationTest) testIceberg2PCCDCRecovery(
	ctx context.Context,
	t *testing.T,
	testTable string,
) error {
	t.Log("Starting Iceberg 2PC CDC Recovery tests")

	if err := cfg.resetTable(ctx, t); err != nil {
		return fmt.Errorf("failed to reset table: %w", err)
	}

	// Drop the Iceberg table and reset state before the first sync, so stale rows and the
	// olake_2pc table property left by a previous run can't leak into this run's recovery timeline.
	dropIcebergTable(t, testTable, cfg.DestinationDB)
	if err := resetStateFile(cfg.TestConfig); err != nil {
		return fmt.Errorf("failed to reset state: %w", err)
	}

	twoPCCDCTestCases := []syncTestCase{
		{
			name:                     Ternary(cfg.TestConfig.Driver == string(constants.Kafka), "CDC - initial load", "Full-Refresh").(string),
			operation:                "",
			useState:                 false,
			opSymbol:                 Ternary(cfg.TestConfig.Driver == string(constants.Kafka), "c", "r").(string),
			expected:                 cfg.ExpectedData,
			verifyNoDuplicates:       true,
			expectedRowCountByOpType: 5,
		},
		{
			name:                     "CDC - insert",
			operation:                Ternary(cfg.TestConfig.Driver == string(constants.Kafka), "add", "insert").(string),
			useState:                 true,
			opSymbol:                 "c",
			expected:                 cfg.ExpectedData,
			preSetup:                 Ternary(cfg.TestConfig.Driver == string(constants.Kafka), []func(*TestConfig) error{}, []func(*TestConfig) error{saveStateFile}).([]func(*TestConfig) error),
			verifyNoDuplicates:       cfg.TestConfig.Driver == string(constants.Kafka),
			expectedRowCountByOpType: 10,
		},
		{
			// Simulate 2PC failure: restore state to pre-insert checkpoint, insert a
			// second record, run sync. The driver recovers: it advances state to the
			// committed metadata LSN by making a bounded sync.
			// expectedRowCountByOpType=1 because no new data lands in Iceberg here,
			// as it just recovers the sync from state -> metadata LSN.
			name:                     "CDC - Recovery Sync",
			operation:                "insert_2pc",
			useState:                 true,
			opSymbol:                 "c",
			expected:                 cfg.ExpectedData,
			verifyNoDuplicates:       true,
			expectedRowCountByOpType: int64(Ternary(cfg.TestConfig.Driver == string(constants.Kafka), 11, 1).(int)),
			preSetup:                 Ternary(cfg.TestConfig.Driver == string(constants.Kafka), []func(*TestConfig) error{}, []func(*TestConfig) error{restoreStateFile}).([]func(*TestConfig) error),
		},
		{
			// After the recovery sync advanced state to the committed metadata LSN,
			// a normal sync should see both the original insert and insert_2pc rows.
			name:                     "CDC - Post Recovery Sync",
			useState:                 true,
			opSymbol:                 "c",
			expected:                 cfg.ExpectedData,
			verifyNoDuplicates:       true,
			expectedRowCountByOpType: int64(Ternary(cfg.TestConfig.Driver == string(constants.Kafka), 12, 2).(int)),
		},
	}

	for _, tc := range twoPCCDCTestCases {
		t.Run(tc.name, func(t *testing.T) {
			for _, preSetup := range tc.preSetup {
				if err := preSetup(cfg.TestConfig); err != nil {
					t.Fatalf("%s pre-sync setup failed: %v", tc.name, err)
				}
			}

			if err := cfg.runSyncAndVerify(
				ctx, t, testTable, tc.useState, "iceberg",
				tc.operation, tc.opSymbol, tc.expected,
				tc.name != "Full-Refresh",
			); err != nil {
				t.Fatalf("%s test failed: %v", tc.name, err)
			}

			if tc.verifyNoDuplicates {
				VerifyIcebergNoDuplicates(ctx, t, testTable, cfg.DestinationDB, tc.opSymbol, tc.expectedRowCountByOpType)
			}
		})
	}

	t.Log("Iceberg 2PC CDC Recovery tests completed successfully")
	dropIcebergTable(t, testTable, cfg.DestinationDB)
	t.Logf("Dropped Iceberg table after 2PC CDC tests: %s", testTable)
	return nil
}

// testIceberg2PCIncrementalRecovery tests 2PC (Two-Phase Commit) failure recovery for
// incremental mode using the Iceberg destination. It simulates a state-save failure after
// the cursor advances: saves a pre-insert checkpoint, performs an incremental insert, then
// restores to the checkpoint and inserts a second record (insert_2pc) to verify that the
// cursor re-reads the overlapping range, deduplicates the original insert via MERGE INTO,
// and correctly surfaces only the net-new insert_2pc row.
func (cfg *IntegrationTest) testIceberg2PCIncrementalRecovery(
	ctx context.Context,
	t *testing.T,
	testTable string,
) error {
	t.Log("Starting Iceberg 2PC Incremental Recovery tests")

	if err := cfg.resetTable(ctx, t); err != nil {
		return fmt.Errorf("failed to reset table: %w", err)
	}

	// Drop the Iceberg table before the first sync, so stale rows and the olake_2pc table
	// property left by a previous run can't leak into this run's recovery timeline.
	dropIcebergTable(t, testTable, cfg.DestinationDB)

	// Patch streams.json: set sync_mode = incremental, cursor_field
	if err := updateStreamConfig(cfg.TestConfig, cfg.Namespace, testTable, "incremental", cfg.CursorField); err != nil {
		return fmt.Errorf("failed to patch streams.json for incremental: %s", err)
	}

	// Reset state so initial incremental behaves like a first full incremental load
	if err := resetStateFile(cfg.TestConfig); err != nil {
		return fmt.Errorf("failed to reset state for incremental: %s", err)
	}

	twoPCIncrementalTestCases := []syncTestCase{
		{
			name:                     "Full-Refresh",
			operation:                "",
			useState:                 false,
			opSymbol:                 "r",
			expected:                 cfg.ExpectedData,
			verifyNoDuplicates:       true,
			expectedRowCountByOpType: 5,
		},
		{
			name:      "Incremental - insert",
			operation: "insert",
			useState:  true,
			opSymbol:  "u",
			expected:  cfg.ExpectedData,
			preSetup: []func(*TestConfig) error{
				saveStateFile,
			},
		},
		{
			// Simulate 2PC failure: restore cursor to pre-insert checkpoint, insert a
			// second record, run sync. The cursor re-reads the range and deduplicates
			// the original insert via MERGE INTO; insert_2pc is net-new.
			// expectedRowCountByOpType=1: only insert_2pc is visible (original deduplicated).
			name:                     "Incremental - State Save Failure Sync",
			operation:                "insert_2pc",
			useState:                 true,
			opSymbol:                 "u",
			expected:                 cfg.ExpectedData,
			verifyNoDuplicates:       true,
			expectedRowCountByOpType: 1,
			preSetup: []func(*TestConfig) error{
				restoreStateFile,
			},
		},
		{
			// After recovery, state is now consistent. A normal sync should see both
			// the original insert row and insert_2pc row — 2 distinct records total.
			name:                     "Incremental - Post Recovery Sync",
			useState:                 true,
			opSymbol:                 "u",
			expected:                 cfg.ExpectedData,
			verifyNoDuplicates:       true,
			expectedRowCountByOpType: 2, // insert row + insert_2pc row, both unique by _olake_id
		},
	}

	for _, tc := range twoPCIncrementalTestCases {
		t.Run(tc.name, func(t *testing.T) {
			for _, preSetup := range tc.preSetup {
				if err := preSetup(cfg.TestConfig); err != nil {
					t.Fatalf("%s pre-sync setup failed: %v", tc.name, err)
				}
			}

			if err := cfg.runSyncAndVerify(
				ctx, t, testTable, tc.useState, "iceberg",
				tc.operation, tc.opSymbol, tc.expected,
				false,
			); err != nil {
				t.Fatalf("Incremental 2PC test %s failed: %v", tc.name, err)
			}

			if tc.verifyNoDuplicates {
				VerifyIcebergNoDuplicates(ctx, t, testTable, cfg.DestinationDB, tc.opSymbol, tc.expectedRowCountByOpType)
			}
		})
	}

	t.Log("Iceberg 2PC Incremental Recovery tests completed successfully")
	dropIcebergTable(t, testTable, cfg.DestinationDB)
	t.Logf("Dropped Iceberg table after 2PC Incremental tests: %s", testTable)
	return nil
}

// keepTestData reports whether OLAKE_TEST_KEEP_DATA=true, the dev switch that skips the
// final source-data drop so the seeded tables/files survive the run for inspection.
// Every pre-test drop/clean still runs, so the next run starts from a clean slate
// regardless of the switch.
func keepTestData() bool {
	return os.Getenv("OLAKE_TEST_KEEP_DATA") == "true"
}

// Test2PCIntegration runs the full Two-Phase Commit (2PC) failure-recovery integration test
// suite against the driver image. It exercises CDC and incremental state-recovery scenarios
// independently of the happy-path integration tests, allowing them to be scheduled and
// reported separately.
func (cfg *IntegrationTest) Test2PCIntegration(t *testing.T) {
	cfg.IsolateSuite(t, "2pc")
	ctx := t.Context()
	cfg.ExecuteQuery = timedExecuteQuery(cfg.TestConfig.Driver, cfg.ExecuteQuery)

	t.Logf("Root Project directory: %s", cfg.TestConfig.HostRootPath)
	t.Logf("Test data directory: %s", cfg.TestConfig.HostTestDataPath)
	currentTestTable := TestTableName(cfg.TestConfig)

	// Postgres only, and for the whole suite: olake validates the CDC config on EVERY sync, so a
	// slot scoped to just the CDC tests fails the incremental ones whose config still names it.
	if cfg.TestConfig.Driver == string(constants.Postgres) {
		cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "create-slot")
		defer cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "drop-slot")
	}

	// 2PC tests don't need schema discovery — the schema is already validated by the regular integration test.
	seedCatalogFromTestStreams(t, cfg.TestConfig, currentTestTable)

	t.Run("Sync", func(t *testing.T) {
		cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "drop")
		cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "create")
		cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "clean")
		cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "add")

		if err := updateSelectedStreams(cfg.TestConfig, cfg.Namespace, cfg.PartitionRegex, cfg.FilterConfig, []string{currentTestTable}, cfg.ColumnToExclude); err != nil {
			t.Fatalf("failed to enable normalization and partition regex in streams.json: %s", err)
		}
		t.Logf("Enabled normalization and added partition regex in %s", cfg.TestConfig.HostCatalogPath)

		writerTypes := []struct {
			name     string
			useArrow bool
		}{
			{"Legacy", false},
			{"Arrow", true},
		}

		if !slices.Contains(constants.SkipCDCDrivers, constants.DriverType(cfg.TestConfig.Driver)) {
			for _, wt := range writerTypes {
				t.Run(fmt.Sprintf("Iceberg (%s) 2PC CDC Recovery tests", wt.name), func(t *testing.T) {
					if err := cfg.testIcebergWriter(ctx, t, currentTestTable, wt.useArrow, cfg.testIceberg2PCCDCRecovery); err != nil {
						t.Fatalf("Iceberg (%s) 2PC CDC Recovery tests failed: %v", wt.name, err)
					}
				})
			}
		}

		if cfg.TestConfig.Driver != string(constants.Kafka) {
			for _, wt := range writerTypes {
				t.Run(fmt.Sprintf("Iceberg (%s) 2PC Incremental Recovery tests", wt.name), func(t *testing.T) {
					if err := cfg.testIcebergWriter(ctx, t, currentTestTable, wt.useArrow, cfg.testIceberg2PCIncrementalRecovery); err != nil {
						t.Fatalf("Iceberg (%s) 2PC Incremental Recovery tests failed: %v", wt.name, err)
					}
				})
			}
		}

		if keepTestData() {
			t.Logf("keeping %s source data (OLAKE_TEST_KEEP_DATA=true)", cfg.TestConfig.Driver)
		} else {
			cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "drop")
			t.Logf("%s 2PC sync test cleanup", cfg.TestConfig.Driver)
		}
	})
}

// WaitForSyncProgress blocks until the running sync has reported its first records in stats.json.
// A driver uses it to time an event at a point where the sync is demonstrably mid-flight, rather
// than guessing with a sleep.
func WaitForSyncProgress(ctx context.Context, t *testing.T, statsPath string) {
	t.Helper()

	require.Eventually(t, func() bool {
		if ctx.Err() != nil {
			return true
		}

		var stats struct {
			SyncedRecords int64 `json:"Synced Records"`
		}
		if err := UnmarshalFile(statsPath, &stats, false); err != nil {
			return false
		}
		if stats.SyncedRecords > 0 {
			t.Logf("sync started: %d records synced", stats.SyncedRecords)
			return true
		}
		return false
	}, SyncTimeout, time.Second)
}

// runRebalanceSync runs a sync command for the rebalance test.
func (cfg *IntegrationTest) runRebalanceSync(
	ctx context.Context,
	t *testing.T,
	useState bool,
) error {
	t.Helper()

	destDBPrefix := destinationDBPrefix(cfg.TestConfig)
	cmd := syncArgs(*cfg.TestConfig, useState, "iceberg", "--destination-database-prefix", destDBPrefix)

	code, out, err := runOlake(ctx, t, cfg.TestConfig, cmd...)
	if err != nil {
		return fmt.Errorf("sync exec error: %w\n%s", err, out)
	}
	if code != 0 {
		return fmt.Errorf("sync failed (%d): %s", code, out)
	}
	t.Logf("sync completed successfully")
	return nil
}

// testKafkaRebalance exercises consumer-group rebalance recovery while syncing a large bulk of messages.
func (cfg *IntegrationTest) testKafkaRebalance(
	ctx context.Context,
	t *testing.T,
	testTable string,
) error {
	t.Log("Starting Kafka rebalance recovery test")

	dropIcebergTable(t, testTable, cfg.DestinationDB)
	if err := resetStateFile(cfg.TestConfig); err != nil {
		return fmt.Errorf("failed to reset state file: %s", err)
	}

	rebalanceTestCases := []syncTestCase{
		{
			name:      "CDC - first rebalance sync",
			operation: "insert_rebalance",
			useState:  true,
		},
		{
			// Stop the trigger consumer before resuming so it cannot hold partition assignments.
			name:      "CDC - second rebalance sync",
			operation: "stop_rebalance",
			useState:  true,
		},
	}

	for _, tc := range rebalanceTestCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg.ExecuteQuery(ctx, t, cfg.TestConfig, tc.operation)

			if err := cfg.runRebalanceSync(ctx, t, tc.useState); err != nil {
				t.Fatalf("%s failed: %v", tc.name, err)
			}
		})
	}

	VerifyIcebergNoDuplicates(ctx, t, testTable, cfg.DestinationDB, "c", kafkaRebalanceBulkMessageCount)

	t.Log("Kafka rebalance recovery test completed successfully")

	dropIcebergTable(t, testTable, cfg.DestinationDB)
	t.Logf("Dropped Iceberg table: %s", testTable)

	return nil
}

// TestRebalance runs the Kafka consumer-group rebalance recovery integration test in an isolated container.
func (cfg *IntegrationTest) TestRebalance(t *testing.T) {
	cfg.IsolateSuite(t, "rebalance")
	ctx := t.Context()

	t.Logf("Root Project directory: %s", cfg.TestConfig.HostRootPath)
	t.Logf("Test data directory: %s", cfg.TestConfig.HostTestDataPath)
	currentTestTable := TestTableName(cfg.TestConfig)

	seedCatalogFromTestStreams(t, cfg.TestConfig, currentTestTable)

	t.Run("Sync", func(t *testing.T) {
		// 1. Query on test table
		cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "create")
		cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "clean")

		// 2. Enable normalization and partition regex in streams.json
		if err := updateSelectedStreams(cfg.TestConfig, cfg.Namespace, cfg.PartitionRegex, cfg.FilterConfig, []string{currentTestTable}, cfg.ColumnToExclude); err != nil {
			t.Fatalf("failed to enable normalization and partition regex in streams.json: %s", err)
		}
		t.Logf("Enabled normalization and added partition regex in %s", cfg.TestConfig.HostCatalogPath)

		// 3. Run Kafka rebalance recovery test (legacy Iceberg writer)
		if err := cfg.testIcebergWriter(ctx, t, currentTestTable, false, cfg.testKafkaRebalance); err != nil {
			t.Fatalf("Kafka rebalance test failed: %v", err)
		}

		// 4. Clean up
		cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "drop")
		t.Logf("%s rebalance test cleanup", cfg.TestConfig.Driver)
	})
}

type Upsert struct {
	Name      string
	Operation string
	UseState  bool
	OpSymbol  string
	Expected  map[string]interface{}
}

// RunUpsertIceberg isolates a Kafka upsert stream, seeds the topic, and runs Arrow Iceberg sync+verify steps.
func (cfg *IntegrationTest) RunUpsertIceberg(t *testing.T, suite, seedOp string, steps []Upsert) {
	cfg.IsolateSuite(t, suite)
	ctx := t.Context()

	cfg.ExecuteQuery = timedExecuteQuery(cfg.TestConfig.Driver, cfg.ExecuteQuery)
	testTable := TestTableName(cfg.TestConfig)
	seedCatalogFromTestStreams(t, cfg.TestConfig, testTable)

	if err := setStreamUpsert(cfg.TestConfig, cfg.Namespace, testTable, cfg.DedupKeys); err != nil {
		t.Fatalf("failed to set stream upsert: %s", err)
	}
	if err := updateSelectedStreams(cfg.TestConfig, cfg.Namespace, cfg.PartitionRegex, cfg.FilterConfig, []string{testTable}, cfg.ColumnToExclude); err != nil {
		t.Fatalf("failed to update streams.json: %s", err)
	}

	if err := cfg.testIcebergWriter(ctx, t, testTable, true, func(ctx context.Context, t *testing.T, testTable string) error {
		dropIcebergTable(t, testTable, cfg.DestinationDB)
		cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "drop")
		cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "create")
		cfg.ExecuteQuery(ctx, t, cfg.TestConfig, seedOp)

		// Reset state file and remove table index
		if err := resetStateFile(cfg.TestConfig); err != nil {
			return err
		}
		if err := dvClearTableIndex(ctx, t, cfg); err != nil {
			return fmt.Errorf("failed to clear table index: %w", err)
		}

		for _, tc := range steps {
			if err := cfg.runSyncAndVerify(ctx, t, testTable, tc.UseState, "iceberg", tc.Operation, tc.OpSymbol, tc.Expected, true); err != nil {
				return fmt.Errorf("%s: %w", tc.Name, err)
			}
		}
		dropIcebergTable(t, testTable, cfg.DestinationDB)
		return nil
	}); err != nil {
		t.Fatalf("upsert iceberg: %v", err)
	}
}

func (cfg *IntegrationTest) RunUpsertIcebergExpectFail(t *testing.T, suite, seedOp, wantErr string) {
	cfg.IsolateSuite(t, suite)
	ctx := t.Context()

	cfg.ExecuteQuery = timedExecuteQuery(cfg.TestConfig.Driver, cfg.ExecuteQuery)
	testTable := TestTableName(cfg.TestConfig)
	seedCatalogFromTestStreams(t, cfg.TestConfig, testTable)

	if err := setStreamUpsert(cfg.TestConfig, cfg.Namespace, testTable, cfg.DedupKeys); err != nil {
		t.Fatalf("failed to set stream upsert: %s", err)
	}
	if err := updateSelectedStreams(cfg.TestConfig, cfg.Namespace, cfg.PartitionRegex, cfg.FilterConfig, []string{testTable}, cfg.ColumnToExclude); err != nil {
		t.Fatalf("failed to update streams.json: %s", err)
	}

	if err := cfg.testIcebergWriter(ctx, t, testTable, true, func(ctx context.Context, t *testing.T, testTable string) error {
		dropIcebergTable(t, testTable, cfg.DestinationDB)
		cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "drop")
		cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "create")
		cfg.ExecuteQuery(ctx, t, cfg.TestConfig, seedOp)

		// Reset state file and remove table index
		if err := resetStateFile(cfg.TestConfig); err != nil {
			return err
		}
		if err := dvClearTableIndex(ctx, t, cfg); err != nil {
			return fmt.Errorf("failed to clear table index: %w", err)
		}
		return cfg.runSyncAndVerify(ctx, t, testTable, false, "iceberg", "", "c", cfg.ExpectedData, true)
	}); err == nil {
		t.Fatalf("sync must fail (%s)", wantErr)
	} else if !strings.Contains(err.Error(), wantErr) {
		t.Fatalf("expected %q, got: %v", wantErr, err)
	}
}

// TestDiscover seeds the source with this driver's test table, runs discover against the driver
// image and asserts the catalog it writes matches test_streams.json exactly.
//
// Its caller must not be parallel. The compare is an equality one, so it only holds while this
// table is the only thing in the source -- and every other suite seeds one of its own. Leaving the
// test serial is what orders it ahead of them: Go resumes parallel tests only once the serial ones
// in the package are done.
func (cfg *IntegrationTest) TestDiscover(t *testing.T) {
	ctx := t.Context()
	cfg.ExecuteQuery = timedExecuteQuery(cfg.TestConfig.Driver, cfg.ExecuteQuery)

	t.Logf("Root Project directory: %s", cfg.TestConfig.HostRootPath)
	t.Logf("Test data directory: %s", cfg.TestConfig.HostTestDataPath)

	// 1. Empty the source, then seed just this table. drop-all is what makes the compare below an
	// equality one: discover enumerates everything, so anything an aborted run (or a perf seed)
	// left behind would show up as an extra stream. Safe only here -- the discover suite runs
	// alone, while every parallel suite owns a table drop-all would take with it.
	cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "drop-all")
	cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "create")
	cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "add")
	// Deferred, so a failed discover still hands the parallel suites behind it a clean source.
	defer func() {
		if keepTestData() {
			t.Logf("keeping %s source data (OLAKE_TEST_KEEP_DATA=true)", cfg.TestConfig.Driver)
			return
		}
		cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "drop")
	}()

	// 2. Run discover against the driver image
	code, out, err := runOlake(ctx, t, cfg.TestConfig, discoverArgs(*cfg.TestConfig)...)
	if err != nil || code != 0 {
		t.Fatalf("discover failed (%d): %s\n%s", code, err, string(out))
	}

	// 3. Verify streams.json describes exactly the streams we expect
	verifyDiscoveredStreams(t, cfg.TestConfig.HostTestCatalogPath, cfg.TestConfig.HostCatalogPath)
}

// TestSync runs the happy-path sync suite: full load, CDC and incremental, over both Iceberg writers
// and Parquet. It seeds its catalog from test_streams.json instead of discovering one, the way the
// 2PC and rebalance suites do -- TestDiscover already proves the two are identical.
func (cfg *IntegrationTest) TestSync(t *testing.T) {
	ctx := t.Context()
	cfg.ExecuteQuery = timedExecuteQuery(cfg.TestConfig.Driver, cfg.ExecuteQuery)

	t.Logf("Root Project directory: %s", cfg.TestConfig.HostRootPath)
	t.Logf("Test data directory: %s", cfg.TestConfig.HostTestDataPath)
	currentTestTable := TestTableName(cfg.TestConfig)

	seedCatalogFromTestStreams(t, cfg.TestConfig, currentTestTable)

	if len(cfg.DedupKeys) > 0 {
		if err := setStreamUpsert(cfg.TestConfig, cfg.Namespace, currentTestTable, cfg.DedupKeys); err != nil {
			t.Fatalf("failed to set stream upsert: %s", err)
		}
	}

	// 1. Query on test table; drop first so an aborted run's leftovers cannot survive
	// the CREATE IF NOT EXISTS
	cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "drop")
	cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "create")
	cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "clean")
	cfg.ExecuteQuery(ctx, t, cfg.TestConfig, Ternary(len(cfg.DedupKeys) > 0, Ternary(cfg.UpsertAddOp != "", cfg.UpsertAddOp, "upsert_add"), "add").(string))

	// 2. Enable normalization, partition regex, filter and column exclusion in streams.json
	if err := updateSelectedStreams(cfg.TestConfig, cfg.Namespace, cfg.PartitionRegex, cfg.FilterConfig, []string{currentTestTable}, cfg.ColumnToExclude); err != nil {
		t.Fatalf("failed to enable normalization and partition regex in streams.json: %s", err)
	}
	t.Logf("Enabled normalization and added partition regex in %s", cfg.TestConfig.HostCatalogPath)

	writerTypes := []struct {
		name     string
		useArrow bool
	}{
		{"Legacy", false},
		{"Arrow", true},
	}

	// Skip cdc tests for drivers not supporting cdc mode
	if !slices.Contains(constants.SkipCDCDrivers, constants.DriverType(cfg.TestConfig.Driver)) {
		for _, wt := range writerTypes {
			t.Run(fmt.Sprintf("Iceberg (%s) Full load + CDC tests", wt.name), func(t *testing.T) {
				if err := cfg.testIcebergWriter(ctx, t, currentTestTable, wt.useArrow, cfg.testIcebergFullLoadAndCDC); err != nil {
					t.Fatalf("Iceberg (%s) Full load + CDC tests failed: %v", wt.name, err)
				}
			})
		}

		t.Run("Parquet Full load + CDC tests", func(t *testing.T) {
			if err := cfg.testParquetFullLoadAndCDC(ctx, t, currentTestTable); err != nil {
				t.Fatalf("Parquet Full load + CDC tests failed: %v", err)
			}
		})

		if hasIcebergTableIndexTest(cfg.TestConfig.Driver) {
			t.Run("Iceberg Table Index Eq to Pos Conversion", func(t *testing.T) {
				if err := cfg.testIcebergEqToPosConversion(ctx, t, currentTestTable); err != nil {
					t.Fatalf("Iceberg Table Index Eq to Pos Conversion test failed: %v", err)
				}
			})
			t.Run("Iceberg Table Index Clean Table Positional", func(t *testing.T) {
				if err := cfg.testIcebergCleanTablePositionalWithPebbleIndex(ctx, t, currentTestTable); err != nil {
					t.Fatalf("Iceberg Table Index Clean Table Positional test failed: %v", err)
				}
			})
			t.Run("Iceberg Table Index Rebuild Index From Scratch", func(t *testing.T) {
				if err := cfg.testIcebergRebuildIndexFromScratch(ctx, t, currentTestTable); err != nil {
					t.Fatalf("Iceberg Table Index Rebuild Index From Scratch test failed: %v", err)
				}
			})
		}
	}

	// Skip incremental tests for drivers not supporting incremental mode
	if cfg.TestConfig.Driver != string(constants.Kafka) {
		for _, wt := range writerTypes {
			t.Run(fmt.Sprintf("Iceberg (%s) Full load + Incremental tests", wt.name), func(t *testing.T) {
				if err := cfg.testIcebergWriter(ctx, t, currentTestTable, wt.useArrow, cfg.testIcebergFullLoadAndIncremental); err != nil {
					t.Fatalf("Iceberg (%s) Full load + Incremental tests failed: %v", wt.name, err)
				}
			})
		}

		t.Run("Parquet Full load + Incremental tests", func(t *testing.T) {
			if err := cfg.testParquetFullLoadAndIncremental(ctx, t, currentTestTable); err != nil {
				t.Fatalf("Parquet Full load + Incremental tests failed: %v", err)
			}
		})
	}

	// Asserts the writer splits bulk output into size-bounded files without losing rows. Runs
	// last: it replaces the table contents and clears streams.json's regex/filter config.
	if hasParquetRollingTest(cfg.TestConfig.Driver) {
		t.Run("Parquet Rolling", func(t *testing.T) {
			if err := cfg.testParquetRolling(ctx, t, currentTestTable); err != nil {
				t.Fatalf("Parquet Rolling test failed: %v", err)
			}
		})
	}

	// 3. Clean up
	if keepTestData() {
		t.Logf("keeping %s source data (OLAKE_TEST_KEEP_DATA=true)", cfg.TestConfig.Driver)
		return
	}
	cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "drop")
	t.Logf("%s sync test cleanup", cfg.TestConfig.Driver)
}

var (
	sharedSparkOnce sync.Once
	sharedSpark     sql.SparkSession
	sharedSparkErr  error
)

// sparkSession returns the shared Spark Connect session, building it on first use and warming it
// so the one-off server bootstrap is timed here instead of inflating whichever verify runs first.
func sparkSession(ctx context.Context, t *testing.T) (sql.SparkSession, error) {
	sharedSparkOnce.Do(func() {
		// The shared session outlives whichever test builds it, so its construction must not be
		// tied to that test's context (t.Context cancels when the test ends).
		ctx := context.WithoutCancel(ctx)
		defer trackPhaseTiming(t, "spark", "session build")()
		for attempt := 1; ; attempt++ {
			sharedSpark, sharedSparkErr = sql.NewSessionBuilder().Remote(sparkConnectAddress).Build(ctx)
			if sharedSparkErr == nil || attempt == 3 {
				break
			}
			t.Logf("Attempt %d/3: Failed to connect to Spark, retrying in 2s: %v", attempt, sharedSparkErr)
			time.Sleep(2 * time.Second)
		}
		if sharedSparkErr != nil {
			return
		}
		if _, err := sharedSpark.Sql(ctx, "SELECT 1"); err != nil {
			t.Logf("Spark session warm-up query failed (non-fatal): %v", err)
		}
	})
	return sharedSpark, sharedSparkErr
}

// dropIcebergTable drops an Iceberg table using Spark SQL
func dropIcebergTable(t *testing.T, tableName, icebergDB string) {
	t.Helper()
	ctx := t.Context()
	spark, err := sparkSession(ctx, t)
	if err != nil {
		t.Logf("Failed to connect to Spark Connect server for dropping table: %v", err)
		return
	}

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
func VerifyIcebergSync(t *testing.T, tableName, icebergDB string, datatypeSchema map[string]string, defaultCDCColumnsSchema map[string]string, schema map[string]interface{}, opSymbol, partitionRegex, driver string, isCDC bool, excludedColumn string) {
	t.Helper()
	ctx := t.Context()
	spark, err := sparkSession(ctx, t)
	require.NoError(t, err, "Failed to connect to Spark Connect server")

	fullTableName := fmt.Sprintf("%s.%s.%s", icebergCatalog, icebergDB, tableName)
	// The shared session caches table snapshots, so refresh to see the rows the sync just committed.
	// Non-fatal: on a first sync the table may not exist yet, which the retry loop below handles.
	if _, refreshErr := spark.Sql(ctx, fmt.Sprintf("REFRESH TABLE %s", fullTableName)); refreshErr != nil {
		t.Logf("REFRESH TABLE before verify (non-fatal): %v", refreshErr)
	}
	selectQuery := fmt.Sprintf(
		"SELECT * FROM %s WHERE _op_type = '%s'",
		fullTableName, opSymbol,
	)
	// Kafka append keeps _op_type 'c'. Upsert writes c (first insert, Iceberg rewrite), u, or d.
	// col_included appears only on evolved/update rows.
	if driver == string(constants.Kafka) {
		if _, ok := schema["col_included"]; ok {
			selectQuery += " AND col_included IS NOT NULL"
		}
	}
	t.Logf("Executing query: %s", selectQuery)

	var selectRows []types.Row
	var queryErr error
	maxRetries := 20
	retryDelay := 5 * time.Second

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

		// For delete operations, 0 rows is acceptable - exit immediately without retrying
		if opSymbol == "d" {
			queryErr = nil
			t.Logf("Delete verification passed: found 0 rows for _op_type = 'd' (acceptable)")
			break
		}

		// for every type of operation, op symbol will be different, using that to ensure data is not stale
		queryErr = fmt.Errorf("stale data: query succeeded but returned 0 rows for _op_type = '%s'", opSymbol)
		t.Logf("Query attempt %d/%d failed: %v", attempt+1, maxRetries, queryErr)

		// Force Spark to refresh the table metadata from the Iceberg catalog.
		refreshQuery := fmt.Sprintf("REFRESH TABLE %s", fullTableName)
		if _, refreshErr := spark.Sql(ctx, refreshQuery); refreshErr != nil {
			t.Logf("REFRESH TABLE attempt %d failed (non-fatal): %v", attempt+1, refreshErr)
		}
	}

	// For delete operations, accept both 0 and 1 row (both are valid outcomes)
	if opSymbol == "d" {
		if len(selectRows) > 0 {
			deletedID := selectRows[0].Value("_olake_id")
			require.NotEmpty(t, deletedID, "Delete verification failed: _olake_id should not be empty")
		}
		t.Logf("Delete verification passed: found %d row(s) for _op_type = 'd'", len(selectRows))
		return
	}
	require.NoError(t, queryErr, "Failed to collect data rows from Iceberg after %d attempts: %v", maxRetries, queryErr)
	require.NotEmpty(t, selectRows, "No rows returned for _op_type = '%s'", opSymbol)

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
		if isCDC {
			for key := range defaultCDCColumnsSchema {
				icebergValue, ok := icebergMap[key]
				require.Truef(t, ok, "Row %d: missing column %q in Iceberg result", rowIdx, key)
				// Kafka offset, partition can be 0, NotEmpty fails for 0 so we check for NotNil instead.
				if key == "_kafka_offset" || key == "_kafka_partition" {
					require.NotNil(t, icebergValue, "Row %d: expected column %q to be non-empty, got %#v", rowIdx, key, icebergValue)
				} else {
					require.NotEmpty(t, icebergValue, "Row %d: expected column %q to be non-empty, got %#v", rowIdx, key, icebergValue)
				}
				if key == constants.CdcTimestamp {
					ts, ok := normalizeToTime(icebergValue)
					require.Truef(t, ok, "Row %d: expected %q to be a timestamp, got %T (%#v)", rowIdx, key, icebergValue, icebergValue)
					minAllowed := time.Now().Add(-1 * time.Hour)
					require.Falsef(t, ts.Before(time.Now().Add(-1*time.Hour)), "Row %d: %q is too old: %v, should not be earlier than %v", rowIdx, key, ts, minAllowed)
				}
			}
		}
		if !isCDC && icebergMap[constants.CdcTimestamp] != nil {
			ts, ok := normalizeToTime(icebergMap[constants.CdcTimestamp])
			require.Truef(t, ok, "expected %q to be a timestamp, got %T", constants.CdcTimestamp, icebergMap[constants.CdcTimestamp])
			// Normalize to UTC to keep tests stable across environments (Local vs UTC).
			require.Equal(t, time.Unix(0, 0).UTC(), ts.UTC())
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

	if excludedColumn != "" {
		_, ok := icebergSchema[Reformat(excludedColumn)]
		require.Falsef(t, ok, "Excluded column %q should not exist in Iceberg schema", excludedColumn)
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
	// Verify datatypes for CDC/default columns as well
	if isCDC {
		for col, expectedIceType := range defaultCDCColumnsSchema {
			iceType, found := icebergSchema[col]
			require.True(t, found, "CDC column %s not found in Iceberg schema", col)

			require.Equal(t, expectedIceType, iceType,
				"CDC data type mismatch for column %s: expected %s, got %s", col, expectedIceType, iceType)
		}
		t.Logf("Verified datatypes for CDC columns in Iceberg after sync")
	}

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

// VerifyIcebergNoDuplicates asserts that no duplicate _olake_id values exist for the given
// _op_type in the Iceberg table.
func VerifyIcebergNoDuplicates(ctx context.Context, t *testing.T, tableName, icebergDB, opSymbol string, expectedRowCountByOpType int64) {
	t.Helper()

	spark, err := sparkSession(ctx, t)
	require.NoError(t, err, "Failed to connect to Spark Connect server for duplicate check")

	fullTableName := fmt.Sprintf("%s.%s.%s", icebergCatalog, icebergDB, tableName)

	// Refresh to get the latest committed Iceberg snapshot.
	refreshQuery := fmt.Sprintf("REFRESH TABLE %s", fullTableName)
	if _, refreshErr := spark.Sql(ctx, refreshQuery); refreshErr != nil {
		t.Logf("REFRESH TABLE (non-fatal): %v", refreshErr)
	}

	countQuery := fmt.Sprintf(
		"SELECT COUNT(*) AS total, COUNT(DISTINCT _olake_id) AS distinct_count FROM %s WHERE _op_type = '%s'",
		fullTableName, opSymbol,
	)
	t.Logf("Executing duplicate-check query: %s", countQuery)

	df, err := spark.Sql(ctx, countQuery)
	require.NoError(t, err, "Failed to run duplicate-check COUNT query")

	rows, err := df.Collect(ctx)
	require.NoError(t, err, "Failed to collect duplicate-check COUNT results")
	require.Len(t, rows, 1, "COUNT query must return exactly one row")

	total, ok := rows[0].Value("total").(int64)
	require.True(t, ok, "COUNT(*) value is not int64: %T", rows[0].Value("total"))

	distinct, ok2 := rows[0].Value("distinct_count").(int64)
	require.True(t, ok2, "COUNT(DISTINCT) value is not int64: %T", rows[0].Value("distinct_count"))

	// 1. No duplicates: every row must have a unique _olake_id.
	require.Equal(t, total, distinct,
		"Duplicate rows detected for _op_type='%s': total=%d, distinct=%d. "+
			"Iceberg MERGE INTO did not deduplicate re-synced records.",
		opSymbol, total, distinct)

	// 2. Exact count: when caller specifies an expected row count, enforce it so that both
	//    over-sync (old rows re-processed and inserted again) and under-sync (new rows missed)
	//    are caught.
	if expectedRowCountByOpType > 0 {
		require.Equal(t, expectedRowCountByOpType, distinct,
			"Row count mismatch for _op_type='%s': expected %d distinct rows, got %d. "+
				"Either old rows were re-synced (over-sync) or new rows were missed (under-sync).",
			opSymbol, expectedRowCountByOpType, distinct)
	}

	t.Logf("Duplicate check passed for _op_type='%s': %d rows, all unique by _olake_id (expected %d)",
		opSymbol, distinct, expectedRowCountByOpType)
}

// VerifyParquetSync verifies that data was correctly synchronized to Parquet files in MinIO
func VerifyParquetSync(t *testing.T, tableName, parquetDB string, datatypeSchema map[string]string, defaultCDCColumnsSchema map[string]string, schema map[string]interface{}, opSymbol, driver string, isCDC bool, excludedColumn string) {
	t.Helper()
	ctx := t.Context()

	spark, err := sparkSession(ctx, t)
	require.NoError(t, err, "Failed to connect to Spark Connect server")

	parquetPath := fmt.Sprintf("s3a://warehouse/%s/%s", parquetDB, tableName)
	viewName := fmt.Sprintf("`%s_view_%d`", tableName, time.Now().UnixNano())

	// create a temporary view for parquet files, allows to run describe query
	createViewQuery := fmt.Sprintf(
		"CREATE OR REPLACE TEMP VIEW %s AS SELECT * FROM parquet.`%s/*.parquet`",
		viewName, parquetPath,
	)

	// Retry logic for transient Spark connection issues (e.g., catalog connection pool exhaustion)
	const maxRetries = 3
	for attempt := 1; attempt <= maxRetries; attempt++ {
		_, err = spark.Sql(ctx, createViewQuery)
		if err == nil {
			break
		}
		// For delete operations, if path doesn't exist that's acceptable (no data written)
		if opSymbol == "d" && strings.Contains(err.Error(), "PATH_NOT_FOUND") {
			t.Logf("Delete verification passed: Parquet path does not exist (no data written)")
			return
		}
		if attempt < maxRetries {
			t.Logf("Attempt %d/%d: Failed to create view, retrying in 2s: %v", attempt, maxRetries, err)
			time.Sleep(2 * time.Second)
		}
	}
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
	// Kafka append keeps _op_type 'c'. Upsert writes c (first insert, Iceberg rewrite), u, or d.
	// col_included appears only on evolved/update rows.
	if driver == string(constants.Kafka) {
		if _, ok := schema["col_included"]; ok {
			selectQuery += " AND `col_included` IS NOT NULL"
		}
	}
	t.Logf("Executing Parquet query: %s", selectQuery)

	df, err := spark.Sql(ctx, selectQuery)
	require.NoError(t, err, "Failed to run select query on Parquet files")

	rows, err := df.Collect(ctx)
	require.NoError(t, err, "Failed to collect rows from Parquet query")

	// For delete operations, accept both 0 and 1 row (both are valid outcomes)
	if opSymbol == "d" {
		if len(rows) > 0 {
			deletedID := rows[0].Value("_olake_id")
			require.NotEmpty(t, deletedID, "Delete verification failed: _olake_id should not be empty")
		}
		t.Logf("Delete verification passed: found %d row(s) for _op_type = 'd'", len(rows))
		return
	}

	// For non-delete operations, require at least one row
	require.NotEmpty(t, rows, "No rows returned for _op_type = '%s'", opSymbol)

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
		if isCDC {
			for key := range defaultCDCColumnsSchema {
				val, ok := parquetMap[key]
				require.Truef(t, ok, "Row %d: missing column %q in Parquet result", rowIdx, key)
				// Kafka offset, partition can be 0, NotEmpty fails for 0 so we check for NotNil instead.
				if key == "_kafka_offset" || key == "_kafka_partition" {
					require.NotNil(t, val, "Row %d: expected column %q to be non-empty, got %#v", rowIdx, key, val)
				} else {
					require.NotEmpty(t, val, "Row %d: expected column %q to be non-empty, got %#v", rowIdx, key, val)
				}
				if key == constants.CdcTimestamp {
					ts, ok := normalizeToTime(val)
					require.Truef(t, ok, "Row %d: expected %q to be a timestamp, got %T (%#v)", rowIdx, key, val, val)
					minAllowed := time.Now().Add(-1 * time.Hour)
					require.Falsef(t, ts.Before(time.Now().Add(-1*time.Hour)), "Row %d: %q is too old: %v, should not be earlier than %v", rowIdx, key, ts, minAllowed)
				}
			}
		}
		if !isCDC && parquetMap[constants.CdcTimestamp] != nil {
			ts, ok := normalizeToTime(parquetMap[constants.CdcTimestamp])
			require.Truef(t, ok, "expected %q to be a timestamp, got %T", constants.CdcTimestamp, parquetMap[constants.CdcTimestamp])
			// Normalize to UTC to keep tests stable across environments (Local vs UTC).
			require.Equal(t, time.Unix(0, 0).UTC(), ts.UTC())
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
	if excludedColumn != "" {
		_, ok := parquetSchema[Reformat(excludedColumn)]
		require.Falsef(t, ok, "Excluded column %q should not exist in Parquet schema", excludedColumn)
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
	// Verify datatypes for CDC/default columns as well
	if isCDC {
		for col, expectedPqType := range defaultCDCColumnsSchema {
			pqType, found := parquetSchema[col]
			require.True(t, found, "CDC column %s not found in Parquet schema", col)
			require.Equal(t, expectedPqType, pqType,
				"CDC data type mismatch for column %s: expected %s, got %s", col, expectedPqType, pqType)
		}
	}
	t.Logf("Verified datatypes for CDC columns in Parquet after sync")
}

func (cfg *PerformanceTest) TestPerformance(t *testing.T) {
	ctx := t.Context()
	// The perf suite runs against the external DBs source.json describes, so hand every
	// ExecuteQuery their credentials; the integration suites leave this nil (local containers).
	cfg.TestConfig.SourceBaseConfig = ReadSourceConfig(t, cfg.TestConfig.HostSourcePath)

	// checks if the current rps (from stats.json) is at least 90% of the benchmark rps
	checkBenchmarkRPS := func(config TestConfig, isBackfill bool) (bool, float64, error) {
		// get current RPS
		var stats SyncSpeed
		if err := UnmarshalFile(config.HostStatsPath, &stats, false); err != nil {
			return false, 0, err
		}
		rps, err := ParseFloat64(strings.Split(stats.Speed, " ")[0])
		if err != nil {
			return false, 0, fmt.Errorf("failed to get RPS from stats: %s", err)
		}

		// Get past benchmark RPS stats
		benchmarks, err := loadBenchmarks(config.BenchmarksPath)
		if err != nil {
			return false, 0, err
		}

		averageRPS, observations := benchmarks.stats(isBackfill)
		t.Logf("currentRPS: %.2f, averageRPS: %.2f, observations: %d", rps, averageRPS, observations)

		// No benchmarks exist yet for this driver/mode
		// Skip validation to allow initial benchmarking.
		if observations == 0 {
			t.Logf("No benchmarks exist yet for %s %s mode, skipping validation", config.Driver, Ternary(isBackfill, "backfill", "cdc").(string))
			return true, rps, nil
		}
		if rps < BenchmarkThreshold*averageRPS {
			return false, rps, nil
		}
		return true, rps, nil
	}

	recordBenchmark := func(config TestConfig, isBackfill bool, rps float64) error {
		benchmarks, err := loadBenchmarks(config.BenchmarksPath)
		if err != nil {
			return err
		}
		return benchmarks.record(isBackfill, rps)
	}

	// runPerfOlake runs the driver image with host networking so the perf run reaches the
	// external benchmark databases directly, exactly as a deployed sync would.
	runPerfOlake := func(olakeArgs ...string) (int, []byte, error) {
		getOrBuildDriverImage(t, cfg.TestConfig)
		args := dockerRunArgs(cfg.TestConfig, []string{"--network", "host"}, olakeArgs)
		out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
		return dockerExitResult(out, err, olakeArgs[0])
	}

	// syncWithTimeout runs a sync bounded by SyncTimeout. Hitting the window is expected (it is
	// a bounded throughput measurement, not a failure), so the still-running container is stopped.
	syncWithTimeout := func(olakeArgs ...string) ([]byte, error) {
		name := fmt.Sprintf("olake-perf-%s", cfg.TestConfig.Driver)
		_ = exec.Command("docker", "rm", "-f", name).Run() // drop any stale container from a previous run
		timedCtx, cancel := context.WithTimeout(ctx, SyncTimeout)
		defer cancel()
		args := dockerRunArgs(cfg.TestConfig, []string{"--network", "host", "--name", name}, olakeArgs)
		out, err := exec.CommandContext(timedCtx, "docker", args...).CombinedOutput()
		if timedCtx.Err() == context.DeadlineExceeded {
			_ = exec.Command("docker", "kill", name).Run()
			return out, nil
		}
		code, out, derr := dockerExitResult(out, err, "sync")
		if derr != nil {
			return out, derr
		}
		if code != 0 {
			return out, fmt.Errorf("sync failed (%d)", code)
		}
		return out, nil
	}

	t.Run("performance", func(t *testing.T) {
		// reset CDC config
		if cfg.TestConfig.Driver == string(constants.Postgres) || cfg.TestConfig.Driver == string(constants.MySQL) {
			cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "reset_cdc_config")
			t.Log("CDC config reset completed")
		}

		t.Logf("(backfill) running performance test for %s", cfg.TestConfig.Driver)

		destDBPrefix := fmt.Sprintf("performance_%s", cfg.TestConfig.Driver)

		t.Log("(backfill) discover started")
		if code, output, err := runPerfOlake(discoverArgs(*cfg.TestConfig, "--destination-database-prefix", destDBPrefix)...); err != nil || code != 0 {
			t.Fatalf("failed to perform discover:\n%s", string(output))
		}
		t.Log("(backfill) discover completed")

		if err := updateSelectedStreams(cfg.TestConfig, cfg.Namespace, "", "", cfg.BackfillStreams, ""); err != nil {
			t.Fatalf("failed to update streams: %s", err)
		}

		t.Log("(backfill) sync started")
		// MySQL derives its chunk plan from InnoDB statistics, which drift between runs; seed the
		// committed plan instead so every benchmark measures the same split.
		usePreChunkedState := cfg.TestConfig.Driver == string(constants.MySQL)
		if usePreChunkedState {
			if err := seedPerformanceState(cfg.TestConfig); err != nil {
				t.Fatalf("failed to seed pre-chunked state from %s: %s", cfg.TestConfig.HostPerformanceStatePath, err)
			}
		}
		if output, err := syncWithTimeout(syncArgs(*cfg.TestConfig, usePreChunkedState, "iceberg", "--destination-database-prefix", destDBPrefix)...); err != nil {
			t.Fatalf("failed to perform sync:\n%s", string(output))
		}
		t.Log("(backfill) sync completed")

		checkRPS, currentRPS, err := checkBenchmarkRPS(*cfg.TestConfig, true)
		if err != nil {
			t.Fatalf("failed to check RPS: %s", err)
		}
		require.True(t, checkRPS, fmt.Sprintf("%s backfill performance below benchmark", cfg.TestConfig.Driver))

		if err := recordBenchmark(*cfg.TestConfig, true, currentRPS); err != nil {
			t.Fatalf("failed to write RPS history: %s", err)
		}
		t.Logf("✅ SUCCESS: %s backfill", cfg.TestConfig.Driver)

		if len(cfg.CDCStreams) > 0 {
			t.Logf("(cdc) running performance test for %s", cfg.TestConfig.Driver)

			t.Log("(cdc) setup cdc started")
			cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "setup_cdc")
			t.Log("(cdc) setup cdc completed")

			t.Log("(cdc) discover started")
			if code, output, err := runPerfOlake(discoverArgs(*cfg.TestConfig, "--destination-database-prefix", destDBPrefix)...); err != nil || code != 0 {
				t.Fatalf("failed to perform discover:\n%s", string(output))
			}
			t.Log("(cdc) discover completed")

			if err := updateSelectedStreams(cfg.TestConfig, cfg.Namespace, "", "", cfg.CDCStreams, ""); err != nil {
				t.Fatalf("failed to update streams: %s", err)
			}

			t.Log("(cdc) state creation started")
			if code, output, err := runPerfOlake(syncArgs(*cfg.TestConfig, false, "iceberg", "--destination-database-prefix", destDBPrefix)...); err != nil || code != 0 {
				t.Fatalf("failed to perform initial sync:\n%s", string(output))
			}
			t.Log("(cdc) state creation completed")

			t.Log("(cdc) trigger cdc started")
			cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "bulk_cdc_data_insert")
			t.Log("(cdc) trigger cdc completed")

			t.Log("(cdc) sync started")
			if output, err := syncWithTimeout(syncArgs(*cfg.TestConfig, true, "iceberg", "--destination-database-prefix", destDBPrefix)...); err != nil {
				t.Fatalf("failed to perform CDC sync:\n%s", string(output))
			}
			t.Log("(cdc) sync completed")

			checkRPS, currentRPS, err := checkBenchmarkRPS(*cfg.TestConfig, false)
			if err != nil {
				t.Fatalf("failed to check RPS: %s", err)
			}
			require.True(t, checkRPS, fmt.Sprintf("%s CDC performance below benchmark", cfg.TestConfig.Driver))

			if err := recordBenchmark(*cfg.TestConfig, false, currentRPS); err != nil {
				t.Fatalf("failed to write RPS history: %s", err)
			}
			t.Logf("✅ SUCCESS: %s cdc", cfg.TestConfig.Driver)
		}
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

func normalizeToTime(v interface{}) (time.Time, bool) {
	switch ts := v.(type) {
	case time.Time:
		return ts, true
	case arrow.Timestamp:
		return time.Unix(0, int64(ts)*int64(time.Microsecond)).UTC(), true
	default:
		return time.Time{}, false
	}
}

// DDLBacklogTable and DDLSwapTable are the two source tables the DDL suite drives. Without
// binlog_row_metadata=FULL (MySQL 5.7, or 8.x at MINIMAL) row events carry no column names, so
// OLake decodes them with column metadata it saved in the state and advances through each DDL in
// binlog order. A row decodes right only when every DDL before it, and none after it, reached
// that metadata. On a FULL server every row carries its own metadata, so the same checks hold
// without the cache: the suite runs on every MySQL stack and expects the same rows on each.
const (
	DDLBacklogTable = "ddl_backlog"
	DDLSwapTable    = "ddl_swap"
)

// ddlTestDrivers gates the DDL suite the way icebergDVTestDrivers gates the deletion-vector one:
// the binlog column cache it exercises is MySQL's.
var ddlTestDrivers = []constants.DriverType{constants.MySQL}

func hasDDLTest(driver string) bool {
	return slices.Contains(ddlTestDrivers, constants.DriverType(driver))
}

// ddlColumnSchema is the type_schema both DDL tables start with. Columns a DDL adds later reach
// the destination through schema evolution, as in the main suite's evolve-schema step.
func ddlColumnSchema() map[string]interface{} {
	return map[string]interface{}{
		"id":                    map[string]interface{}{"type": []interface{}{"integer_small"}, "destination_column_name": "id"},
		"a":                     map[string]interface{}{"type": []interface{}{"integer_small", "null"}, "destination_column_name": "a"},
		"b":                     map[string]interface{}{"type": []interface{}{"integer_small", "null"}, "destination_column_name": "b"},
		"status":                map[string]interface{}{"type": []interface{}{"string", "null"}, "destination_column_name": "status"},
		"_op_type":              map[string]interface{}{"type": []interface{}{"string", "null"}, "destination_column_name": "_op_type", "olake_column": true},
		"_olake_id":             map[string]interface{}{"type": []interface{}{"string", "null"}, "destination_column_name": "_olake_id", "olake_column": true},
		"_olake_timestamp":      map[string]interface{}{"type": []interface{}{"timestamp_micro", "null"}, "destination_column_name": "_olake_timestamp", "olake_column": true},
		"_cdc_timestamp":        map[string]interface{}{"type": []interface{}{"timestamp_micro", "null"}, "destination_column_name": "_cdc_timestamp", "olake_column": true},
		"_cdc_binlog_file_name": map[string]interface{}{"type": []interface{}{"string", "null"}, "destination_column_name": "_cdc_binlog_file_name", "olake_column": true},
		"_cdc_binlog_file_pos":  map[string]interface{}{"type": []interface{}{"integer", "null"}, "destination_column_name": "_cdc_binlog_file_pos", "olake_column": true},
	}
}

// ddlCatalogDoc builds a streams.json selecting both DDL tables under CDC. Like dvCatalogDoc it
// sets destination_database itself, since --destination-database-prefix only reaches a catalog
// through discover.
func ddlCatalogDoc(namespace, destinationDatabase string) map[string]interface{} {
	streams := []interface{}{}
	selected := []interface{}{}
	for _, table := range []string{DDLBacklogTable, DDLSwapTable} {
		streams = append(streams, map[string]interface{}{
			"stream": map[string]interface{}{
				"name":                       table,
				"namespace":                  namespace,
				"type_schema":                map[string]interface{}{"properties": ddlColumnSchema()},
				"supported_sync_modes":       []interface{}{"strict_cdc", "full_refresh", "incremental", "cdc"},
				"source_defined_primary_key": []interface{}{"id"},
				"available_cursor_fields":    []interface{}{"id", "a", "b", "status"},
				"sync_mode":                  "cdc",
				"destination_database":       destinationDatabase,
				"destination_table":          table,
				"default_stream_properties":  map[string]interface{}{"normalization": true, "append_mode": false},
			},
		})
		selected = append(selected, map[string]interface{}{
			"stream_name":     table,
			"partition_regex": "",
			"normalization":   true,
			"append_mode":     false,
		})
	}
	return map[string]interface{}{
		"streams":          streams,
		"selected_streams": map[string]interface{}{namespace: selected},
	}
}

// ddlSync runs one sync and fails the test on a non-zero exit. It uses its own
// --destination-database-prefix for the reason dvSyncArgs gives: every sync creates a check
// table named from the prefix, which is not safe to share with the suites running in parallel.
func (cfg *IntegrationTest) ddlSync(ctx context.Context, t *testing.T, label string) {
	t.Helper()
	args := syncArgs(*cfg.TestConfig, true, "iceberg", "--destination-database-prefix", fmt.Sprintf("integration_%s_ddl", cfg.TestConfig.Driver))
	code, out, err := runOlake(ctx, t, cfg.TestConfig, args...)
	require.NoError(t, err, "%s sync failed to launch: %s", label, out)
	require.Zerof(t, code, "%s sync exited %d: %s", label, code, out)
}

// ddlRows reads the given columns of every row in a destination table, keyed by id, with each
// value printed as text and NULL for a missing one.
func ddlRows(ctx context.Context, t *testing.T, spark sql.SparkSession, fullTableName string, columns ...string) map[string]map[string]string {
	t.Helper()
	refreshTable(ctx, t, spark, fullTableName)
	query := fmt.Sprintf("SELECT id, %s FROM %s", strings.Join(columns, ", "), fullTableName)
	df, err := spark.Sql(ctx, query)
	require.NoError(t, err, "spark sql failed: %s", query)
	rows, err := df.Collect(ctx)
	require.NoError(t, err, "spark collect failed: %s", query)

	out := make(map[string]map[string]string, len(rows))
	for _, r := range rows {
		row := make(map[string]string, len(columns))
		for _, column := range columns {
			value := r.Value(column)
			row[column] = Ternary(value == nil, "NULL", fmt.Sprint(value)).(string)
		}
		out[fmt.Sprint(r.Value("id"))] = row
	}
	return out
}

// TestDDL syncs both tables three times: an initial sync records the binlog position, the
// second decodes one row per table and saves its column metadata while another table takes
// writes, and the third reads a backlog of rows and DDL written while no sync ran. ExecuteQuery
// owns the SQL (ddl-create, ddl-seed, ddl-noise-start / -stop, ddl-backlog, ddl-drop).
//
// In the backlog, ddl_backlog takes a reorder, an ENUM change behind a "--" comment, an ADD
// COLUMN with no character set, DDL that must leave it alone (CREATE TABLE IF NOT EXISTS of
// itself, a trigger, DDL on other tables, one the parser cannot read) and a second reorder, with
// a row after each. ddl_swap takes a gh-ost cutover: a shadow table is altered, a row lands in
// ddl_swap during the copy, then RENAME swaps the two.
func (cfg *IntegrationTest) TestDDL(t *testing.T) {
	ctx := t.Context()
	if !hasDDLTest(cfg.TestConfig.Driver) {
		t.Skip("DDL suite is gated to specific drivers")
	}
	defer dropIcebergTable(t, DDLBacklogTable, cfg.DestinationDB)
	defer dropIcebergTable(t, DDLSwapTable, cfg.DestinationDB)
	if !keepTestData() {
		defer cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "ddl-drop")
	}

	cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "ddl-create")
	raw, err := json.MarshalIndent(ddlCatalogDoc(cfg.Namespace, cfg.DestinationDB), "", "  ")
	require.NoError(t, err, "failed to build the DDL catalog")
	require.NoError(t, writeHostFile(cfg.TestConfig.HostCatalogPath, raw), "failed to write the DDL catalog")
	require.NoError(t, resetStateFile(cfg.TestConfig), "failed to reset the state file")

	cfg.ddlSync(ctx, t, "initial")

	// Writes elsewhere during the saving sync move the server past the position it stops at, so
	// the metadata it loads is anchored after that position: the busy-server case.
	cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "ddl-seed")
	cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "ddl-noise-start")
	defer cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "ddl-noise-stop")
	cfg.ddlSync(ctx, t, "saving")
	cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "ddl-noise-stop")

	cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "ddl-backlog")
	cfg.ddlSync(ctx, t, "backlog")

	spark := getSparkSession(ctx, t)
	t.Run("Backlog of DDL", func(t *testing.T) {
		got := ddlRows(ctx, t, spark, fmt.Sprintf("%s.%s.%s", icebergCatalog, cfg.DestinationDB, DDLBacklogTable), "a", "b", "status", "note")
		require.Equal(t, map[string]map[string]string{
			"100": {"a": "0", "b": "0", "status": "new", "note": "NULL"},
			"1":   {"a": "10", "b": "20", "status": "paid", "note": "NULL"},     // saved metadata, from the busy sync
			"2":   {"a": "11", "b": "21", "status": "paid", "note": "NULL"},     // after the reorder
			"3":   {"a": "12", "b": "22", "status": "refunded", "note": "NULL"}, // after the commented ENUM change
			"4":   {"a": "13", "b": "23", "status": "new", "note": "n4"},        // after ADD COLUMN with no charset
			"5":   {"a": "14", "b": "24", "status": "paid", "note": "n5"},       // after DDL that must not evict the table
			"6":   {"a": "15", "b": "25", "status": "refunded", "note": "n6"},   // after the second reorder
		}, got)
	})
	t.Run("gh-ost cutover", func(t *testing.T) {
		got := ddlRows(ctx, t, spark, fmt.Sprintf("%s.%s.%s", icebergCatalog, cfg.DestinationDB, DDLSwapTable), "a", "b", "status")
		require.Equal(t, map[string]map[string]string{
			"100": {"a": "0", "b": "0", "status": "new"},
			"1":   {"a": "10", "b": "20", "status": "paid"}, // before the swap
			"2":   {"a": "11", "b": "21", "status": "paid"}, // during the copy, between DDLs on the shadow table
			"3":   {"a": "12", "b": "22", "status": "new"},  // after the swap, in the new layout
		}, got)
	})
}

func setStreamUpsert(cfg *TestConfig, namespace, table string, dedupKeys []string) error {
	if len(dedupKeys) == 0 {
		return fmt.Errorf("upsert mode requires at least one dedup key")
	}
	keys := make([]interface{}, 0, len(dedupKeys))
	for _, k := range dedupKeys {
		keys = append(keys, k)
	}
	return editJSONFile(cfg.HostCatalogPath, func(doc map[string]interface{}) error {
		selected, _ := doc["selected_streams"].(map[string]interface{})
		namespaceStreams, ok := selected[namespace].([]interface{})
		if !ok {
			return fmt.Errorf("namespace %q missing in selected streams", namespace)
		}
		matched := false
		tableName := normalizeStreamName(cfg.Driver, table)
		for _, raw := range namespaceStreams {
			s, ok := raw.(map[string]interface{})
			if !ok || fmt.Sprint(s["stream_name"]) != tableName {
				continue
			}
			s["append_mode"] = false
			s["dedup_keys"] = keys
			matched = true
		}
		if !matched {
			return fmt.Errorf("stream %q not found in namespace %q", table, namespace)
		}
		return nil
	})
// KeepTestData reports whether the test suite should keep the source data after a run, for debugging.
func KeepTestData() bool {
	return strings.EqualFold(os.Getenv(KeepTestDataEnvVar), "true")
}
