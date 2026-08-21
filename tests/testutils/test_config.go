package testutils

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/stretchr/testify/require"
)

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

	// DriverImage the test is intedned to run under. Defaults to the building the image of the current codebase
	DriverImage string

	// IcebergDestinationFile names the destination config an iceberg sync runs against. It is how the
	// writer variants are selected: testIcebergWriter points it at the arrow config for the arrow
	// subtests, and Setup resets it to the committed base config.
	IcebergDestinationFile string

	// SyncImage, when set, picks the image each sync runs on from whether it carries state. The
	// compatibility suite runs the stateless initial load on the baseline and every --state sync
	// after it on the candidate.
	SyncImage func(useState bool) string `json:"-"`

	// FilterFlags, when set, drops olake flags the image under test does not understand. The
	// compatibility suite hands a baseline the flag vocabulary of its own release.
	FilterFlags func(flags []string) []string `json:"-"`

	// IsolateSource renames the source resources concurrent suites contend on (mssql's database,
	// postgres's slot, kafka's consumer group). Off for suites that run alone, whose catalog is
	// compared against the committed fixture.
	IsolateSource bool

	// SourceBaseConfig, when set, is the parsed source config ExecuteQuery implementations connect
	// with; nil means the integration containers' fixed local credentials.
	SourceBaseConfig SourceConfig

	// Driver shape: the same for every suite this driver runs, so it is declared once here
	// rather than per test.
	Namespace       string
	ExecuteQuery    func(ctx context.Context, t *testing.T, cfg *TestConfig, operation string) `json:"-"`
	DestinationDB   string
	CursorField     string
	PartitionRegex  string
	FilterConfig    string
	ColumnToExclude string

	// OlakeRootPath is the repo the tests run from, the directory `make docker.<driver>.build`
	// runs in and the committed fixtures are read from. Resolved by setupWorkingDir.
	OlakeRootPath string

	// TestWorkingDir is this config's private /tmp working dir, the folder where we run olake
	// commands and expect the generated files. Every file the suite reads or writes lives in it and
	// is addressed by name through GetFilePath, so there is no path to keep a field for.
	TestWorkingDir string
}

// uniqueID identifies this run among every suite that can be running beside it: the driver, the
// data format it drives and the suite itself. Everything a suite must not share is named after it.
func (c *TestConfig) uniqueID() string {
	return Combine(c.withSuite(c.Driver))
}

func (c *TestConfig) withSuite(base string) string {
	return Combine(base, c.DataFormat, c.Suite)
}

// WithImagePlatform is used to override the platform for the test container image.
// This is useful for testing on different architectures (e.g., arm64 vs amd64).
func (c *TestConfig) WithImagePlatform(platform string) *TestConfig {
	c.ImagePlatform = platform
	return c
}

// TestTableName is the source table a suite drives. The suite suffix is what keeps concurrent
// suites off each other's table -- without it they race the same DROP/CREATE.
func (c *TestConfig) GetTableName() string {
	return Combine(c.Driver, c.DataFormat, "test_table_olake", c.Suite)
}

// GetDestinationDBPrefix returns the unique prefix for destination per test suite based on config.
func (c *TestConfig) GetDestinationDBPrefix() string {
	return c.uniqueID()
}

// Validate checks that the TestConfig is valid and complete in order to derive/setup. It asserts
// what Setup and every suite body already assume, so a malformed config fails by name here rather
// than as a confusing container failure once the driver is running.
func (c *TestConfig) Validate(t *testing.T) {
	t.Helper()
	require.NotEmpty(t, c.Driver, "TestConfig.Driver is not set")
	require.NotEmpty(t, c.Suite, "TestConfig.Suite is not set; the suite has nothing to isolate its resources by")
	require.Equalf(t, strings.ToLower(c.Suite), c.Suite, "TestConfig.Suite %q must be lowercase", c.Suite)
	require.NotNil(t, c.ExecuteQuery, "TestConfig.ExecuteQuery is not set; the suite has no way to communicate with its source")
	require.NotEmpty(t, c.Namespace, "TestConfig.Namespace is not set")
}

// setup initializes the derived fields of the TestConfig and does the steup for configuring the test isolation like isolated configs
func (c *TestConfig) Setup(t *testing.T) {
	t.Helper()

	c.setupWorkingDir(t)
	c.getOrBuildDriverImage(t)
	c.addTimingLogsMiddleware()
	c.applySuite(t)

	t.Logf("test setup completed")
	t.Logf("Root Project directory: %s", c.OlakeRootPath)
	t.Logf("Test working directory: %s", c.TestWorkingDir)
	config, err := json.MarshalIndent(c, "", "  ")
	require.NoError(t, err, "failed to render the test config")
	t.Logf("TestConfig: %s", config)
}

func (c *TestConfig) addTimingLogsMiddleware() {
	executeFunc := c.ExecuteQuery
	c.ExecuteQuery = func(ctx context.Context, t *testing.T, cfg *TestConfig, operation string) {
		defer TrackPhaseTiming(t, c.Driver, fmt.Sprintf("query %q", operation))()
		executeFunc(ctx, t, cfg, operation)
	}
}

// getOrBuildDriverImage just sets the driver image in case  builds the driver image against current codebase
func (c *TestConfig) getOrBuildDriverImage(t *testing.T) {
	if c.DriverImage != "" {
		return
	}

	driverVersion := os.Getenv(driverVersionEnvVar)
	if driverVersion == "" {
		buildDriverImage(t, c)
		driverVersion = currentDriverVersion
	}

	c.DriverImage = getDriverImage(c.Driver, driverVersion)
}

// GetFilePath addresses a file in the suite's working directory by name -- the configs, the
// catalog, state and stats all live there, and the container reads them under the same names.
func (c *TestConfig) GetFilePath(fileName string) string {
	return filepath.Join(c.TestWorkingDir, fileName)
}

// GetFixturePath addresses a committed fixture in the driver's testdata directory, for the one
// thing a run must outlive its working directory: the benchmark history the perf suite appends to.
// Everything else a suite reads is the working copy setupWorkingDir made, via GetFilePath.
func (c *TestConfig) GetFixturePath(fileName string) string {
	return filepath.Join(c.OlakeRootPath, "tests", c.Driver, "testdata", c.DataFormat, fileName)
}

// RepoRoot is the git checkout the tests run from. Read straight from git rather than derived from
// the running test's directory, which is the one thing here the repo layout does not fix.
func RepoRoot(t *testing.T) string {
	t.Helper()
	root, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	require.NoError(t, err, "failed to determine the repo root; the tests run from a git checkout")
	return strings.TrimSpace(string(root))
}

// setupWorkingDir gives the suite a private working directory holding its own copy of every config
// the driver container reads, so the repo fixtures stay read-only and concurrent suites never share
// a writable file. The shared fixtures land first and the driver's own overwrite them by name, so a
// driver overrides a common config just by committing a file of the same name.
func (c *TestConfig) setupWorkingDir(t *testing.T) {
	t.Helper()

	c.OlakeRootPath = RepoRoot(t)

	workingDir, err := os.MkdirTemp("/tmp", fmt.Sprintf("olake-it-%s", c.uniqueID()))
	require.NoError(t, err, "failed to create the test working directory")
	t.Cleanup(func() {
		if err := os.RemoveAll(workingDir); err != nil {
			t.Logf("failed to remove the test working directory %s: %s", workingDir, err)
		}
	})
	c.TestWorkingDir = workingDir

	commonFixturesDir := filepath.Join(c.OlakeRootPath, "tests/testdata")
	driverFixuresDir := filepath.Join(c.OlakeRootPath, "tests", c.Driver, "testdata", c.DataFormat)
	for _, fixtures := range []string{commonFixturesDir, driverFixuresDir} {
		require.NoError(t, copyDirFiles(fixtures, workingDir), "failed to copy files to the test working directory")
	}
}

// getSourceEdit gives edit logic for drivers config to isolate it with other concurrent suites of the same driver.
// For example, for mssql, it will change the database name to a unique one per suite to provide CDC isolation.
func (c *TestConfig) getSourceEdit() func(map[string]interface{}) error {
	switch c.Driver {
	case string(constants.MSSQL):
		// Table separation alone races: DROP/CREATE TABLE modify database-scoped shared metadata
		// (system catalog, cdc schema) even for separate tables, and the loser transaction fails
		// as the deadlock victim (error 1205) -- so each suite owns a whole database.
		return func(mssqlSourceConfig map[string]interface{}) error {
			base, ok := mssqlSourceConfig["database"].(string)
			if !ok {
				return fmt.Errorf("no database in source config")
			}
			mssqlSourceConfig["database"] = Combine(base, c.Suite)
			return nil
		}
	case string(constants.Postgres):
		return func(pgSourceConfig map[string]interface{}) error {
			updateMethod, ok := pgSourceConfig["update_method"].(map[string]interface{})
			if !ok {
				return fmt.Errorf("no update_method object in source config")
			}
			updateMethod["replication_slot"] = c.GetTableName()
			return nil
		}
	case string(constants.S3):
		// Discover infers a schema from EVERY stream under path_prefix, so concurrent suites read
		// each other's files and 404 on whichever one is mid drop/seed. A prefix per suite is the
		// object-store equivalent of mssql's per-suite database.
		return func(s3SourceConfig map[string]interface{}) error {
			base, ok := s3SourceConfig["path_prefix"].(string)
			if !ok || base == "" {
				return fmt.Errorf("no path_prefix in source config")
			}
			s3SourceConfig["path_prefix"] = Combine(base, c.Suite)
			return nil
		}
	case string(constants.Kafka):
		return func(kafkaSourceConfig map[string]interface{}) error {
			base, ok := kafkaSourceConfig["consumer_group_id"].(string)
			if !ok || base == "" {
				return fmt.Errorf("no consumer_group_id in source config")
			}
			kafkaSourceConfig["consumer_group_id"] = Combine(base, c.Suite)
			return nil
		}
	}
	return nil
}

// applySuite gives the suite its own copy of every config the driver container reads, then updates
// the TestConfig fields and those copies to the names this suite owns.
func (c *TestConfig) applySuite(t *testing.T) {
	t.Helper()

	c.DestinationDB = c.withSuite(c.DestinationDB)

	// The arrow writer variant is derived, never committed: the base config stays the single
	// source of truth, and writer variants become a pure file choice (see testIcebergWriter),
	// starting from the base one here.
	c.IcebergDestinationFile = "iceberg_destination.json"
	require.NoError(t, CopyJSONWithEdit(c.GetFilePath("iceberg_destination.json"), c.GetFilePath("iceberg_destination_arrow.json"),
		func(destinationConf map[string]interface{}) error {
			writer, ok := destinationConf["writer"].(map[string]interface{})
			if !ok {
				return fmt.Errorf("no writer object in iceberg_destination.json")
			}
			writer["arrow_writes"] = true

			return nil
		}), "failed to edit the arrow destination config")

	if edit := c.getSourceEdit(); c.IsolateSource && edit != nil {
		require.NoError(t, EditJSONFile(c.GetFilePath("source.json"), edit),
			"failed to derive the source config for suite %q", c.Suite)
	}

	c.SeedCatalog(t)
}

// seedCatalog writes test_streams.json out as the suite's catalog, retargeting it at the suite's
// table. Field by field: stream names carry the source's casing, tables the destination's, so one
// text substitution would rename only one of them.
func (c *TestConfig) SeedCatalog(t *testing.T) {
	t.Helper()
	table := c.GetTableName()
	streamName := NormalizeStreamName(c.Driver, table)

	require.NoError(t, EditJSONFile(c.GetFilePath("test_streams.json"), func(catalog map[string]interface{}) error {
		entries, _ := catalog["streams"].([]interface{})
		for _, entry := range entries {
			wrapper, ok := entry.(map[string]interface{})
			if !ok {
				continue
			}
			stream, ok := wrapper["stream"].(map[string]interface{})
			if !ok {
				continue
			}
			oldStreamName := stream["name"].(string)
			if strings.HasPrefix(streamName, oldStreamName) {
				stream["name"] = streamName
			}

			oldTableName := stream["destination_table"].(string)
			if strings.HasPrefix(table, oldTableName) {
				stream["destination_table"] = table
			}
		}
		byNamespace, _ := catalog["selected_streams"].(map[string]interface{})
		for _, raw := range byNamespace {
			selected, _ := raw.([]interface{})
			for _, entry := range selected {
				stream, ok := entry.(map[string]interface{})
				if !ok {
					continue
				}
				oldStreamName := stream["stream_name"].(string)
				if strings.HasPrefix(streamName, oldStreamName) {
					stream["stream_name"] = streamName
				}
			}
		}
		return nil
	}), "failed to seed the %q catalog", c.Suite)
}
