package testutils

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

	// DriverVersion to run the test against. This can be commit id or a released version
	// Defaults to local codebase
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
	// database and prefixes, after applySuite renamed what it isolates. ExecuteQuery connects with
	// it, so the harness and olake always drive the same source.
	SourceBaseConfig SourceConfig `json:"-"`

	// Driver shape: the same for every suite this driver runs, so it is declared once here
	// rather than per test.
	Namespace       string
	ExecuteQuery    ExecuteQueryFn `json:"-"`
	DestinationDB   string
	CursorField     string
	PartitionRegex  string
	FilterConfig    string
	ColumnToExclude string

	// sourceEdit and streamEdit are the driver's own per-suite isolation: what a driver must rename
	// so its concurrent suites do not contend, and whatever that rename implies for the catalog.
	sourceEdit ConfigEditFn
	streamEdit ConfigEditFn
}

type TestConfigOption func(*TestConfig)

// ConfigEditFn edits one of the suite's working-copy JSON files. It receives the config so a
// driver can name what it isolates after the suite.
type ConfigEditFn func(cfg *TestConfig, doc map[string]interface{}) error

// NewTestConfig builds a driver's config from what a suite cannot derive for itself: the source it
// reads, the namespace it drives and the destination olake derives from them. dataFormat names the
// driver's testdata subdirectory, for the drivers that have one.
func NewTestConfig(t *testing.T, driver constants.DriverType, namespace, destinationDB string, executeQuery ExecuteQueryFn, opts ...TestConfigOption) (*TestConfig, error) {
	t.Helper()
	cfg := &TestConfig{
		Driver:        string(driver),
		Namespace:     namespace,
		DestinationDB: destinationDB,
		ExecuteQuery:  executeQuery,
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

func WithSourceEdit(edit ConfigEditFn) TestConfigOption {
	return func(c *TestConfig) {
		c.sourceEdit = edit
	}
}

func WithStreamEdit(edit ConfigEditFn) TestConfigOption {
	return func(c *TestConfig) {
		c.streamEdit = edit
	}
}

func (c *TestConfig) generateSuiteName(t *testing.T) {
	t.Helper()
	nonSuiteChars := regexp.MustCompile(`[^a-z0-9]+`)
	suite := strings.ToLower(t.Name())
	suite = strings.TrimPrefix(suite, "test")
	suite = strings.TrimPrefix(suite, strings.ToLower(string(c.Driver)))
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
	if err := c.applySuite(); err != nil {
		return err
	}

	sourceConfig, err := ReadSourceConfig(c.GetFilePath("source.json"))
	if err != nil {
		return fmt.Errorf("failed to read the source config of driver %q suite %q: %s", c.Driver, c.Suite, err)
	}
	c.SourceBaseConfig = sourceConfig

	return nil
}

func (c *TestConfig) String() string {
	config, _ := json.MarshalIndent(c, "", "  ")
	return string(config)
}

// pullOrBuildDriverImage just sets the driver image in case  builds the driver image against current codebase
func (c *TestConfig) pullOrBuildDriverImage(t *testing.T) (err error) {
	if c.DriverVersion == "" {
		c.DriverVersion = CurrentDriverVersion
	}

	if driverVersionEnv := os.Getenv(driverVersionEnvVar); driverVersionEnv != "" {
		c.DriverVersion = driverVersionEnv
	}

	return c.resolveImage(t)
}

// resolveImage turns a version string into an image ref present on the local daemon:
//
//	"local"                            -> built from current code
//	"latest", "v0.6.5"                 -> olakego/source-<driver>:<spec>, pulled
//	"9f3c1ab", "sha:9f3c1ab"           -> built from a detached worktree at that commit
func (c *TestConfig) resolveImage(t *testing.T) error {
	if c.DriverVersion == CurrentDriverVersion {
		return buildDriverImage(t, c)
	}
	// A commit id is abbreviated on the way in, and that short form becomes the image tag.
	if commitID, ok := ResolveToCommit(c.OlakeRootPath, c.DriverVersion); ok {
		c.DriverVersion = commitID
		return buildImageFromCommit(t, c, commitID)
	}

	return ensureImagePresent(t, c.GetDriverImage())
}

func (c *TestConfig) addTimingLogsMiddleware() {
	executeFunc := c.ExecuteQuery
	c.ExecuteQuery = func(ctx context.Context, t *testing.T, cfg *TestConfig, operation string) {
		defer TrackPhaseTiming(t, c.Driver, fmt.Sprintf("query %q", operation))()
		executeFunc(ctx, t, cfg, operation)
	}
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

	commonFixturesDir := filepath.Join(c.OlakeRootPath, "tests/testdata")
	driverFixuresDir := filepath.Join(c.OlakeRootPath, "tests", c.Driver, "testdata", c.DataFormat)
	for _, fixtures := range []string{commonFixturesDir, driverFixuresDir} {
		if err := CopyDirFiles(fixtures, c.TestWorkingDir); err != nil {
			return fmt.Errorf("failed to copy the fixtures of %s into %s: %s", fixtures, c.TestWorkingDir, err)
		}
	}
	return nil
}

// applySuite derives every config the driver container reads from its committed base, so the base
// files stay untouched, and retargets the copies at the names this suite owns.
func (c *TestConfig) applySuite() error {
	c.DestinationDB = c.withSuite(c.DestinationDB)

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

	isolateSource := func(source map[string]interface{}) error {
		if c.sourceEdit == nil {
			return nil
		}
		return c.sourceEdit(c, source)
	}
	if err := c.getOrRenderConfig("source.template.json", "source.json", isolateSource); err != nil {
		return fmt.Errorf("failed to isolate the source config of driver %q for suite %q: %s", c.Driver, c.Suite, err)
	}

	isolateCatalog := func(catalog map[string]interface{}) error {
		if c.streamEdit == nil {
			return nil
		}
		return c.streamEdit(c, catalog)
	}
	if err := c.getOrRenderConfig("streams.template.json", "streams.json", isolateCatalog); err != nil {
		return fmt.Errorf("failed to retarget the catalog of driver %q at suite %q table %s: %s", c.Driver, c.Suite, c.GetTableName(), err)
	}
	return nil
}

func (c *TestConfig) getOrRenderConfig(template, configPath string, edit editFunc) error {
	_, err := os.Stat(c.GetFilePath(configPath))
	if errors.Is(err, os.ErrNotExist) {
		return c.renderConfig(template, configPath, edit)
	} else if err != nil {
		return err
	}

	return nil
}

// renderConfig expands the placeholders of the committed template in base into the working copy the
// container reads at out, and applies edit to the result.
func (c *TestConfig) renderConfig(base, out string, edit editFunc) error {
	raw, err := os.ReadFile(c.GetFilePath(base))
	if err != nil {
		return fmt.Errorf("failed to read %s: %s", base, err)
	}
	expanded, err := c.expandPlaceholders(raw)
	if err != nil {
		return fmt.Errorf("failed to expand %s: %s", base, err)
	}
	doc, err := ParseJSONDoc(expanded)
	if err != nil {
		return fmt.Errorf("failed to parse %s: %s", base, err)
	}
	if err := edit(doc); err != nil {
		return err
	}
	data, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal %s: %s", out, err)
	}
	return WriteHostFile(c.GetFilePath(out), data)
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

// WriteHostFile writes to the shared /testdata mount, unlinking first: the container runs as root,
// so on Linux CI the test user cannot truncate a file a previous run left behind, only replace it.
func WriteHostFile(path string, data []byte) error {
	_ = os.Remove(path)
	return os.WriteFile(path, data, 0600)
}

// NormalizeStreamName uppercases the stream name for drivers whose catalogs store
// uppercase identifiers (e.g. Oracle).
func NormalizeStreamName(driver, streamName string) string {
	return Ternary(slices.Contains(constants.UppercaseStreamDrivers, constants.DriverType(driver)), strings.ToUpper(streamName), streamName).(string)
}

// UpdateSelectedStreams rewrites selected_streams so only the given streams stay selected, with
// normalization enabled and the partition regex, filter config and excluded column applied.
func UpdateSelectedStreams(config *TestConfig, namespace, partitionRegex, filterConfig string, streams []string, columnToExclude string, extraExcluded ...string) error {
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
			stream["normalization"] = true
			stream["partition_regex"] = partitionRegex
			stream["filter_config"] = filter
			for _, excluded := range append([]string{columnToExclude}, extraExcluded...) {
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
		doc["selected_streams"] = map[string]interface{}{namespace: kept}

		for _, entry := range doc["streams"].([]interface{}) {
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
			if !strings.HasSuffix(destinationDB, config.Suite) {
				destinationDB = config.withSuite(destinationDB)
				stream["destination_database"] = destinationDB
			}
			config.DestinationDB = strings.ReplaceAll(destinationDB, ":", "_")
		}
		return nil
	})
}

// ResetStateFile clears state.json so incremental can perform its initial load
// (equivalent to a full load on first run), irrespective of any previous CDC run.
//
// Every call site must keep this BEFORE a stateless (useState=false) sync, which is where they
// all sit today. The version written here is the product's current one (ProductStateVersion), and
// the stateless load that follows overwrites the file with whatever version the binary that ran
// it stamps (protocol/root.go writes state next to --config even with no --state flag).
// The compatibility suite depends on that overwrite: it is how a baseline image's own state version ends
// up pinning the candidate's syncs. Call this after a compatibility run's initial load instead and the
// pipeline is silently promoted to latest semantics -- the suite would pass while testing nothing.
func ResetStateFile(config *TestConfig) error {
	version, err := ProductStateVersion(config.OlakeRootPath)
	if err != nil {
		return err
	}
	return WriteHostFile(config.GetFilePath("state.json"), fmt.Appendf(nil, `{"version": %d}`, version))
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

// KeepTestData reports whether the test suite should keep the source data after a run, for debugging.
func KeepTestData() bool {
	return strings.EqualFold(os.Getenv(KeepTestDataEnvVar), "true")
}

// TestDiscover seeds the source with this driver's test table, runs discover against the driver
// image and asserts the catalog it writes matches the one rendered from streams.template.json exactly.
//
