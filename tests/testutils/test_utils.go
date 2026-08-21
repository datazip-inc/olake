package testutils

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/datazip-inc/olake/tests/testutils/constants"
)

const (
	SyncTimeout = 10 * time.Minute

	KeepTestDataEnvVar = "OLAKE_TEST_KEEP_DATA"
)

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

	return EditJSONFile(config.GetFilePath("test_streams.json"), func(doc map[string]interface{}) error {
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

		// A baked destination_database is used verbatim (it overrides the prefix flag), so suffix
		// it or concurrent suites race the CREATE on one shared namespace.
		suffix := Combine(config.DataFormat, config.Suite)
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
			if !ok || destinationDB == "" || strings.HasSuffix(destinationDB, suffix) {
				continue
			}
			stream["destination_database"] = config.withSuite(destinationDB)
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

// ProductStateVersion reads the product's state version straight out of its state-versions.json,
// once per process. The harness holds no copy of its own: go:embed cannot reach the file from
// another package (parent paths and symlinks are both rejected), so the tests read it at runtime.
func ProductStateVersion(rootPath string) (int, error) {
	stateVersionOnce.Do(func() {
		path := filepath.Join(rootPath, "constants", "state-versions.json")
		data, err := os.ReadFile(path)
		if err != nil {
			stateVersionErr = fmt.Errorf("failed to read the product state version at %s: %w", path, err)
			return
		}
		var doc struct {
			LatestStateVersion int `json:"latest_state_version"`
		}
		if err := json.Unmarshal(data, &doc); err != nil {
			stateVersionErr = fmt.Errorf("failed to parse %s: %w", path, err)
			return
		}
		if doc.LatestStateVersion <= 0 {
			stateVersionErr = fmt.Errorf("%s does not set latest_state_version to a positive integer", path)
			return
		}
		stateVersionValue = doc.LatestStateVersion
	})
	return stateVersionValue, stateVersionErr
}

var (
	stateVersionOnce  sync.Once
	stateVersionValue int
	stateVersionErr   error
)

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
// SyncFailure formats a failed sync's exit, translating the one code worth translating: 137 is
// SIGKILL, which in this harness almost always means the --memory cap or the docker VM OOM-killed
// the container -- raise Docker's memory, or shrink OLAKE_TEST_CONTAINER_MEMORY /
// OLAKE_TEST_WRITER_HEAP when many syncs run concurrently.
func SyncFailure(code int, err error, out []byte) error {
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
// image and asserts the catalog it writes matches test_streams.json exactly.
//
