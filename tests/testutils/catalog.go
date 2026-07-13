package testutils

import (
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The helpers below edit the driver's config/catalog files on the host; the driver
// container sees the changes through the /testdata mount. They replace the jq edits the
// old harness ran inside the test container.

// editJSONFile reads path, applies edit to the decoded document, and writes it back.
func editJSONFile(path string, edit func(doc map[string]interface{}) error) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read %s: %s", path, err)
	}
	var doc map[string]interface{}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return fmt.Errorf("failed to parse %s: %s", path, err)
	}
	if err := edit(doc); err != nil {
		return err
	}
	out, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal %s: %s", path, err)
	}
	return os.WriteFile(path, out, 0644)
}

// normalizeStreamName uppercases the stream name for drivers whose catalogs store
// uppercase identifiers (e.g. Oracle).
func normalizeStreamName(driver, streamName string) string {
	return ternary(slices.Contains(skipCDCDrivers, driver), strings.ToUpper(streamName), streamName)
}

// updateSelectedStreams rewrites selected_streams so only the given streams (by name,
// under namespace) stay selected, each with normalization enabled and the partition
// regex, filter config and excluded column applied.
func updateSelectedStreams(config *TestConfig, namespace, partitionRegex, filterConfig string, streams []string, columnToExclude string) error {
	if len(streams) == 0 {
		return nil
	}
	selectedNames := make(map[string]bool, len(streams))
	for _, s := range streams {
		selectedNames[normalizeStreamName(config.Driver, s)] = true
	}

	var filter interface{} = map[string]interface{}{}
	if filterConfig != "" {
		if err := json.Unmarshal([]byte(filterConfig), &filter); err != nil {
			return fmt.Errorf("failed to parse filter config: %s", err)
		}
	}

	return editJSONFile(config.HostCatalogPath, func(doc map[string]interface{}) error {
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
			if columnToExclude != "" {
				if selectedColumns, ok := stream["selected_columns"].(map[string]interface{}); ok {
					if columns, ok := selectedColumns["columns"].([]interface{}); ok {
						remaining := make([]interface{}, 0, len(columns))
						for _, col := range columns {
							if fmt.Sprint(col) != columnToExclude {
								remaining = append(remaining, col)
							}
						}
						selectedColumns["columns"] = remaining
					}
				}
			}
			kept = append(kept, stream)
		}
		doc["selected_streams"] = map[string]interface{}{namespace: kept}
		return nil
	})
}

// updateStreamConfig sets sync_mode and cursor_field on the stream identified by
// namespace+name in streams[].
func updateStreamConfig(config *TestConfig, namespace, streamName, syncMode, cursorField string) error {
	streamName = normalizeStreamName(config.Driver, streamName)
	return editJSONFile(config.HostCatalogPath, func(doc map[string]interface{}) error {
		streams, _ := doc["streams"].([]interface{})
		for _, raw := range streams {
			wrapper, ok := raw.(map[string]interface{})
			if !ok {
				continue
			}
			stream, ok := wrapper["stream"].(map[string]interface{})
			if !ok {
				continue
			}
			if stream["namespace"] == namespace && stream["name"] == streamName {
				stream["sync_mode"] = syncMode
				stream["cursor_field"] = cursorField
			}
		}
		return nil
	})
}

// toggleArrowIcebergWrites flips writer.arrow_writes in the iceberg destination config.
func toggleArrowIcebergWrites(config *TestConfig, enabled bool) error {
	return editJSONFile(config.HostIcebergDestPath, func(doc map[string]interface{}) error {
		writer, ok := doc["writer"].(map[string]interface{})
		if !ok {
			return fmt.Errorf("no writer object in %s", config.HostIcebergDestPath)
		}
		writer["arrow_writes"] = enabled
		return nil
	})
}

// resetStateFile clears state.json so incremental can perform its initial load
// (equivalent to a full load on first run), irrespective of any previous CDC run.
func resetStateFile(config *TestConfig) error {
	return os.WriteFile(config.HostStatePath, []byte("{}"), 0644)
}

func copyFile(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return fmt.Errorf("failed to read %s: %s", src, err)
	}
	return os.WriteFile(dst, data, 0644)
}

// saveStateFile copies state.json to the checkpoint state file.
func saveStateFile(config *TestConfig) error {
	return copyFile(config.HostStatePath, config.HostStateCheckpointPath)
}

// restoreStateFile replaces state.json with the previously saved checkpoint backup.
func restoreStateFile(config *TestConfig) error {
	return copyFile(config.HostStateCheckpointPath, config.HostStatePath)
}

// seedCatalog copies the expected test_streams.json into streams.json so sync-oriented
// tests don't depend on a prior discover run (the schema itself is validated by TestDiscover).
func seedCatalog(t *testing.T, config *TestConfig) {
	t.Helper()
	testStreamsData, err := os.ReadFile(config.HostTestCatalogPath)
	require.NoError(t, err, "failed to read test_streams.json")
	require.NoError(t, os.WriteFile(config.HostCatalogPath, testStreamsData, 0600), "failed to write streams.json")
}
