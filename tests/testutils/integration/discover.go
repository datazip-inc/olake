package integration

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"slices"
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/require"
)

// Its caller must not be parallel. The compare is an equality one, so it only holds while this
// table is the only thing in the source -- and every other suite seeds one of its own. Leaving the
// test serial is what orders it ahead of them: Go resumes parallel tests only once the serial ones
// in the package are done.
func (cfg *Test) TestDiscover(t *testing.T) {
	ctx := t.Context()

	// 1. Empty the source, then seed just this table. drop-all is what makes the compare below an
	// equality one: discover enumerates everything, so anything an aborted run (or a perf seed)
	// left behind would show up as an extra stream. Safe only here -- the discover suite runs
	// alone, while every parallel suite owns a table drop-all would take with it.
	cfg.TestConfig.ExecuteQuery(ctx, t, cfg.TestConfig, "drop-all")
	cfg.TestConfig.ExecuteQuery(ctx, t, cfg.TestConfig, "create")
	cfg.TestConfig.ExecuteQuery(ctx, t, cfg.TestConfig, "add")
	// Deferred, so a failed discover still hands the parallel suites behind it a clean source.
	defer func() {
		if testutils.KeepTestData() {
			t.Logf("keeping %s discover data in source as (%s) is set", cfg.TestConfig.Driver, testutils.KeepTestDataEnvVar)
		} else {
			cfg.TestConfig.ExecuteQuery(ctx, t, cfg.TestConfig, "drop")
		}
	}()

	// 2. Stage what discover has to reproduce before it writes its own streams.json next to the config
	generateExpectedStreams(t, cfg.TestConfig)

	// 3. Run discover against the driver image
	code, out, err := testutils.RunOlake(ctx, cfg.TestConfig, testutils.DiscoverArgs()...)
	if err != nil || code != 0 {
		t.Fatal(testutils.RenderOlakeFailure(code, err, out))
	}

	verifyDiscoveredStreams(t, cfg.GetFilePath("expected_discover_streams.json"), cfg.GetFilePath("streams.json"))
}

// generateExpectedStreams writes the catalog discover has to return: the one applySuite already
// rendered from streams.template.json, under a name discover's own output cannot overwrite.
func generateExpectedStreams(t *testing.T, config *testutils.TestConfig) {
	t.Helper()
	require.NoError(t, testutils.CopyFile(config.GetFilePath("streams.json"), config.GetFilePath("expected_discover_streams.json")),
		"failed to generate the expected discover catalog")
}

// verifyDiscoveredStreams asserts the discovered catalog holds exactly the streams expected_streams.json
func verifyDiscoveredStreams(t *testing.T, expectedPath, actualPath string) {
	t.Helper()

	load := func(path, what string) map[string]interface{} {
		data, err := os.ReadFile(path)
		require.NoError(t, err, "failed to read %s streams JSON (%s)", what, path)
		var doc map[string]interface{}
		require.NoError(t, json.Unmarshal(data, &doc), "failed to parse %s streams JSON (%s)", what, path)
		return doc
	}
	expected := load(expectedPath, "expected")
	actual := load(actualPath, "discovered")

	// streams[]: keyed by namespace.name, which is what makes a stream unique in a catalog.
	indexStreams := func(doc map[string]interface{}) map[string]interface{} {
		out := map[string]interface{}{}
		entries, _ := doc["streams"].([]interface{})
		for _, raw := range entries {
			wrapper, ok := raw.(map[string]interface{})
			if !ok {
				continue
			}
			stream, ok := wrapper["stream"].(map[string]interface{})
			if !ok {
				continue
			}
			out[fmt.Sprintf("%v.%v", stream["namespace"], stream["name"])] = wrapper
		}
		return out
	}
	// selected_streams: a map of namespace -> []{stream_name, ...}; key the same way.
	indexSelected := func(doc map[string]interface{}) map[string]interface{} {
		out := map[string]interface{}{}
		byNamespace, _ := doc["selected_streams"].(map[string]interface{})
		for namespace, raw := range byNamespace {
			entries, _ := raw.([]interface{})
			for _, entry := range entries {
				selected, ok := entry.(map[string]interface{})
				if !ok {
					continue
				}
				out[fmt.Sprintf("%v.%v", namespace, selected["stream_name"])] = selected
			}
		}
		return out
	}

	compare := func(section string, want, got map[string]interface{}) {
		require.Equal(t, slices.Sorted(maps.Keys(want)), slices.Sorted(maps.Keys(got)),
			"%s: discover returned a different set of streams than expected_streams.json", section)
		for key, wantEntry := range want {
			wantJSON, err := json.Marshal(wantEntry)
			require.NoError(t, err)
			gotJSON, err := json.Marshal(got[key])
			require.NoError(t, err)
			require.Truef(t, testutils.NormalizedEqual(string(wantJSON), string(gotJSON)),
				"%s: discovered %q does not match expected_streams.json\nExpected:\n%s\nGot:\n%s", section, key, wantJSON, gotJSON)
		}
	}
	compare("streams", indexStreams(expected), indexStreams(actual))
	compare("selected_streams", indexSelected(expected), indexSelected(actual))

	t.Logf("Generated streams validated with test streams")
}
