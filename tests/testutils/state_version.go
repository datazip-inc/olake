package testutils

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// The product's constants/state-versions.json is the single source of truth for state-file
// semantics: the version the build writes today, and the release history behind every bump. The
// harness holds no copy of its own -- go:embed cannot reach the file from another module, so it is
// read at runtime, once per process.

// StateVersionBaseline is one entry of the manifest's release history: the release that introduced
// a state version, which drivers it gated, and why. The compatibility suite sweeps these tags.
type StateVersionBaseline struct {
	StateVersion int    `json:"state_version"`
	ReleaseTag   string `json:"release_tag"`
	Drivers      string `json:"drivers"`
	Note         string `json:"note"`
}

type stateVersionManifest struct {
	LatestStateVersion int                    `json:"latest_state_version"`
	Baselines          []StateVersionBaseline `json:"baselines"`
}

var (
	stateVersionOnce  sync.Once
	stateVersionValue stateVersionManifest
	stateVersionErr   error
)

func readStateVersionManifest(rootPath string) (stateVersionManifest, error) {
	stateVersionOnce.Do(func() {
		path := filepath.Join(rootPath, "constants", "state-versions.json")
		data, err := os.ReadFile(path)
		if err != nil {
			stateVersionErr = fmt.Errorf("failed to read the product state versions at %s: %w", path, err)
			return
		}
		if err := json.Unmarshal(data, &stateVersionValue); err != nil {
			stateVersionErr = fmt.Errorf("failed to parse %s: %w", path, err)
			return
		}
		if stateVersionValue.LatestStateVersion <= 0 {
			stateVersionErr = fmt.Errorf("%s does not set latest_state_version to a positive integer", path)
			return
		}
		if len(stateVersionValue.Baselines) == 0 {
			stateVersionErr = fmt.Errorf("%s carries no baselines; the compatibility sweep would silently shrink", path)
			return
		}
	})
	return stateVersionValue, stateVersionErr
}

// ProductStateVersion is the state-file version the product writes today.
func ProductStateVersion(rootPath string) (int, error) {
	manifest, err := readStateVersionManifest(rootPath)
	return manifest.LatestStateVersion, err
}

// StateVersionBaselines is the manifest's release history, in file order.
func StateVersionBaselines(rootPath string) ([]StateVersionBaseline, error) {
	manifest, err := readStateVersionManifest(rootPath)
	return manifest.Baselines, err
}
