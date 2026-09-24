package testutils

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var (
	stateVersionOnce  sync.Once
	stateVersionValue stateVersionManifest
	stateVersionErr   error
)

type stateVersionManifest struct {
	LatestStateVersion int                    `json:"latest_state_version"`
	Baselines          []StateVersionBaseline `json:"baselines"`
}

type StateVersionBaseline struct {
	StateVersion int    `json:"state_version"`
	ReleaseTag   string `json:"release_tag"`
	Drivers      string `json:"drivers"`
	Note         string `json:"note"`
}

// Gates reports whether this release's bump changed driver's semantics: the manifest names the
// drivers it touched, comma-separated, or "*" for all.
func (b StateVersionBaseline) Gates(driver string) bool {
	for gated := range strings.SplitSeq(b.Drivers, ",") {
		if gated = strings.TrimSpace(gated); gated == "*" || gated == driver {
			return true
		}
	}
	return false
}

// stateVersionManifestPath names the manifest file, for messages that point readers at it
func stateVersionManifestPath() (string, error) {
	root, err := RepoRoot()
	if err != nil {
		return "", fmt.Errorf("failed to locate the repo holding the product state versions: %w", err)
	}
	return filepath.Join(root, "constants", "state-versions.json"), nil
}

func readStateVersionManifest() (stateVersionManifest, error) {
	stateVersionOnce.Do(func() {
		path, err := stateVersionManifestPath()
		if err != nil {
			stateVersionErr = err
			return
		}
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

// LatestStateVersion is the state-file version the product writes today.
func LatestStateVersion() (int, error) {
	manifest, err := readStateVersionManifest()
	return manifest.LatestStateVersion, err
}

// StateVersionBaselines is the manifest's release history, in file order.
func StateVersionBaselines() ([]StateVersionBaseline, error) {
	manifest, err := readStateVersionManifest()
	return manifest.Baselines, err
}
