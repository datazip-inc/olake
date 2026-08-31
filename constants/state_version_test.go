package constants

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stateVersionEntry struct {
	StateVersion int    `json:"state_version"`
	ReleaseTag   string `json:"release_tag"`
	Drivers      string `json:"drivers"`
	Note         string `json:"note"`
}

// A published release: vMAJOR.MINOR.PATCH, which is what the driver images are tagged with.
var releaseTagPattern = regexp.MustCompile(`^v\d+\.\d+\.\d+$`)

func loadStateVersions(t *testing.T) (int, []stateVersionEntry) {
	t.Helper()
	var doc struct {
		LatestStateVersion int                 `json:"latest_state_version"`
		Baselines          []stateVersionEntry `json:"baselines"`
	}
	require.NoError(t, json.Unmarshal(rawStateVersions, &doc), "state-versions.json is not valid JSON")
	require.NotEmpty(t, doc.Baselines, "state-versions.json lists no baselines")
	return doc.LatestStateVersion, doc.Baselines
}

func TestStateVersionsAreContiguous(t *testing.T) {
	latest, baselines := loadStateVersions(t)

	seen := make(map[int]string, len(baselines))
	for _, baseline := range baselines {
		previous, duplicate := seen[baseline.StateVersion]
		assert.Falsef(t, duplicate, "state version %d is listed twice, by %s and %s",
			baseline.StateVersion, previous, baseline.ReleaseTag)
		seen[baseline.StateVersion] = baseline.ReleaseTag
	}

	for version := 0; version <= latest; version++ {
		assert.Containsf(t, seen, version,
			"no entry for state version %d; every version from 0 to latest_state_version (%d) needs one", version, latest)
	}

	highest := 0
	for version := range seen {
		if version > highest {
			highest = version
		}
	}
	assert.Equalf(t, latest, highest,
		"latest_state_version is %d but the newest entry is %d; bumping one without the other leaves the build writing a version it cannot describe",
		latest, highest)
}

func TestStateVersionReleaseTagsAreValid(t *testing.T) {
	_, baselines := loadStateVersions(t)

	for _, baseline := range baselines {
		t.Run(fmt.Sprintf("v%d", baseline.StateVersion), func(t *testing.T) {
			assert.Regexpf(t, releaseTagPattern, baseline.ReleaseTag,
				"release_tag %q is not a vMAJOR.MINOR.PATCH release", baseline.ReleaseTag)
		})
	}
}

// Which drivers a bump changed semantics for: "*" for all of them, otherwise a comma separated list
// of driver names. The suite skips a baseline whose bump touched no driver it is testing, so a name
// that matches nothing silently drops that baseline from the sweep.
func TestStateVersionDriversAreKnown(t *testing.T) {
	_, baselines := loadStateVersions(t)

	known := map[string]bool{}
	for _, driver := range []DriverType{MongoDB, Postgres, MySQL, Oracle, DB2, S3, Kafka, MSSQL} {
		known[string(driver)] = true
	}

	for _, baseline := range baselines {
		t.Run(fmt.Sprintf("v%d", baseline.StateVersion), func(t *testing.T) {
			require.NotEmptyf(t, baseline.Drivers,
				"state version %d names no drivers; use \"*\" when a bump changes every driver", baseline.StateVersion)
			if baseline.Drivers == "*" {
				return
			}
			for driver := range strings.SplitSeq(baseline.Drivers, ",") {
				trimmed := strings.TrimSpace(driver)
				assert.NotEmptyf(t, trimmed, "drivers %q has an empty entry", baseline.Drivers)
				assert.Containsf(t, known, trimmed,
					"drivers names %q, which is not a driver; expected \"*\" or a comma separated list of known drivers", trimmed)
			}
		})
	}
}
