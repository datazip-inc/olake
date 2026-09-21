package compatibility

// Release tags and the baseline floor.
//
// Every gate and every dated rule is keyed on a release tag, so the whole suite needs one way to
// read "vX.Y.Z" and order two of them. A baseline that is not a release -- a commit, an image ref --
// is undatable here, which is what makes every dated rule read it as newest.

import (
	"os/exec"
	"strconv"
	"strings"

	"github.com/datazip-inc/olake/tests/testutils"
)

func releaseTagLess(a, b string) bool {
	av, aok := parseReleaseTag(a)
	bv, bok := parseReleaseTag(b)
	return aok && bok && compareRelease(av, bv) < 0
}

// equivalentRelease is the release a commit baseline reads the gates and rules as: the newest release
// tag reachable from it. "" when none is, which leaves the commit undated.
func equivalentRelease(rootPath, commitID string) string {
	out, err := exec.Command("git", "-C", rootPath, "tag", "--list", "--merged", commitID, "v*").Output()
	if err != nil {
		return ""
	}
	var newest string
	var newestVersion [3]int
	for tag := range strings.FieldsSeq(string(out)) {
		if version, ok := parseReleaseTag(tag); ok && (newest == "" || compareRelease(version, newestVersion) > 0) {
			newest, newestVersion = tag, version
		}
	}
	return newest
}

// parseReleaseTag reads "vX.Y.Z" (optionally behind a "repo:tag" prefix) into a comparable triple;
// ok is false for anything that is not a release tag (a sha, "latest", a bare image).
func parseReleaseTag(spec string) ([3]int, bool) {
	var version [3]int
	if i := strings.LastIndex(spec, ":"); i >= 0 {
		spec = spec[i+1:]
	}
	parts := strings.Split(strings.TrimPrefix(strings.TrimSpace(spec), "v"), ".")
	if len(parts) != 3 {
		return version, false
	}
	for i, part := range parts {
		n, err := strconv.Atoi(part)
		if err != nil || n < 0 {
			return version, false
		}
		version[i] = n
	}
	return version, true
}

func compareRelease(a, b [3]int) int {
	for i := range a {
		switch {
		case a[i] < b[i]:
			return -1
		case a[i] > b[i]:
			return 1
		}
	}
	return 0
}

// compatibilityGlobalFloor is the oldest baseline the suite runs for any driver: the oldest entry in the
// product's state-versions.json. Derived rather than restated, so adding or retiring a baseline
// moves the floor with it.
func compatibilityGlobalFloor() (string, error) {
	versionBumps, err := testutils.StateVersionBaselines()
	if err != nil {
		return "", err
	}
	oldest := versionBumps[0]
	for _, bump := range versionBumps[1:] {
		if bump.StateVersion < oldest.StateVersion {
			oldest = bump
		}
	}
	return oldest.ReleaseTag, nil
}
