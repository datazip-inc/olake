package compatibility

// Release tags and the baseline floor.
//
// Every gate and every dated rule is keyed on a release tag, read and ordered by semver. A baseline
// that is not a release -- a commit, an image ref -- is undatable here, which is what makes every
// dated rule read it as newest.

import (
	"cmp"
	"os/exec"
	"slices"
	"strings"

	"github.com/datazip-inc/olake/tests/testutils"
	"golang.org/x/mod/semver"
)

// equivalentRelease is the release a commit baseline reads the gates and rules as: the newest release
// tag reachable from it. "" when none is, which leaves the commit undated.
func equivalentRelease(rootPath, commitID string) string {
	out, err := exec.Command("git", "-C", rootPath, "tag", "--list", "--merged", commitID, "v*").Output()
	if err != nil {
		return ""
	}
	newest := ""
	for tag := range strings.FieldsSeq(string(out)) {
		if semver.IsValid(tag) && semver.Compare(tag, newest) > 0 {
			newest = tag
		}
	}
	return newest
}

// compatibilityGlobalFloor is the oldest baseline the suite runs for any driver: the oldest entry in the
// product's state-versions.json. Derived rather than restated, so adding or retiring a baseline
// moves the floor with it.
func compatibilityGlobalFloor() (string, error) {
	versionBumps, err := testutils.StateVersionBaselines()
	if err != nil {
		return "", err
	}
	oldest := slices.MinFunc(versionBumps, func(a, b testutils.StateVersionBaseline) int {
		return cmp.Compare(a.StateVersion, b.StateVersion)
	})
	return oldest.ReleaseTag, nil
}
