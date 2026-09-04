package compatibility

// What a failed compatibility run reports.
//
// The evidence a run produces -- the columns that differ, the rows only one side has -- is found
// deep inside the variant subtests, and Go attributes it to them. The assertion CI shows in red is
// the one at the end of runCompatibilityBaseline, which knew only that something below it had
// failed. The two types here carry the evidence back up to it: diagnostics collects a variant's
// findings as they are produced, and failureReport gathers every failed variant into the single
// message that assertion reports.

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
)

// diagnostics is one variant's account of its own failure: the reason it stopped, and whatever
// detail was gathered before it did.
type diagnostics struct {
	mu     sync.Mutex
	reason string
	lines  []string
}

// logf logs a line of detail, in the red a failure is reported in, and keeps a plain copy. The
// subtest's own output remains the fuller story -- it carries the passing variants too -- but
// everything logged through here also survives into the final report, which paints itself.
func (d *diagnostics) logf(t *testing.T, format string, args ...any) {
	t.Helper()
	line := fmt.Sprintf(format, args...)
	t.Log(testutils.Red(line))
	d.mu.Lock()
	defer d.mu.Unlock()
	d.lines = append(d.lines, line)
}

// fatalf records why the variant stopped and then stops it. The reason leads the report, ahead of
// the detail, however late it was discovered.
func (d *diagnostics) fatalf(t *testing.T, format string, args ...any) {
	t.Helper()
	line := fmt.Sprintf(format, args...)
	d.mu.Lock()
	d.reason = line
	d.mu.Unlock()
	t.Fatal(testutils.Red(line))
}

func (d *diagnostics) collected() (string, []string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.reason, d.lines
}

// variantFailure is one broken variant of one writer group.
type variantFailure struct {
	group, variant string
	reason         string
	detail         []string
}

// failureReport accumulates the variants that failed one baseline and renders the message the
// run's final assertion reports. Written from the group goroutines, which run in parallel.
type failureReport struct {
	mu        sync.Mutex
	driver    string
	spec      string
	baseline  string // the image both sides start on
	candidate string // the image the upgrade side hands off to
	failures  []variantFailure
}

func (r *failureReport) add(group, variant string, d *diagnostics) {
	reason, detail := d.collected()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failures = append(r.failures, variantFailure{group: group, variant: variant, reason: reason, detail: detail})
}

// render is the whole of what CI shows in red.
//
// It opens with the state version, in capitals, because that is the thing a reader has to reason
// about: "v0.3.15 failed" names the release we compared against, not what about it matters, while
// the state version IS the contract under test -- it is what a state file written by that release
// tells this build to honor. The manifest's note for the bump says what that meant, then every
// failed variant with the detail it gathered, then what the two runs actually were.
func (r *failureReport) render(rootPath string) string {
	r.mu.Lock()
	defer r.mu.Unlock()

	headline, note := r.headline(rootPath)
	var b strings.Builder
	fmt.Fprintf(&b, "\n%s\n", headline)
	if note != "" {
		fmt.Fprintf(&b, "what that state version changed: %s\n", note)
	}

	fmt.Fprintf(&b, "\n%s failed:\n", plural(len(r.failures), "scenario"))
	for _, f := range r.failures {
		fmt.Fprintf(&b, "\n  %s/%s\n", f.group, f.variant)
		if f.reason != "" {
			fmt.Fprintf(&b, "%s\n", indent(f.reason, "    "))
		}
		for _, line := range f.detail {
			fmt.Fprintf(&b, "%s\n", indent(line, "    "))
		}
		if f.reason == "" && len(f.detail) == 0 {
			fmt.Fprintf(&b, "    failed before the two destinations could be compared; its own subtest output has why\n")
		}
	}

	fmt.Fprintf(&b, "\nreference run: every sync on %s\n", r.baseline)
	fmt.Fprintf(&b, "upgrade run:   the stateless load on %s, every sync after it on %s\n", r.baseline, r.candidate)
	return b.String()
}

// headline names the state version the baseline introduced, in capitals, and returns the
// manifest's note for it. A baseline that is not a release in the manifest -- a commit, an image
// ref, the base branch a pull request merges into -- has no state version to name, so it says what
// it does have.
func (r *failureReport) headline(rootPath string) (string, string) {
	if baselines, err := testutils.StateVersionBaselines(rootPath); err == nil {
		for _, b := range baselines {
			if b.ReleaseTag == r.spec {
				return fmt.Sprintf("STATE VERSION %d FAILED for %s -- baseline %s is the release that introduced it",
					b.StateVersion, r.driver, b.ReleaseTag), b.Note
			}
		}
	}
	// No state version to name, so name the one this build reads it with: that is still the half
	// of the contract the reader can act on.
	if current, err := testutils.ProductStateVersion(rootPath); err == nil {
		return fmt.Sprintf("BASELINE %s FAILED for %s -- state written by that build is not read the same way by this one, which is at state version %d",
			r.spec, r.driver, current), ""
	}
	return fmt.Sprintf("BASELINE %s FAILED for %s", r.spec, r.driver), ""
}

// indent prefixes every line, not just the first: a message that carries its own detail (a schema
// diff, a row dump) is several lines long and reads as one block only if all of them move.
func indent(s, prefix string) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	for i, line := range lines {
		lines[i] = prefix + line
	}
	return strings.Join(lines, "\n")
}

func plural(n int, noun string) string {
	if n == 1 {
		return fmt.Sprintf("1 %s", noun)
	}
	return fmt.Sprintf("%d %ss", n, noun)
}
