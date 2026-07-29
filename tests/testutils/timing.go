package testutils

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Integration-test timing instrumentation.
//
// Every `[timing]` line the harness emits flows through logPhaseTiming, so the format stays
// uniform and greppable:
//
//	[timing] <scope> <phase>: <duration>
//
// The phases are designed to nest to a subtest's wall-clock total, so a slow run can be attributed
// rather than guessed at:
//
//	base image + container ready + in-container work ≈ subtest total
//	in-container work            ≈ Σ query <op> + <phase> build + <phase> run (+ verify)

// logPhaseTiming emits one uniformly-formatted timing line. scope is usually the driver name (or a
// shared resource such as "base-image"); phase names the measured step.
func logPhaseTiming(t *testing.T, scope, phase string, d time.Duration) {
	t.Helper()
	t.Logf("[timing] %s %s: %s", scope, phase, d.Round(time.Millisecond))
}

// trackPhaseTiming starts a wall-clock timer and returns a stop func that logs the elapsed span.
// Call stop() when the phase ends, or `defer trackPhaseTiming(t, scope, phase)()` to time a scope
// (the deferred form also captures the duration when the body t.Fatal/Goexits).
func trackPhaseTiming(t *testing.T, scope, phase string) (stop func()) {
	start := time.Now()
	return func() { logPhaseTiming(t, scope, phase, time.Since(start)) }
}

// timedExecuteQuery wraps IntegrationTest.ExecuteQuery so every source-DB operation
// (create/clean/add/drop/evolve-schema/...) is timed by name. These run against the source and,
// for CDC engines like MSSQL (capture-instance enablement + readiness polling), are a real slice
// of wall-clock that would otherwise fall through the cracks between the other phase timings.
func timedExecuteQuery(
	driver string,
	executeQuery func(context.Context, *testing.T, []string, string, bool, string),
) func(context.Context, *testing.T, []string, string, bool, string) {
	return func(ctx context.Context, t *testing.T, streams []string, operation string, fileConfig bool, suite string) {
		defer trackPhaseTiming(t, driver, fmt.Sprintf("query %q", operation))()
		executeQuery(ctx, t, streams, operation, fileConfig, suite)
	}
}

// build.sh's `timed <MARKER> <cmd>` helper brackets its in-container `go mod tidy`, `go build`
// and `./olake` run with OLAKE_<MARKER>_MS=<elapsed> markers so the harness can attribute those
// spans separately — the single ExecCommand call that triggers build.sh only sees one combined
// duration.
var buildRunMsRe = regexp.MustCompile(`OLAKE_([A-Z]+)_MS=(\d+)`)

// logBuildRunTimings splits build.sh's output into "<phase> <marker>" timings ("discover tidy",
// "iceberg sync build", ...). Absent markers (e.g. build.sh run under BSD date) are simply
// skipped, so callers can invoke it unconditionally.
func logBuildRunTimings(t *testing.T, driver, phase string, out []byte) {
	t.Helper()
	for _, m := range buildRunMsRe.FindAllSubmatch(out, -1) {
		ms, _ := strconv.Atoi(string(m[2]))
		logPhaseTiming(t, driver, phase+" "+strings.ToLower(string(m[1])), time.Duration(ms)*time.Millisecond)
	}
}
