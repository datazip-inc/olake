package testutils

import (
	"context"
	"fmt"
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
//	driver image (one-time build, if missing) + Σ query <op> + <cmd> run (+ verify) ≈ subtest total

// logPhaseTiming emits one uniformly-formatted timing line. scope is usually the driver name (or a
// shared resource such as "driver-image"); phase names the measured step.
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
	executeQuery func(context.Context, *testing.T, []string, string, bool),
) func(context.Context, *testing.T, []string, string, bool) {
	return func(ctx context.Context, t *testing.T, streams []string, operation string, fileConfig bool) {
		defer trackPhaseTiming(t, driver, fmt.Sprintf("query %q", operation))()
		executeQuery(ctx, t, streams, operation, fileConfig)
	}
}
