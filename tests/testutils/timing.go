package testutils

import (
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

// TrackPhaseTiming starts a wall-clock timer and returns a stop func that logs the elapsed span.
// Call stop() when the phase ends, or `defer TrackPhaseTiming(t, scope, phase)()` to time a scope
// (the deferred form also captures the duration when the body t.Fatal/Goexits).
func TrackPhaseTiming(t *testing.T, scope, phase string) (stop func()) {
	start := time.Now()
	return func() { logPhaseTiming(t, scope, phase, time.Since(start)) }
}
