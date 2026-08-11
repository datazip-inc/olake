package logger

import (
	"os"
	"time"
)

// TimingEnvVar gates phase timing, emitted as `[timing] <scope> <phase>: <duration>`. Unset -- the
// default -- a sync pays nothing for it; the integration harness sets it on every container.
const TimingEnvVar = "OLAKE_TIMING"

var timingEnabled = os.Getenv(TimingEnvVar) != ""

// LogTiming emits one timing line. No-op unless OLAKE_TIMING is set.
func LogTiming(scope, phase string, d time.Duration) {
	if !timingEnabled {
		return
	}
	Infof("[timing] %s %s: %s", scope, phase, d.Round(time.Millisecond))
}

// TrackTiming starts a wall-clock timer and returns a stop func that logs the elapsed span. Use
// `defer TrackTiming(scope, phase)()`, or hold the returned func to close a span early.
func TrackTiming(scope, phase string) (stop func()) {
	if !timingEnabled {
		return func() {}
	}
	start := time.Now()
	return func() { LogTiming(scope, phase, time.Since(start)) }
}
