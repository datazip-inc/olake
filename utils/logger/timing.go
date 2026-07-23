package logger

import (
	"os"
	"time"
)

// Phase timing for the olake process itself.
//
// Every line is emitted in one greppable shape so a whole run reads as a single timeline:
//
//	[timing] <scope> <phase>: <duration>
//
// Off unless OLAKE_TIMING is set, so a production sync pays nothing for it. The integration
// harness sets it on every container and lifts the lines into the test log, where they sit
// alongside its own `[timing]` spans and break the opaque `sync run` span into phases.
const TimingEnvVar = "OLAKE_TIMING"

var timingEnabled = os.Getenv(TimingEnvVar) != ""

// LogTiming emits one timing line. No-op unless OLAKE_TIMING is set.
func LogTiming(scope, phase string, d time.Duration) {
	if !timingEnabled {
		return
	}
	Infof("[timing] %s %s: %s", scope, phase, d.Round(time.Millisecond))
}

// TrackTiming starts a wall-clock timer and returns a stop func that logs the elapsed span.
// Use `defer logger.TrackTiming(scope, phase)()` to time an enclosing scope, or hold the
// returned func to close a span early.
func TrackTiming(scope, phase string) (stop func()) {
	if !timingEnabled {
		return func() {}
	}
	start := time.Now()
	return func() { LogTiming(scope, phase, time.Since(start)) }
}
