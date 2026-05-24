package protocol

import (
	"context"
	"os"
	"os/exec"
	"runtime"
	"syscall"
	"testing"
	"time"
)

// TestSignalAwareRootContextCancelsOnSignal verifies that signalAwareRootContext
// cancels the returned context when the process receives SIGINT or SIGTERM.
//
// The test re-execs itself in a child process per signal case so that we can
// safely deliver a real OS signal without affecting the parent test runner.
// The OLAKE_SIGNAL_CONTEXT_HELPER env var switches the binary into "helper"
// mode; OLAKE_TEST_SIGNAL selects which signal the helper sends to itself.
func TestSignalAwareRootContextCancelsOnSignal(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("process signal behavior differs on windows")
	}

	tests := []struct {
		name   string
		signal syscall.Signal
		env    string
	}{
		{
			name:   "SIGTERM cancellation",
			signal: syscall.SIGTERM,
			env:    "SIGTERM",
		},
		{
			name:   "SIGINT cancellation",
			signal: syscall.SIGINT,
			env:    "SIGINT",
		},
	}

	// Helper-mode branch: runs inside the re-execed child. Installs the
	// signal handler under test, sends the chosen signal to ourselves, and
	// asserts the context cancels with context.Canceled within a bounded
	// timeout.
	if os.Getenv("OLAKE_SIGNAL_CONTEXT_HELPER") == "1" {
		ctx := signalAwareRootContext(context.Background())

		var signal syscall.Signal

		switch os.Getenv("OLAKE_TEST_SIGNAL") {
		case "SIGTERM":
			signal = syscall.SIGTERM
		case "SIGINT":
			signal = syscall.SIGINT
		default:
			t.Fatal("unknown test signal")
		}

		currentProcess, err := os.FindProcess(os.Getpid())
		if err != nil {
			t.Fatal(err)
		}

		if err := currentProcess.Signal(signal); err != nil {
			t.Fatal(err)
		}

		select {
		case <-ctx.Done():
			if ctx.Err() != context.Canceled {
				t.Fatalf("expected canceled context, got %v", ctx.Err())
			}
		case <-time.After(time.Second):
			t.Fatalf("context was not canceled after %v", signal)
		}

		return
	}

	// Parent-mode branch: spawns one helper child per signal case and fails
	// the subtest if the child exits non-zero.
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=TestSignalAwareRootContextCancelsOnSignal")
			cmd.Env = append(os.Environ(), "OLAKE_SIGNAL_CONTEXT_HELPER=1", "OLAKE_TEST_SIGNAL="+tt.env)

			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("signal helper failed: %v\n%s", err, output)
			}
		})
	}
}

// TestSignalAwareRootContextPreservesParentCancellation verifies that
// canceling the parent context still propagates through the signal-aware
// wrapper. Without this, callers that cancel via context.WithCancel /
// context.WithTimeout would be silently ignored after the wrap.
func TestSignalAwareRootContextPreservesParentCancellation(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	ctx := signalAwareRootContext(parent)
	cancel()

	select {
	case <-ctx.Done():
		if ctx.Err() != context.Canceled {
			t.Fatalf("expected canceled context, got %v", ctx.Err())
		}
	case <-time.After(time.Second):
		t.Fatal("context was not canceled after parent cancellation")
	}
}
