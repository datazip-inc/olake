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

func TestSignalAwareRootContextCancelsOnSIGTERM(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("process signal behavior differs on windows")
	}

	if os.Getenv("OLAKE_SIGNAL_CONTEXT_HELPER") == "1" {
		ctx := signalAwareRootContext(context.Background())
		proc, err := os.FindProcess(os.Getpid())
		if err != nil {
			t.Fatal(err)
		}
		if err := proc.Signal(syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}

		select {
		case <-ctx.Done():
			if ctx.Err() != context.Canceled {
				t.Fatalf("expected canceled context, got %v", ctx.Err())
			}
			return
		case <-time.After(time.Second):
			t.Fatal("context was not canceled after SIGTERM")
		}
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestSignalAwareRootContextCancelsOnSIGTERM")
	cmd.Env = append(os.Environ(), "OLAKE_SIGNAL_CONTEXT_HELPER=1")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("signal helper failed: %v\n%s", err, output)
	}
}

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
