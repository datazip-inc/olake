//go:build unix

package destinationtest

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"syscall"
	"time"
)

// lockFile takes an exclusive lock on path, waiting until ctx ends, and returns its release.
// The kernel drops the lock if this process dies, so a killed test never blocks the next one.
func lockFile(ctx context.Context, path string) (func(), error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	fd := file.Fd()
	if fd > math.MaxInt {
		_ = file.Close()
		return nil, fmt.Errorf("lock %s: file descriptor %d is out of range", path, fd)
	}
	for {
		err := syscall.Flock(int(fd), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return func() { _ = file.Close() }, nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) {
			_ = file.Close()
			return nil, fmt.Errorf("lock %s: %w", path, err)
		}
		select {
		case <-ctx.Done():
			_ = file.Close()
			return nil, fmt.Errorf("waiting for lock %s: %w", path, ctx.Err())
		case <-time.After(pollInterval):
		}
	}
}
