package backoff

import (
	"time"
)

// Retry executes f up to attempts times with exponential backoff starting at sleep.
// It returns nil on the first successful attempt, or the last error if all attempts fail.
// The shouldRetry predicate decides whether a given error is retryable; if it returns false,
// Retry stops immediately and returns that error.
func Retry(attempts int, sleep time.Duration, f func() error, shouldRetry func(error) bool) error {
	if attempts < 1 {
		attempts = 1
	}
	if sleep <= 0 {
		sleep = time.Second
	}
	var lastErr error
	for cur := 0; cur < attempts; cur++ {
		err := f()
		if err == nil {
			return nil
		}
		lastErr = err
		if !shouldRetry(err) {
			return err
		}
		if cur != attempts-1 {
			time.Sleep(sleep)
			sleep *= 2
		}
	}
	return lastErr
}
