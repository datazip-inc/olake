package utils

import (
	"context"
	"time"

	"golang.org/x/sync/errgroup"
)

// Concurrent runs execute for every element of array with the given concurrency limit.
func Concurrent[T any](ctx context.Context, array []T, concurrency int, execute func(ctx context.Context, one T, executionNumber int) error) error {
	executor, ctx := errgroup.WithContext(ctx)
	executor.SetLimit(concurrency)

	for idx, one := range array {
		executor.Go(func() error {
			return execute(ctx, one, idx)
		})
	}

	return executor.Wait()
}

// RetryOnBackoff retries f up to attempts times with doubling backoff. This is a simplified copy
// for the tests; the product version additionally logs and honours ErrNonRetryable, which need
// olake-only packages the leaf lib can't import.
func RetryOnBackoff(ctx context.Context, attempts int, sleep time.Duration, f func(ctx context.Context) error) (err error) {
	for cur := range attempts {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err = f(ctx); err == nil {
				return nil
			}
		}
		if attempts > 1 && cur != attempts-1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(sleep):
				sleep = sleep * 2
			}
		}
	}
	return err
}
