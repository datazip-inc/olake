package utils

import (
	"context"
	"fmt"

	"github.com/hashicorp/go-multierror"
	"golang.org/x/sync/errgroup"
)

// ErrExec executes multiple functions concurrently and returns the first error encountered.
// It ensures proper cancellation of remaining goroutines if an error occurs.
func ErrExec(functions ...func() error) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Ensure cancellation in case of early return

	group, ctx := errgroup.WithContext(ctx)

	for _, fn := range functions {
		fn := fn // Avoid closure variable reuse issue
		group.Go(func() error {
			select {
			case <-ctx.Done():
				return ctx.Err() // Propagate cancellation error if needed
			default:
				return fn()
			}
		})
	}

	return group.Wait()
}

// ErrExecSequential executes multiple functions sequentially and collects all errors.
func ErrExecSequential(functions ...func() error) error {
	var result error
	for _, fn := range functions {
		if err := fn(); err != nil {
			result = multierror.Append(result, err)
		}
	}
	return result
}

// ErrExecFormat wraps a function execution with a formatted error message if it fails.
func ErrExecFormat(format string, function func() error) func() error {
	return func() error {
		if err := function(); err != nil {
			return fmt.Errorf(format+": %w", err) // Use %w for proper error wrapping
		}
		return nil
	}
}
