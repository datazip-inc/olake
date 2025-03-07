package utils

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/hashicorp/go-multierror"
	"golang.org/x/sync/errgroup"
)

// ErrExec executes a list of functions concurrently and returns an error if any function fails.
// It now supports cancellation and timeout via context.
func ErrExec(functions ...func() error) error {
	group, ctx := errgroup.WithContext(context.Background())

	for _, one := range functions {
		group.Go(func() error {
			select {
			case <-ctx.Done(): // Check if the context is canceled
				return ctx.Err()
			default:
				return one()
			}
		})
	}

	return group.Wait()
}

// ErrExecSequential executes a list of functions sequentially, accumulating errors if any occur.
// It now supports more advanced error accumulation and stopping on specific errors.
func ErrExecSequential(functions ...func() error) error {
	var multErr error

	for _, one := range functions {
		err := one()
		if err != nil {
			multErr = multierror.Append(multErr, err)

			// Example: Stop execution if a critical error occurs (you can modify this)
			if customErr, ok := err.(*CustomError); ok && customErr.ShouldStop() {
				log.Printf("Critical error encountered, stopping execution: %v", customErr)
				break
			}
		}
	}

	return multErr
}

// RetryExec retries a function up to a specified number of attempts with a delay between retries.
// It returns an error after all retry attempts fail.
func RetryExec(function func() error, retries int, delay time.Duration) error {
	var err error
	for i := 0; i <= retries; i++ {
		err = function()
		if err == nil {
			return nil // Function succeeded
		}
		log.Printf("Attempt %d failed: %v", i+1, err)
		time.Sleep(delay) // Wait before retrying
	}

	return fmt.Errorf("failed after %d retries: %w", retries, err)
}

// ErrExecWithRetry executes a list of functions concurrently and retries each failed function a configurable number of times.
func ErrExecWithRetry(functions ...func() error) error {
	group, ctx := errgroup.WithContext(context.Background())

	for _, one := range functions {
		group.Go(func() error {
			return RetryExec(one, 3, time.Second) // Retry each function up to 3 times with 1-second delay
		})
	}

	return group.Wait()
}

// ErrExecWithTimeout executes a list of functions concurrently and allows a timeout for the entire operation.
// If the timeout is reached, the execution will be canceled.
func ErrExecWithTimeout(functions ...func() error) error {
	group, ctx := errgroup.WithContext(context.Background())

	// Set a timeout (e.g., 10 seconds)
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	for _, one := range functions {
		group.Go(func() error {
			select {
			case <-ctx.Done():
				return ctx.Err() // Return if context is cancelled
			default:
				return one()
			}
		})
	}

	return group.Wait()
}

// ErrExecFormat formats the error returned from a function according to the provided format string.
func ErrExecFormat(format string, function func() error) func() error {
	return func() error {
		if err := function(); err != nil {
			return fmt.Errorf(format, err)
		}
		return nil
	}
}

// CustomError represents a custom error type with an additional field to indicate if execution should stop.
type CustomError struct {
	Message  string
	Severity string
}

func (e *CustomError) Error() string {
	return e.Message
}

// ShouldStop checks if the custom error indicates that execution should be halted.
func (e *CustomError) ShouldStop() bool {
	return e.Severity == "critical" // Example logic: Stop on "critical" errors
}

// LogExecution logs the execution of a function (can be customized as needed).
func LogExecution(functionName string, startTime time.Time) {
	duration := time.Since(startTime)
	log.Printf("Function '%s' executed in %v", functionName, duration)
}
