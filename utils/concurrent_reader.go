package utils

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/datazip-inc/olake/logger"
	"golang.org/x/sync/errgroup"
)

// Global pool for controlling total concurrent database operations
var ReaderPool struct {
	semaphore chan struct{} // Limits concurrent operations across all streams
	once      sync.Once     // Ensures one-time initialization of the pool

	// The following counters are maintained for monitoring purposes only.
	// They can be exposed via metrics collectors or debugging tools without
	// adding runtime logging overhead.
	active  atomic.Int64 // Tracks currently running operations
	waiting atomic.Int64 // Tracks operations waiting in queue
	total   atomic.Int64 // Counts total completed operations
}

// InitReaderPool initializes the global reader pool with the specified concurrency limit.
// This function should be called during application startup to ensure a consistent
// concurrency limit is used. If not called explicitly, the pool will be initialized
// with the concurrency value from the first ConcurrentReader call.
//
// Important: Only the first initialization (either explicit via this function or implicit
// via ConcurrentReader) will set the concurrency limit. Subsequent calls with different
// concurrency values will have no effect on the pool size.
func InitReaderPool(maxConcurrent int) {
	ReaderPool.once.Do(func() {
		ReaderPool.semaphore = make(chan struct{}, maxConcurrent)
		logger.Infof("Initialized global reader pool with max concurrent: %d", maxConcurrent)
	})
}

// ConcurrentReader executes database read operations with a global concurrency limit
// to prevent database connection overload across multiple streams.
//
// Note: If the global reader pool hasn't been initialized via InitReaderPool(),
// this function will initialize it with the provided concurrency parameter.
// Only the first initialization will take effect.
func ConcurrentReader[T any](ctx context.Context, array []T, concurrency int,
	execute func(ctx context.Context, one T, executionNumber int) error) error {

	// Initialize global pool if needed
	ReaderPool.once.Do(func() {
		ReaderPool.semaphore = make(chan struct{}, concurrency)
		logger.Infof("Initialized global reader pool with max concurrent: %d", concurrency)
	})

	executor, ctx := errgroup.WithContext(ctx)

	for idx, one := range array {
		idx, one := idx, one // capture
		executor.Go(func() error {
			// Increment waiting counter
			ReaderPool.waiting.Add(1)

			// Acquire global semaphore
			select {
			case ReaderPool.semaphore <- struct{}{}:
				// Decrement waiting, increment active
				ReaderPool.waiting.Add(-1)
				ReaderPool.active.Add(1)

				defer func() {
					<-ReaderPool.semaphore
					ReaderPool.active.Add(-1)
					ReaderPool.total.Add(1)
				}()
			case <-ctx.Done():
				ReaderPool.waiting.Add(-1)
				return ctx.Err()
			}

			// Execute with global limit enforced
			return execute(ctx, one, idx+1)
		})
	}

	return executor.Wait()
}
