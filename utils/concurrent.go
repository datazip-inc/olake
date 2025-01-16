package utils

import (
	"context"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

type CtxFunc = func(ctx context.Context) error

func Concurrent[T any](ctx context.Context, array []T, concurrency int, execute func(ctx context.Context, one T, executionNumber int) error) error {
	executor, ctx := errgroup.WithContext(ctx)
	executor.SetLimit(concurrency)

	for idx, one := range array {
		// schedule an execution
		// hold loop till a slot is available
		executor.Go(func() error {
			return execute(ctx, one, idx+1)
		})
	}

	// block the execution
	return executor.Wait()
}

func ConcurrentF(ctx context.Context, functions ...CtxFunc) error {
	executor, ctx := errgroup.WithContext(ctx)

	for _, one := range functions {
		// schedule an execution
		executor.Go(func() error {
			return one(ctx)
		})
	}

	// block the execution
	return executor.Wait()
}

func ConcurrentC[T any](ctx context.Context, next *Next[T], concurrency int, execute func(ctx context.Context, one T, sequence int64) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	executor, ctx := errgroup.WithContext(ctx)
	executor.SetLimit(concurrency)

	// Channel to signal that all tasks have been scheduled
	done := make(chan struct{})

	go func() {
		defer close(done)
		counter := atomic.Int64{} // execution count to track the executions
		for next.Next() {
			select {
			case <-ctx.Done():
				return
			default:
				one := next.curr
				sequence := counter.Add(1)
				// schedule an execution
				executor.Go(func() error {
					return execute(ctx, one, sequence)
				})
			}
		}
	}()

	// block the execution
	select {
	case <-done:
		if next.err != nil {
			return next.err
		}

		return executor.Wait()
	case <-ctx.Done():
		return executor.Wait()
	}
}

type Next[T any] struct {
	closed bool
	err    error
	curr   T
	next   func(curr T) (exit bool, one T, err error)
}

func (n *Next[T]) Next() bool {
	exit, next, err := n.next(n.curr)
	if err != nil {
		n.err = err
		return false
	}
	if exit {
		return false
	}

	n.curr = next
	return true
}

func (n *Next[T]) Close() {
	n.closed = true
}

func Yield[T any](next func(prev T) (bool, T, error)) *Next[T] {
	return &Next[T]{
		next: next,
	}
}

type CxGroup struct {
	ctx      context.Context
	executor *errgroup.Group
}

func NewCGroup(ctx context.Context) *CxGroup {
	return newCGroup(ctx, 0)
}

func NewCGroupWithLimit(ctx context.Context, limit int) *CxGroup {
	return newCGroup(ctx, limit)
}

func newCGroup(ctx context.Context, limit int) *CxGroup {
	group := &CxGroup{}
	group.executor, group.ctx = errgroup.WithContext(ctx)
	if limit > 0 {
		group.executor.SetLimit(limit)
	}

	return group
}

func (g *CxGroup) Add(execute func(ctx context.Context) error) {
	g.executor.Go(func() error {
		return execute(g.ctx)
	})
}

func (g *CxGroup) Block() error {
	return g.executor.Wait()
}

func ConcurrentInGroup[T any](group *CxGroup, array []T, execute func(ctx context.Context, one T) error) {
	for _, one := range array {
		// schedule an execution
		group.Add(func(ctx context.Context) error {
			return execute(ctx, one)
		})
	}
}
