package jdbc

import (
	"context"
	"fmt"
	"strings"

	"github.com/datazip-inc/olake/types"
)

type Reader[T types.Iterable] struct {
	query     string
	args      []any
	batchSize int
	offset    int
	ctx       context.Context

	exec func(ctx context.Context, query string, args ...any) (T, error)
}

func NewReader[T types.Iterable](ctx context.Context, baseQuery string, batchSize int,
	exec func(ctx context.Context, query string, args ...any) (T, error), args ...any) *Reader[T] {
	setter := &Reader[T]{
		query:     baseQuery,
		batchSize: batchSize,
		offset:    0,
		ctx:       ctx,
		exec:      exec,
		args:      args,
	}

	return setter
}

func (o *Reader[T]) Capture(onCapture func(T) error) error {
	if strings.HasSuffix(o.query, ";") {
		return fmt.Errorf("base query ends with ';': %s", o.query)
	}

	rows, err := o.exec(o.ctx, o.query, o.args...)
	if err != nil {
		return err
	}

	for rows.Next() {
		err := onCapture(rows)
		if err != nil {
			return err
		}
	}

	err = rows.Err()
	if err != nil {
		return err
	}
	return nil
}
