package jdbc

import (
	"context"
	"fmt"
	"strings"

	"github.com/datazip-inc/olake/types"
	"github.com/piyushsingariya/relec/safego"
)

type Reader[T types.Iterable] struct {
	query     string
	args      []any
	batchSize int
	offset    int
	err       chan error
	rows      chan T
	closed    bool
	ctx       context.Context

	exec func(ctx context.Context, query string, args ...any) (T, error)
}

func NewReader[T types.Iterable](ctx context.Context, baseQuery string, batchSize int,
	exec func(ctx context.Context, query string, args ...any) (T, error), args ...any) *Reader[T] {
	setter := &Reader[T]{
		query:     baseQuery,
		batchSize: batchSize,
		offset:    0,
		err:       make(chan error),
		rows:      make(chan T),
		ctx:       ctx,
		exec:      exec,
		args:      args,
	}

	return setter
}

func (o *Reader[T]) Close() {
	o.closed = true
	safego.Close(o.err)
	safego.Close(o.rows)
}

func (o *Reader[T]) Capture(onCapture func(T) error) error {
	defer o.Close()

	if strings.HasSuffix(o.query, ";") {
		return fmt.Errorf("base query ends with ';': %s", o.query)
	}

	for {
		rows, err := o.exec(o.ctx, o.query, o.args...)
		if err != nil {
			return err
		}

		length := 0
		for rows.Next() {
			err := onCapture(rows)
			if err != nil {
				return err
			}

			length++
		}

		err = rows.Err()
		if err != nil {
			return err
		}

		if length != o.batchSize {
			return nil
		}

		o.offset += length
	}
}
