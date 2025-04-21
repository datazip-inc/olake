package jdbc

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/typeutils"
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

func MapScan(rows *sql.Rows, dest map[string]any, converter func(value interface{}, columnType string) (interface{}, error)) error {
	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	types, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	scanValues := make([]any, len(columns))
	for i := range scanValues {
		scanValues[i] = new(any) // Allocate pointers for scanning
	}

	if err := rows.Scan(scanValues...); err != nil {
		return err
	}

	for i, col := range columns {
		rawData := *(scanValues[i].(*any)) // Dereference pointer before storing
		if converter != nil {
			conv, err := converter(rawData, types[i].DatabaseTypeName())
			if err != nil && err != typeutils.ErrNullValue {
				return err
			}
			dest[col] = conv
		} else {
			dest[col] = rawData
		}
	}

	return nil
}
