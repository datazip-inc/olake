package jdbc

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/typeutils"
)

type Reader[T types.Iterable] struct {
	query  string
	args   []any
	offset int
	ctx    context.Context

	exec func(ctx context.Context, query string, args ...any) (T, error)
}

func NewReader[T types.Iterable](ctx context.Context, baseQuery string,
	exec func(ctx context.Context, query string, args ...any) (T, error), args ...any) *Reader[T] {
	setter := &Reader[T]{
		query:  baseQuery,
		offset: 0,
		ctx:    ctx,
		exec:   exec,
		args:   args,
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
			datatype := types[i].DatabaseTypeName()
			precision, scale, hasPrecisionScale := types[i].DecimalSize()
			if datatype == "NUMBER" && hasPrecisionScale && scale == 0 {
				datatype = utils.Ternary(precision > 9, "int64", "int32").(string)
			}
			conv, err := converter(rawData, datatype)
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

func MapScanConcurrent(setter *Reader[*sql.Rows], converter func(value interface{}, columnType string) (interface{}, error), OnMessage abstract.BackfillMsgFn) error {
	ctx := setter.ctx
	valuesCh := make(chan []any)
	doneCh := make(chan error, 1)

	var (
		columns  []string
		colTypes []*sql.ColumnType
	)

	go func() {
		defer close(doneCh)
		for vals := range valuesCh {
			record := make(map[string]any)
			for i, col := range columns {
				rawData := vals[i]
				datatype := colTypes[i].DatabaseTypeName()
				precision, scale, hasPrecisionScale := colTypes[i].DecimalSize()
				if datatype == "NUMBER" && hasPrecisionScale && scale == 0 {
					datatype = utils.Ternary(precision > 9, "int64", "int32").(string)
				}
				conv, err := converter(rawData, datatype)
				if err != nil && err != typeutils.ErrNullValue {
					doneCh <- fmt.Errorf("failed to convert value for column %s: %w", col, err)
					return
				}
				record[col] = conv
			}
			if err := OnMessage(ctx, record); err != nil {
				doneCh <- err
				return
			}
		}
		doneCh <- nil
	}()

	// Capture rows: scan quickly and hand off raw values to the processor
	err := setter.Capture(func(rows *sql.Rows) error {
		if columns == nil {
			var metaErr error
			columns, metaErr = rows.Columns()
			if metaErr != nil {
				return metaErr
			}
			colTypes, metaErr = rows.ColumnTypes()
			if metaErr != nil {
				return metaErr
			}
		}

		// Prepare scan destinations
		scanDests := make([]any, len(columns))
		for i := range scanDests {
			scanDests[i] = new(any)
		}
		if err := rows.Scan(scanDests...); err != nil {
			return err
		}

		// Copy out dereferenced values so the next Scan can proceed safely
		vals := make([]any, len(columns))
		for i := range scanDests {
			vals[i] = *(scanDests[i].(*any))
		}

		// Hand off to processor via unbuffered channel; if processor finished early, surface its error
		select {
		case valuesCh <- vals:
			return nil
		case procErr := <-doneCh:
			if procErr != nil {
				return procErr
			}
			return nil
		}
	})

	close(valuesCh)
	procErr := <-doneCh
	if err != nil {
		return err
	}
	return procErr
}
