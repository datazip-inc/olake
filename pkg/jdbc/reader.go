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

// getColumnMetadata extracts column names and types from sql.Rows
func getColumnMetadata(rows *sql.Rows) ([]string, []*sql.ColumnType, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, nil, err
	}

	return columns, colTypes, nil
}

// normalizeDataTypeAndConvert normalizes the datatype and converts the raw value using the converter function
func normalizeDataTypeAndConvert(rawData any, colType *sql.ColumnType, converter func(value interface{}, columnType string) (interface{}, error)) (any, error) {
	datatype := colType.DatabaseTypeName()
	precision, scale, hasPrecisionScale := colType.DecimalSize()
	if datatype == "NUMBER" && hasPrecisionScale && scale == 0 {
		datatype = utils.Ternary(precision > 9, "int64", "int32").(string)
	}
	conv, err := converter(rawData, datatype)
	if err != nil && err != typeutils.ErrNullValue {
		return nil, err
	}
	return conv, nil
}

// TODO: Use MapScanConcurrent instead of MapScan for incremental as well
//
// MapScan scans the current row into dest. The optional addRowBytes callback is
// invoked once per complete row with all raw (pre-conversion) column values and
// their SQL column types, allowing callers to compute per-row byte counts
// (including inter-column alignment padding) without an extra pass.
// Pass nil or omit to skip byte counting.
func MapScan(rows *sql.Rows, dest map[string]any, converter func(value interface{}, columnType string) (interface{}, error), addRowBytes ...func(rawVals []any, colTypes []*sql.ColumnType)) error {
	columns, colTypes, err := getColumnMetadata(rows)
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

	rawVals := make([]any, len(columns))
	for i := range scanValues {
		rawVals[i] = *(scanValues[i].(*any)) // Dereference pointer before storing
	}

	if len(addRowBytes) > 0 && addRowBytes[0] != nil {
		addRowBytes[0](rawVals, colTypes)
	}

	for i, col := range columns {
		if converter != nil {
			conv, err := normalizeDataTypeAndConvert(rawVals[i], colTypes[i], converter)
			if err != nil {
				return err
			}
			dest[col] = conv
		} else {
			dest[col] = rawVals[i]
		}
	}

	return nil
}

// MapScanConcurrent scans rows concurrently using a producer/consumer pattern.
// The optional addRowBytes callback is invoked in the producer goroutine once
// per complete row with all raw (pre-conversion) column values and their SQL
// column types, allowing callers to compute per-row byte counts (including
// inter-column alignment padding) without a second pass. Pass nil or omit to
// skip byte counting.
func MapScanConcurrent(setter *Reader[*sql.Rows], converter func(value interface{}, columnType string) (interface{}, error), OnMessage abstract.BackfillMsgFn, addRowBytes ...func(rawVals []any, colTypes []*sql.ColumnType)) error {
	valuesCh := make(chan []any)

	var (
		columns   []string
		colTypes  []*sql.ColumnType
		scanDests []any // reused pointers for rows.Scan
	)

	var rowBytesFn func([]any, []*sql.ColumnType)
	if len(addRowBytes) > 0 && addRowBytes[0] != nil {
		rowBytesFn = addRowBytes[0]
	}

	// Producer: scan rows and push raw values onto the channel.
	producer := func(ctx context.Context) error {
		defer close(valuesCh)

		return setter.Capture(func(rows *sql.Rows) error {
			if columns == nil {
				var metaErr error
				columns, colTypes, metaErr = getColumnMetadata(rows)
				if metaErr != nil {
					return metaErr
				}

				scanDests = make([]any, len(columns))
				for i := range scanDests {
					scanDests[i] = new(any)
				}
			}

			if err := rows.Scan(scanDests...); err != nil {
				return err
			}

			vals := make([]any, len(columns))
			for i := range scanDests {
				vals[i] = *(scanDests[i].(*any))
			}

			if rowBytesFn != nil {
				rowBytesFn(vals, colTypes)
			}

			select {
			case <-ctx.Done():
				// If the processor failed, errgroup cancels the ctx; return nil so the original error wins.
				return nil
			case valuesCh <- vals:
				return nil
			}
		})
	}

	// Consumer: convert + emit records.
	consumer := func(ctx context.Context) error {
		for vals := range valuesCh {
			record := make(map[string]any, len(columns))
			for i, col := range columns {
				rawData := vals[i]
				if converter == nil {
					record[col] = rawData
					continue
				}

				conv, err := normalizeDataTypeAndConvert(rawData, colTypes[i], converter)
				if err != nil {
					return fmt.Errorf("failed to convert value for column %s: %s", col, err)
				}
				record[col] = conv
			}

			if err := OnMessage(ctx, record); err != nil {
				return err
			}
		}
		return nil
	}

	return utils.ConcurrentF(setter.ctx, consumer, producer)
}
