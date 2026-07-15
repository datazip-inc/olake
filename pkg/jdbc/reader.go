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
// MapScan scans the current row into dest and returns the row's source-DB byte
// size. sizeOf sizes a single raw (pre-conversion) column value by its SQL column
// type; it is summed inline in the scan loop.
func MapScan(rows *sql.Rows, dest map[string]any, converter func(value interface{}, columnType string) (interface{}, error), sizeOf func(colType *sql.ColumnType, v any) int64) (int64, error) {
	columns, colTypes, err := getColumnMetadata(rows)
	if err != nil {
		return 0, err
	}

	scanValues := make([]any, len(columns))
	for i := range scanValues {
		scanValues[i] = new(any) // Allocate pointers for scanning
	}

	if err := rows.Scan(scanValues...); err != nil {
		return 0, err
	}

	var rowBytes int64
	for i, col := range columns {
		rawData := *(scanValues[i].(*any)) // Dereference pointer before storing
		rowBytes += sizeOf(colTypes[i], rawData)
		if converter != nil {
			conv, err := normalizeDataTypeAndConvert(rawData, colTypes[i], converter)
			if err != nil {
				return 0, err
			}
			dest[col] = conv
		} else {
			dest[col] = rawData
		}
	}

	return rowBytes, nil
}

// MapScanConcurrent scans rows concurrently using a producer/consumer pattern.
// bytesCalculatorFunction is a factory invoked once (with the result set's SQL column
// types) to build a per-column sizing function. The returned sizer
// runs in the consumer goroutine on each raw (pre-conversion) column value, and is summed inline in the conversion loop
func MapScanConcurrent(setter *Reader[*sql.Rows], converter func(value interface{}, columnType string) (interface{}, error), OnMessage abstract.BackfillMsgFn, bytesCalculatorFn func(colTypes []*sql.ColumnType) func(i int, v any) int64) error {
	valuesCh := make(chan []any)

	var (
		columns   []string
		colTypes  []*sql.ColumnType
		scanDests []any // reused pointers for rows.Scan
	)

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
		var sizeOf func(i int, v any) int64
		for vals := range valuesCh {
			// colTypes is safely visible here: the producer sets it before the
			// first channel send. Build the sizer once for the whole result set.
			if sizeOf == nil {
				sizeOf = bytesCalculatorFn(colTypes)
			}

			var rowBytes int64
			record := make(map[string]any, len(columns))
			for i, col := range columns {
				rawData := vals[i]
				rowBytes += sizeOf(i, rawData)
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

			if err := OnMessage(ctx, record, rowBytes); err != nil {
				return err
			}
		}
		return nil
	}

	return utils.ConcurrentF(setter.ctx, consumer, producer)
}
