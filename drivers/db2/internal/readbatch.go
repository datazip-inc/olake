package driver

// readbatch.go — fast-path DB2 bulk reader using the patched go_ibm_db driver.
//
// Architecture
// ────────────
// Instead of the standard database/sql.Rows.Scan path (one CGO SQLFetch per
// row, convertAssign reflection per cell), this file uses two features of the
// patched driver:
//
//  1. Block fetch (SQL_ATTR_ROW_ARRAY_SIZE): SQLFetch returns db2DefaultFetchSize
//     rows per CGO call instead of one.
//
//  2. ReadBatch(*[]T, ...): reads the entire current rowset directly into
//     typed Go slices, bypassing database/sql.Scan and convertAssign entirely.
//
// Producer-consumer concurrency
// ──────────────────────────────
// The producer goroutine calls ReadBatch in a tight loop. Each call fills
// typed column slices for db2DefaultFetchSize rows in one shot. The consumer
// goroutine converts those raw values through the pre-compiled converters
// and hands records to OnMessage. A buffered channel decouples the two so that
// I/O-bound reading and CPU-bound conversion overlap.

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
	goibmdb "github.com/ibmdb/go_ibm_db"
)

const (
	// batchChannelDepth controls how many filled rowsets can be queued between the
	// producer and consumer.
	batchChannelDepth = 4
	// inFlight is the number of colBatch buffers pooled on freeCh: one being
	// filled, up to batchChannelDepth queued, and one being drained.
	inFlight            = batchChannelDepth + 2
	db2DefaultFetchSize = 200
)

// colBatch holds all per-column buffers, null masks, and the row count for one
// ReadBatch call. It is the unit exchanged between the producer and consumer.
//
// dests are *[]T pointers passed directly to ReadBatch (one per column).
// getValues are closures that extract element i from each column's typed slice.
// nulls holds one []bool per column (each sized fetchSize). The driver fills
// nulls[c][i] = true when column c, row i is SQL NULL.
// rowCount is set by the producer to the number of rows filled in the last ReadBatch.
type colBatch struct {
	dests     []interface{}
	getValues []func(i int) interface{}
	nulls     [][]bool
	rowCount  int
}

// readBatchConcurrent executes query (with optional args) via the patched driver,
// fetches rows in bulk with ReadBatch, converts them to OLake records, and calls onMessage.
func (d *DB2) readBatchConcurrent(ctx context.Context, query string, args []any, onMessage abstract.BackfillMsgFn) error {
	sqlConn, err := d.client.Conn(ctx)
	if err != nil {
		return fmt.Errorf("readBatchConcurrent: acquire conn: %s", err)
	}
	defer sqlConn.Close()

	// raw connection to enable access to the underlying driver connection,
	// used to bypass the database/sql layer and directly interact with the forked driver.
	return sqlConn.Raw(func(driverConn interface{}) error {
		conn, ok := driverConn.(*goibmdb.Conn)
		if !ok {
			return fmt.Errorf("readBatchConcurrent: not a *go_ibm_db.Conn (%T)", driverConn)
		}

		// set the fetch size for the connection to the default value.
		conn.SetFetchSize(db2DefaultFetchSize)

		driverArgs := make([]driver.Value, len(args))
		for i, a := range args {
			v, err := driver.DefaultParameterConverter.ConvertValue(a)
			if err != nil {
				return fmt.Errorf("readBatchConcurrent: arg[%d] type %T not supported by ODBC: %w", i, a, err)
			}
			driverArgs[i] = v
		}
		rows, err := conn.QueryBatch(query, driverArgs)
		if err != nil {
			return fmt.Errorf("readBatchConcurrent: query: %s", err)
		}
		defer rows.Close()

		// Collect column metadata once.
		colNames := rows.Columns()
		nCols := len(colNames)
		// Go types of the columns
		scanTypes := make([]reflect.Type, nCols)
		// Actual DB2 data types of the columns
		colTypeNames := make([]string, nCols)
		for idx := range nCols {
			scanTypes[idx] = rows.ColumnTypeScanType(idx)
			colTypeNames[idx] = rows.ColumnTypeDatabaseTypeName(idx)
		}

		// Pre-compile per-column converters once, with the olake DataType
		// resolved a single time per column
		convFuncs := d.buildResolvedConverters(colTypeNames)

		// Read the fetch size back from the rows object this is the value the
		// driver actually applied to SQL_ATTR_ROW_ARRAY_SIZE. When the result
		// set contains non-bindable columns (CLOB/DBCLOB/XML), BindColumns in
		// the forked driver resets SQL_ATTR_ROW_ARRAY_SIZE to 1. In that case
		// rows.FetchSize() returns 1 and we skip the producer-consumer.
		fetchSize := rows.FetchSize()

		// processBatch converts one filled colBatch to OLake records.
		processBatch := func(batch *colBatch) error {
			for rowIdx := range batch.rowCount {
				record := make(map[string]interface{}, nCols)
				for colIdx, colName := range colNames {
					// SQL NULL is reported via the driver's null mask
					if batch.nulls[colIdx][rowIdx] {
						record[colName] = nil
						continue
					}
					raw := batch.getValues[colIdx](rowIdx)
					conv, err := convFuncs[colIdx](raw)
					if err != nil {
						return fmt.Errorf("column %s: %s", colName, err)
					}
					record[colName] = conv
				}
				if err := onMessage(ctx, record); err != nil {
					return err
				}
			}
			return nil
		}

		// Inline path (FetchSize=1, non-bindable columns present)
		// When the driver downgraded to FetchSize=1 due to CLOB/DBCLOB/XML
		// columns, producer-consumer goroutine overhead (two channel round-trips
		// per row) costs more than it saves. Run a single-goroutine loop instead.
		if fetchSize == 1 {
			logger.Infof("[DB2] readBatchConcurrent: block fetch downgraded to fetch_size=1 (non-bindable columns such as CLOB/DBCLOB/XML); using inline read path")
			batch := newColBatch(scanTypes, 1)
			for {
				if err := ctx.Err(); err != nil {
					return ctx.Err()
				}
				var readErr error
				batch.rowCount, readErr = rows.ReadBatch(batch.dests, batch.nulls)
				if err := processBatch(batch); err != nil {
					return err
				}
				if errors.Is(readErr, io.EOF) {
					return nil
				} else if readErr != nil {
					return fmt.Errorf("ReadBatchConcurrent: %s", readErr)
				}
			}
		}

		// Concurrent path (FetchSize>1, all columns bindable)
		batchCh := make(chan *colBatch, batchChannelDepth)

		freeCh := make(chan *colBatch, inFlight)
		for range inFlight {
			freeCh <- newColBatch(scanTypes, fetchSize)
		}

		// ── Producer ────────────────────────────────────────────────────────
		// Calls ReadBatch in a tight loop. Each call fills typed slices for up
		// to fetchSize rows in a single SQLFetch.
		// Buffer sets are leased from freeCh and recycled by the consumer.
		producer := func(ctx context.Context) (err error) {
			// recover so a panic in ReadBatch returns an error instead of crashing the process.
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("readBatchConcurrent producer: panic: %v", r)
				}
			}()
			defer close(batchCh)
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case batch := <-freeCh:
					var readErr error
					batch.rowCount, readErr = rows.ReadBatch(batch.dests, batch.nulls)
					select {
					case <-ctx.Done():
						return ctx.Err()
					case batchCh <- batch:
					}
					if errors.Is(readErr, io.EOF) {
						return nil
					} else if readErr != nil {
						return fmt.Errorf("ReadBatchConcurrent producer: %s", readErr)
					}
				}
			}
		}

		// ── Consumer ────────────────────────────────────────────────────────
		// Converts each row of each batch through the pre-compiled converters
		// and calls OnMessage. Runs concurrently with the producer, then returns
		// the buffer set to freeCh for reuse.
		consumer := func(ctx context.Context) (err error) {
			// recover so a panic in processBatch returns an error instead of crashing the process.
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("readBatchConcurrent consumer: panic: %v", r)
				}
			}()
			for batch := range batchCh {
				if err := processBatch(batch); err != nil {
					return err
				}
				freeCh <- batch
			}
			return nil
		}

		return utils.ConcurrentF(ctx, consumer, producer)
	})
}

// newColBatch allocates typed column buffers, null masks, and the dest slice
// for one ReadBatch call at the given fetchSize.
func newColBatch(scanTypes []reflect.Type, fetchSize int) *colBatch {
	n := len(scanTypes)
	batch := &colBatch{
		dests:     make([]interface{}, n),
		getValues: make([]func(int) interface{}, n),
		nulls:     make([][]bool, n),
	}
	for idx, scanType := range scanTypes {
		switch {
		case scanType.Kind() == reflect.Bool:
			setColColumn[bool](batch, idx, fetchSize)
		case scanType.Kind() == reflect.Int32:
			setColColumn[int32](batch, idx, fetchSize)
		case scanType.Kind() == reflect.Int64:
			setColColumn[int64](batch, idx, fetchSize)
		case scanType.Kind() == reflect.Float64:
			setColColumn[float64](batch, idx, fetchSize)
		case scanType.Kind() == reflect.String:
			setColColumn[string](batch, idx, fetchSize)
		case scanType.Kind() == reflect.Slice: // []byte
			setColColumn[[]byte](batch, idx, fetchSize)
		case scanType == reflect.TypeOf(time.Time{}):
			setColColumn[time.Time](batch, idx, fetchSize)
		default:
			setColColumn[string](batch, idx, fetchSize)
		}
	}
	return batch
}

func setColColumn[T any](batch *colBatch, idx, fetchSize int) {
	colValues := make([]T, fetchSize)
	batch.dests[idx] = &colValues
	batch.getValues[idx] = func(j int) interface{} { return colValues[j] }
	batch.nulls[idx] = make([]bool, fetchSize)
}

// buildResolvedConverters returns one converter per column. Each converter
// turns a raw ReadBatch value into the OLake representation (e.g. int64,
// timestamp, "HH:MM:SS" for TIME). DB2 type -> OLake type mapping.
func (d *DB2) buildResolvedConverters(colTypeNames []string) []func(interface{}) (interface{}, error) {
	convFuncs := make([]func(interface{}) (interface{}, error), len(colTypeNames))
	for i, typeName := range colTypeNames {
		olakeType := typeutils.ExtractAndMapColumnType(typeName, db2TypeToDataTypes)
		convFuncs[i] = func(raw interface{}) (interface{}, error) {
			if strings.EqualFold(typeName, "TIME") {
				return typeutils.ReformatTimeValue(raw)
			}

			if olakeType == types.String {
				if s, ok := raw.(string); ok {
					return strings.TrimSpace(s), nil
				}
			}

			v, err := typeutils.ReformatValue(olakeType, raw)
			if err != nil && !errors.Is(err, typeutils.ErrNullValue) {
				return nil, err
			}
			return v, nil
		}
	}
	return convFuncs
}
