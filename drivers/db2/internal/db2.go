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
// goroutine converts those raw values through the driver's dataTypeConverter
// and hands records to OnMessage. A buffered channel decouples the two so that
// I/O-bound reading and CPU-bound conversion overlap.
//
// Selector
// ────────
// Set DB2_READ_MODE=mapscan to fall back to the standard MapScanConcurrent
// path (e.g. for debugging or when query has bound parameters).

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"reflect"
	"time"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/utils"
	goibmdb "github.com/ibmdb/go_ibm_db"
)

// batchChannelDepth controls how many filled rowsets can be queued between the
// producer and consumer. 4 lets the producer run ~800 rows ahead.
const batchChannelDepth = 4

// colBuffer holds the typed destination slice for one column in a ReadBatch
// call. dest is passed to ReadBatch; getValue extracts the i-th element.
type colBuffer struct {
	// dest is a *[]T pointer. ReadBatch writes into the slice it points to.
	dest interface{}
	// getValue returns element i as interface{} for type-conversion.
	// The closure captures the slice variable directly: because the patched
	// driver's ensureCapXxx only reallocates when len < n, and we pre-allocate
	// to fetchSize which equals the driver's rowset size, the underlying array
	// is never replaced and the captured slice header stays valid.
	getValue func(i int) interface{}
}

// colBatch bundles a reusable set of column buffers with the matching
// []interface{} destination slice handed to ReadBatch. Recycling these across
// fetches (via a free-list channel) removes the per-batch buildColBuffers
// allocations that the alloc profile showed at ~25% of all heap allocations.
//
// nulls holds one []bool per column (each sized fetchSize). The driver's
// ReadBatchWithNulls fills nulls[c][i] = true when column c, row i is SQL NULL.
// This is what lets the fast path preserve the NULL-vs-zero-value distinction
// for every type — the typed destination slices alone cannot express NULL.
type colBatch struct {
	bufs  []colBuffer
	dests []interface{}
	nulls [][]bool
}

// batchMsg is the unit sent from producer to consumer: one filled colBatch plus
// the row count for that fetch.
type batchMsg struct {
	n     int
	batch *colBatch
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

		// TODO: check if we need to make it configurable
		// set the fetch size for the connection to the default value.
		conn.SetFetchSize(db2DefaultFetchSize)

		var rows *goibmdb.Rows
		// len(args) = 0: backfill for full_refresh
		// len(args) > 0: incremental sync
		if len(args) == 0 {
			driverRows, err := conn.Query(query, nil)
			if err != nil {
				return fmt.Errorf("readBatchConcurrent: query: %s", err)
			}
			var ok bool
			rows, ok = driverRows.(*goibmdb.Rows)
			if !ok {
				driverRows.Close()
				return fmt.Errorf("readBatchConcurrent: expected *go_ibm_db.Rows, got %T", driverRows)
			}
		} else {
			driverArgs := make([]driver.Value, len(args))
			for i, a := range args {
				driverArgs[i] = a
			}
			rows, err = conn.QueryWithArgs(query, driverArgs)
			if err != nil {
				return fmt.Errorf("readBatchConcurrent: query with args: %s", err)
			}
		}
		defer rows.Close()

		// Collect column metadata once.
		colNames := rows.Columns()
		nCols := len(colNames)
		scanTypes := make([]reflect.Type, nCols)
		colTypeNames := make([]string, nCols)
		for idx := range nCols {
			scanTypes[idx] = rows.ColumnTypeScanType(idx)
			colTypeNames[idx] = rows.ColumnTypeDatabaseTypeName(idx)
		}

		// Pre-compile per-column converters once, with the olake DataType
		// resolved a single time per column (not per cell). See
		// buildResolvedConverters.
		convFuncs := d.buildResolvedConverters(colTypeNames)

		// Read the fetch size back from the rows object — this is the value the
		// driver actually applied to SQL_ATTR_ROW_ARRAY_SIZE. When the result
		// set contains non-bindable columns (CLOB/DBCLOB/XML), BindColumns in
		// the patched driver resets SQL_ATTR_ROW_ARRAY_SIZE to 1. In that case
		// rows.FetchSize() returns 1 and we skip the producer-consumer goroutine
		// pair (two channel hops per row) and run a simple inline loop instead.
		fetchSize := rows.FetchSize()

		// processBatch converts one filled colBatch (n rows) to OLake records.
		// Shared by both the inline (FetchSize=1) and concurrent paths below.
		processBatch := func(cb *colBatch, n int) error {
			for rowIdx := 0; rowIdx < n; rowIdx++ {
				record := make(map[string]interface{}, nCols)
				for colIdx, colName := range colNames {
					// SQL NULL is reported via the driver's null mask, not the
					// typed slice (which stores a zero-value for NULL cells).
					// Emit nil so NULL stays distinct from a real 0/""/false/
					if cb.nulls[colIdx][rowIdx] {
						record[colName] = nil
						continue
					}
					raw := cb.bufs[colIdx].getValue(rowIdx)
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

		// ── Inline path (FetchSize=1, non-bindable columns present) ─────────
		// When the driver downgraded to FetchSize=1 due to CLOB/DBCLOB/XML
		// columns, producer-consumer goroutine overhead (two channel round-trips
		// per row) costs more than it saves. Run a single-goroutine loop instead.
		if fetchSize == 1 {
			cb := newColBatch(scanTypes, 1)
			for {
				if err := ctx.Err(); err != nil {
					return nil
				}
				n, readErr := rows.ReadBatch(cb.dests, cb.nulls)
				if n > 0 {
					if err := processBatch(cb, n); err != nil {
						return err
					}
				}
				switch {
				case errors.Is(readErr, io.EOF):
					return nil
				case readErr != nil:
					return fmt.Errorf("ReadBatch: %s", readErr)
				}
			}
		}

		// ── Concurrent path (FetchSize>1, all columns bindable) ─────────────
		// Buffered channel: producer stages batchChannelDepth rowsets ahead.
		batchCh := make(chan batchMsg, batchChannelDepth)

		// Free-list of reusable buffer sets. Sized to cover every colBatch that
		// can be in flight simultaneously: one being filled by the producer, up
		// to batchChannelDepth queued in batchCh, and one being drained by the
		// consumer. The consumer returns each set to freeCh once it is done, so
		// no per-batch column-buffer allocation happens in steady state.
		const inFlight = batchChannelDepth + 2
		freeCh := make(chan *colBatch, inFlight)
		for range inFlight {
			freeCh <- newColBatch(scanTypes, fetchSize)
		}

		// ── Producer ────────────────────────────────────────────────────────
		// Calls ReadBatch in a tight loop. Each call fills typed slices for up
		// to fetchSize rows in a single SQLFetch — no database/sql overhead.
		// Buffer sets are leased from freeCh and recycled by the consumer.
		producer := func(ctx context.Context) (err error) {
			// recover so an unsafe.Pointer panic in ReadBatch returns an error instead of crashing the process.
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("readBatchConcurrent producer: panic: %v", r)
				}
			}()
			defer close(batchCh)
			for {
				var cb *colBatch
				select {
				case <-ctx.Done():
					return nil
				case cb = <-freeCh:
				}

				n, readErr := rows.ReadBatch(cb.dests, cb.nulls)
				if n > 0 {
					select {
					case <-ctx.Done():
					case batchCh <- batchMsg{n: n, batch: cb}:
					}
				} else {
					freeCh <- cb
				}
				switch {
				case errors.Is(readErr, io.EOF):
					return nil
				case readErr != nil:
					return fmt.Errorf("ReadBatch: %s", readErr)
				}
			}
		}

		// ── Consumer ────────────────────────────────────────────────────────
		// Converts each row of each batch through the pre-compiled converters
		// and calls OnMessage. Runs concurrently with the producer, then returns
		// the buffer set to freeCh for reuse.
		consumer := func(ctx context.Context) (err error) {
			// recover so a panic in getValue, convFuncs, or onMessage returns an error instead of crashing the process.
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("readBatchConcurrent consumer: panic: %v", r)
				}
			}()
			for b := range batchCh {
				if err := processBatch(b.batch, b.n); err != nil {
					return err
				}
				freeCh <- b.batch
			}
			return nil
		}

		return utils.ConcurrentF(ctx, consumer, producer)
	})
}

// makeColBuffer allocates a typed column slice and wires dest/getValue for ReadBatch.
func makeColBuffer[T any](fetchSize int) colBuffer {
	colValues := make([]T, fetchSize)
	return colBuffer{
		dest:     &colValues,
		getValue: func(j int) interface{} { return colValues[j] },
	}
}

// buildColBuffers allocates a fresh set of typed column slices for one ReadBatch
// call. The element type for each column is determined from its ColumnTypeScanType.
func buildColBuffers(scanTypes []reflect.Type, fetchSize int) []colBuffer {
	bufs := make([]colBuffer, len(scanTypes))

	for i, scanType := range scanTypes {
		switch {
		case scanType.Kind() == reflect.Bool:
			bufs[i] = makeColBuffer[bool](fetchSize)
		case scanType.Kind() == reflect.Int32:
			bufs[i] = makeColBuffer[int32](fetchSize)
		case scanType.Kind() == reflect.Int64:
			bufs[i] = makeColBuffer[int64](fetchSize)
		case scanType.Kind() == reflect.Float64:
			bufs[i] = makeColBuffer[float64](fetchSize)
		case scanType.Kind() == reflect.String:
			bufs[i] = makeColBuffer[string](fetchSize)
		case scanType.Kind() == reflect.Slice: // []byte
			bufs[i] = makeColBuffer[[]byte](fetchSize)
		case scanType == reflect.TypeOf(time.Time{}):
			bufs[i] = makeColBuffer[time.Time](fetchSize)
		default:
			bufs[i] = makeColBuffer[string](fetchSize)
		}
	}
	return bufs
}

// newColBatch allocates typed column buffers, null masks, and the dest slice
// for one ReadBatch call at the given fetchSize.
func newColBatch(scanTypes []reflect.Type, fetchSize int) *colBatch {
	bufs := buildColBuffers(scanTypes, fetchSize)
	colDests := make([]interface{}, len(bufs))
	nulls := make([][]bool, len(bufs))
	for j := range bufs {
		colDests[j] = bufs[j].dest
		nulls[j] = make([]bool, fetchSize)
	}
	return &colBatch{bufs: bufs, dests: colDests, nulls: nulls}
}
