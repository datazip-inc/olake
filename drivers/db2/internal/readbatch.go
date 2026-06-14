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

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"

	goibmdb "github.com/ibmdb/go_ibm_db"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/utils"
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

// buildColBuffers allocates a fresh set of typed column slices for one ReadBatch
// call. The element type for each column is determined from its ColumnTypeScanType.
func buildColBuffers(scanTypes []reflect.Type, fetchSize int) []colBuffer {
	timeType := reflect.TypeOf(time.Time{})
	bufs := make([]colBuffer, len(scanTypes))

	for i, t := range scanTypes {
		switch {
		case t.Kind() == reflect.Bool:
			s := make([]bool, fetchSize)
			bufs[i].dest = &s
			bufs[i].getValue = func(j int) interface{} { return s[j] }

		case t.Kind() == reflect.Int32:
			s := make([]int32, fetchSize)
			bufs[i].dest = &s
			bufs[i].getValue = func(j int) interface{} { return s[j] }

		case t.Kind() == reflect.Int64:
			s := make([]int64, fetchSize)
			bufs[i].dest = &s
			bufs[i].getValue = func(j int) interface{} { return s[j] }

		case t.Kind() == reflect.Float64:
			s := make([]float64, fetchSize)
			bufs[i].dest = &s
			bufs[i].getValue = func(j int) interface{} { return s[j] }

		case t.Kind() == reflect.String:
			s := make([]string, fetchSize)
			bufs[i].dest = &s
			bufs[i].getValue = func(j int) interface{} { return s[j] }

		case t.Kind() == reflect.Slice: // []byte
			s := make([][]byte, fetchSize)
			bufs[i].dest = &s
			bufs[i].getValue = func(j int) interface{} { return s[j] }

		case t == timeType: // time.Time (Struct kind)
			s := make([]time.Time, fetchSize)
			bufs[i].dest = &s
			bufs[i].getValue = func(j int) interface{} { return s[j] }

		default:
			// Fallback: treat as string.
			s := make([]string, fetchSize)
			bufs[i].dest = &s
			bufs[i].getValue = func(j int) interface{} { return s[j] }
		}
	}
	return bufs
}

// isNullDecimalRaw treats driver zero-values for NULL DECIMAL/DECFLOAT cells as
// SQL null. ReadBatch uses [][]byte (nil) after the driver TypeScan fix; empty
// string/slice remain as a fallback for older driver builds.
func isNullDecimalRaw(raw interface{}, dbTypeName string) bool {
	n := strings.ToLower(dbTypeName)
	if n != "decimal" && n != "decfloat" {
		return false
	}
	switch v := raw.(type) {
	case nil:
		return true
	case string:
		return v == ""
	case []byte:
		return len(v) == 0
	default:
		return false
	}
}

// colBatch bundles a reusable set of column buffers with the matching
// []interface{} argument slice handed to ReadBatch. Recycling these across
// fetches (via a free-list channel) removes the per-batch buildColBuffers
// allocations that the alloc profile showed at ~25% of all heap allocations.
//
// nulls holds one []bool per column (each sized fetchSize). The driver's
// ReadBatchWithNulls fills nulls[c][i] = true when column c, row i is SQL NULL.
// This is what lets the fast path preserve the NULL-vs-zero-value distinction
// for every type — the typed destination slices alone cannot express NULL.
type colBatch struct {
	bufs  []colBuffer
	args  []interface{}
	nulls [][]bool
}

// batchMsg is the unit sent from producer to consumer: one filled colBatch plus
// the row count for that fetch.
type batchMsg struct {
	n     int
	batch *colBatch
}

// readBatchConcurrent reads the result of query using the patched driver's
// ReadBatch API. It runs a producer goroutine (SQLFetch + typed decode) and a
// consumer goroutine (OLake type conversion + OnMessage) concurrently.
func (d *DB2) readBatchConcurrent(ctx context.Context, query string, onMessage abstract.BackfillMsgFn) error {
	sqlConn, err := d.client.Conn(ctx)
	if err != nil {
		return fmt.Errorf("readBatchConcurrent: acquire conn: %w", err)
	}
	defer sqlConn.Close()

	return sqlConn.Raw(func(driverConn interface{}) error {
		c, ok := driverConn.(*goibmdb.Conn)
		if !ok {
			return fmt.Errorf("readBatchConcurrent: not a *go_ibm_db.Conn (%T)", driverConn)
		}

		// Push the fetch size to the driver before executing the query.
		c.SetFetchSize(db2DefaultFetchSize)

		dr, err := c.Query(query, nil)
		if err != nil {
			return fmt.Errorf("readBatchConcurrent: query: %w", err)
		}
		defer dr.Close()

		rows, ok := dr.(*goibmdb.Rows)
		if !ok {
			return fmt.Errorf("readBatchConcurrent: expected *go_ibm_db.Rows, got %T", dr)
		}

		// Collect column metadata once.
		colNames := rows.Columns()
		nCols := len(colNames)
		scanTypes := make([]reflect.Type, nCols)
		colTypeNames := make([]string, nCols)
		for i := 0; i < nCols; i++ {
			scanTypes[i] = rows.ColumnTypeScanType(i)
			colTypeNames[i] = rows.ColumnTypeDatabaseTypeName(i)
		}

		// Pre-compile per-column converters once, with the olake DataType
		// resolved a single time per column (not per cell). See
		// buildResolvedConverters.
		convFuncs := d.buildResolvedConverters(colTypeNames)

		// Read the fetch size back from the rows object — this is the value the
		// driver actually applied to SQL_ATTR_ROW_ARRAY_SIZE (confirming that
		// SetFetchSize took effect) and is used to pre-allocate buffer sets.
		fetchSize := rows.FetchSize()
		if fetchSize <= 0 {
			fetchSize = db2DefaultFetchSize
		}

		// Buffered channel: producer stages batchChannelDepth rowsets ahead.
		batchCh := make(chan batchMsg, batchChannelDepth)

		// Free-list of reusable buffer sets. Sized to cover every colBatch that
		// can be in flight simultaneously: one being filled by the producer, up
		// to batchChannelDepth queued in batchCh, and one being drained by the
		// consumer. The consumer returns each set to freeCh once it is done, so
		// no per-batch column-buffer allocation happens in steady state.
		const inFlight = batchChannelDepth + 2
		freeCh := make(chan *colBatch, inFlight)
		for k := 0; k < inFlight; k++ {
			bufs := buildColBuffers(scanTypes, fetchSize)
			args := make([]interface{}, nCols)
			nulls := make([][]bool, nCols)
			for j := range bufs {
				args[j] = bufs[j].dest
				nulls[j] = make([]bool, fetchSize)
			}
			freeCh <- &colBatch{bufs: bufs, args: args, nulls: nulls}
		}

		// ── Producer ────────────────────────────────────────────────────────
		// Calls ReadBatch in a tight loop. Each call fills typed slices for up
		// to fetchSize rows in a single SQLFetch — no database/sql overhead.
		// Buffer sets are leased from freeCh and recycled by the consumer.
		producer := func(ctx context.Context) error {
			defer close(batchCh)
			for {
				var cb *colBatch
				select {
				case <-ctx.Done():
					return nil
				case cb = <-freeCh:
				}

				n, err := rows.ReadBatch(cb.args, cb.nulls)
				if err == io.EOF {
					return nil
				}
				if err != nil {
					return fmt.Errorf("ReadBatch: %w", err)
				}

				select {
				case <-ctx.Done():
					return nil // consumer failed; its error will surface via errgroup
				case batchCh <- batchMsg{n: n, batch: cb}:
				}
			}
		}

		// ── Consumer ────────────────────────────────────────────────────────
		// Converts each row of each batch through the pre-compiled converters
		// and calls OnMessage. Runs concurrently with the producer, then returns
		// the buffer set to freeCh for reuse.
		consumer := func(ctx context.Context) error {
			for b := range batchCh {
				for i := 0; i < b.n; i++ {
					record := make(map[string]interface{}, nCols)
					for j, col := range colNames {
						// SQL NULL is reported via the driver's null mask, not the
						// typed slice (which stores a zero-value for NULL cells).
						// Emit nil so NULL stays distinct from a real 0/""/false/
						if b.batch.nulls[j][i] {
							record[col] = nil
							continue
						}
						raw := b.batch.bufs[j].getValue(i)
						conv, err := convFuncs[j](raw)
						if err != nil {
							return fmt.Errorf("column %s: %w", col, err)
						}
						record[col] = conv
					}
					if err := onMessage(ctx, record); err != nil {
						return err
					}
				}
				freeCh <- b.batch
			}
			return nil
		}

		return utils.ConcurrentF(ctx, consumer, producer)
	})
}
