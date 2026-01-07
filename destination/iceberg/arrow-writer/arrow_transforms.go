//go:build duckdb_arrow
// +build duckdb_arrow

package arrowwriter

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	duckdb "github.com/duckdb/duckdb-go/v2"
)

//
// -----------------------------
// Public Engine
// -----------------------------
//

type ArrowTransformEngine struct {
	config *TransformConfig
}

// Constructor (same idea as before)
func NewArrowTransformEngine(config *TransformConfig) *ArrowTransformEngine {
	if config == nil {
		config = &TransformConfig{}
	}
	return &ArrowTransformEngine{config: config}
}

//
// -----------------------------
// Main Entry Point
// -----------------------------
//x

func (e *ArrowTransformEngine) TransformRecord(
	ctx context.Context,
	record arrow.RecordBatch,
) (arrow.RecordBatch, error) {

	// Fast path: nothing to do
	if e.config == nil ||
		(len(e.config.RowFilters) == 0 && len(e.config.ComputedColumns) == 0) {
		record.Retain()
		return record, nil
	}

	sql := e.buildSQL(record.Schema())

	return executeDuckDB(ctx, record, sql)
}

//
// -----------------------------
// SQL Builder
// -----------------------------
//

func (e *ArrowTransformEngine) buildSQL(_ *arrow.Schema) string {
	var sb strings.Builder

	// SELECT clause
	sb.WriteString("SELECT *")

	for _, cc := range e.config.ComputedColumns {
		// Example: price * qty AS total
		sb.WriteString(", ")
		sb.WriteString(cc.Expression)
		sb.WriteString(" AS ")
		sb.WriteString(cc.Name)
	}

	sb.WriteString(" FROM input")

	// WHERE clause
	if len(e.config.RowFilters) > 0 {
		sb.WriteString(" WHERE ")
		for i, f := range e.config.RowFilters {
			if i > 0 {
				sb.WriteString(" AND ")
			}
			sb.WriteString(f.Column)
			sb.WriteString(" ")
			sb.WriteString(normalizeCondition(f.Condition))
		}
	}

	return sb.String()
}

// Normalize a few custom conditions to SQL
func normalizeCondition(cond string) string {
	c := strings.TrimSpace(strings.ToLower(cond))

	switch c {
	case "== null", "is null":
		return "IS NULL"
	case "!= null", "is not null":
		return "IS NOT NULL"
	default:
		return cond
	}
}

//
// -----------------------------
// DuckDB Execution
// -----------------------------
//

func executeDuckDB(
	ctx context.Context,
	batch arrow.Record,
	query string,
) (arrow.Record, error) {
	// Create in-memory connector (empty DSN = in-memory). [web:30][web:42]
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create connector: %w", err)
	}
	defer connector.Close()

	// Single connection from connector (bypasses database/sql pooling). [web:42]
	conn, err := connector.Connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to connect duckdb: %w", err)
	}
	defer conn.Close()

	// Arrow interface from driver connection (Arrow build tag must be enabled).
	// go build -tags=duckdb_arrow
	arrowIface, err := duckdb.NewArrowFromConn(conn)
	if err != nil {
		return nil, fmt.Errorf("failed to create arrow interface: %w", err)
	}

	// Wrap the single Record as a RecordReader. [web:28]
	rdr, _ := array.NewRecordReader(batch.Schema(), []arrow.Record{batch})
	defer rdr.Release()

	// Register Arrow reader as view "input".
	release, err := arrowIface.RegisterView(rdr, "input")
	if err != nil {
		return nil, fmt.Errorf("failed to register arrow view: %w", err)
	}
	defer release()

	// Run SQL; get Arrow RecordReader as result. [web:28]
	resultReader, err := arrowIface.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("duckdb query failed: %w", err)
	}
	defer resultReader.Release()

	// No batches returned → empty record with same schema.
	if !resultReader.Next() {
		schema := resultReader.Schema()
		if schema == nil {
			schema = batch.Schema()
		}
		empty := array.NewRecordBatch(schema, nil, 0)
		return empty, nil
	}

	// Get first batch; retain so caller owns it.
	out := resultReader.RecordBatch()
	out.Retain()
	return out, nil
}

// singleRecordReader wraps an arrow.RecordBatch to implement array.RecordReader
type singleRecordReader struct {
	record arrow.RecordBatch
	read   bool
}

func (r *singleRecordReader) Schema() *arrow.Schema {
	return r.record.Schema()
}

func (r *singleRecordReader) Record() arrow.RecordBatch {
	// Deprecated: Use RecordBatch() instead
	return r.record
}

func (r *singleRecordReader) RecordBatch() arrow.RecordBatch {
	return r.record
}

func (r *singleRecordReader) Next() bool {
	if !r.read {
		r.read = true
		return true
	}
	return false
}

func (r *singleRecordReader) Retain() {
	r.record.Retain()
}

func (r *singleRecordReader) Release() {
	r.record.Release()
}

func (r *singleRecordReader) Err() error {
	return nil
}
