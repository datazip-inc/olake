// File: util.go — arrow utilities for parquet writing.

package arrowdst

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"

	arrowlib "github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/typeutils"
)

// ---------------------------------------------------------------------------
// BuildArrowRecord
//
// Converts a []types.RawRecord into a single arrowlib.Record. OlakeColumns
// wins over Data for system columns. nil values append null.
// ---------------------------------------------------------------------------

func BuildArrowRecord(records []types.RawRecord, arrSchema *arrowlib.Schema, alloc memory.Allocator) (arrowlib.Record, error) {
	if alloc == nil {
		alloc = memory.NewGoAllocator()
	}
	builder := array.NewRecordBuilder(alloc, arrSchema)
	defer builder.Release()

	for _, rec := range records {
		for i, field := range arrSchema.Fields() {
			val, ok := rec.OlakeColumns[field.Name]
			if !ok {
				val = rec.Data[field.Name]
			}
			if val == nil {
				builder.Field(i).AppendNull()
				continue
			}
			if err := appendValueToBuilder(builder.Field(i), val); err != nil {
				return nil, fmt.Errorf("append %q: %s", field.Name, err)
			}
		}
	}
	return builder.NewRecord(), nil
}

func appendValueToBuilder(b array.Builder, val any) error {
	switch typed := b.(type) {
	case *array.BooleanBuilder:
		v, err := typeutils.ReformatBool(val)
		if err != nil {
			return err
		}
		typed.Append(v)
	case *array.Int32Builder:
		v, err := typeutils.ReformatInt32(val)
		if err != nil {
			return err
		}
		typed.Append(v)
	case *array.Int64Builder:
		v, err := typeutils.ReformatInt64(val)
		if err != nil {
			return err
		}
		typed.Append(v)
	case *array.Float32Builder:
		v, err := typeutils.ReformatFloat32(val)
		if err != nil {
			return err
		}
		typed.Append(v)
	case *array.Float64Builder:
		v, err := typeutils.ReformatFloat64(val)
		if err != nil {
			return err
		}
		typed.Append(v)
	case *array.TimestampBuilder:
		ts, err := typeutils.ReformatDate(val, true)
		if err != nil {
			return err
		}
		typed.Append(arrowlib.Timestamp(ts.UnixMicro()))
	case *array.StringBuilder:
		if m, ok := val.(map[string]any); ok {
			blob, err := json.Marshal(m)
			if err != nil {
				return err
			}
			typed.Append(string(blob))
		} else {
			typed.Append(fmt.Sprint(val))
		}
	default:
		return fmt.Errorf("unsupported arrow builder type %T", b)
	}
	return nil
}

// ---------------------------------------------------------------------------
// ParquetWriter
//
// Wraps parquet file.Writer and writes arrowlib.Records column-by-column
// via pqarrow.WriteArrowToColumn. Schema conversion preserves field-ID
// metadata and logical types exactly as Iceberg requires.
// ---------------------------------------------------------------------------

type ParquetWriter struct {
	wr     *file.Writer
	schema *arrowlib.Schema
	rgw    file.BufferedRowGroupWriter
	ctx    context.Context
	closed bool
}

func NewParquetWriter(ctx context.Context, arrSchema *arrowlib.Schema, w io.Writer,
	writerOpts []parquet.WriterProperty, kvMeta metadata.KeyValueMetadata,
) (*ParquetWriter, error) {
	props := parquet.NewWriterProperties(writerOpts...)
	pqSchema, err := arrowToParquetSchema(arrSchema)
	if err != nil {
		return nil, err
	}
	baseWriter := file.NewParquetWriter(w, pqSchema.Root(),
		file.WithWriterProps(props),
		file.WithWriteMetadata(kvMeta))
	return &ParquetWriter{
		wr:     baseWriter,
		schema: arrSchema,
		ctx:    pqarrow.NewArrowWriteContext(ctx, nil),
	}, nil
}

func (p *ParquetWriter) newBufferedRowGroup() {
	if p.rgw != nil {
		_ = p.rgw.Close()
	}
	p.rgw = p.wr.AppendBufferedRowGroup()
}

func (p *ParquetWriter) WriteBuffered(rec arrowlib.Record) error {
	if p.rgw == nil {
		p.newBufferedRowGroup()
	}
	numRows := int(rec.NumRows())
	for colIdx := 0; colIdx < int(rec.NumCols()); colIdx++ {
		col := rec.Column(colIdx)
		field := p.schema.Field(colIdx)

		cw, err := p.rgw.Column(colIdx)
		if err != nil {
			return fmt.Errorf("get column writer %d: %s", colIdx, err)
		}

		var defLevels []int16
		if field.Nullable {
			defLevels = make([]int16, numRows)
			for i := 0; i < numRows; i++ {
				if col.IsNull(i) {
					defLevels[i] = 0
				} else {
					defLevels[i] = 1
				}
			}
		}

		if err := pqarrow.WriteArrowToColumn(p.ctx, cw, col, defLevels, nil, field.Nullable); err != nil {
			return fmt.Errorf("write column %d (%s): %s", colIdx, field.Name, err)
		}
	}
	return nil
}

func (p *ParquetWriter) RowGroupTotalBytesWritten() int64 {
	if p.rgw == nil {
		return 0
	}
	return p.rgw.TotalBytesWritten()
}

func (p *ParquetWriter) Close() error {
	if p.closed {
		return nil
	}
	p.closed = true
	if p.rgw != nil {
		if err := p.rgw.Close(); err != nil {
			return err
		}
	}
	return p.wr.Close()
}

// ---------------------------------------------------------------------------
// DefaultParquetWriterProps — Iceberg-compatible defaults.
// ---------------------------------------------------------------------------

func DefaultParquetWriterProps() []parquet.WriterProperty {
	return []parquet.WriterProperty{
		parquet.WithCompression(compress.Codecs.Zstd),
		parquet.WithCompressionLevel(3),
		parquet.WithDataPageSize(1 * 1024 * 1024),
		parquet.WithDictionaryPageSizeLimit(2 * 1024 * 1024),
		parquet.WithDictionaryDefault(true),
		parquet.WithBatchSize(20000),
		parquet.WithStats(true),
		parquet.WithVersion(parquet.V1_0),
		parquet.WithRootName("table"),
	}
}

// ---------------------------------------------------------------------------
// RollingArrowWriter — callback-driven, size-based rolling parquet writer.
// ---------------------------------------------------------------------------

type RollingCallbacks struct {
	Allocate func(ctx context.Context, fileType string) (path string, err error)
	Persist  func(ctx context.Context, path, fileType string, data []byte,
		rowCount, sizeBytes int64) error
}

type RollingArrowWriter struct {
	fileType    string
	target      int64
	schema      *arrowlib.Schema
	kvMeta      metadata.KeyValueMetadata
	props       []parquet.WriterProperty
	cb          RollingCallbacks
	currentBuf  *bytes.Buffer
	currentWr   *ParquetWriter
	currentPath string
	rowCount    int64
}

func NewRollingArrowWriter(ctx context.Context, arrSchema *arrowlib.Schema, fileType string,
	target int64, kv metadata.KeyValueMetadata, cb RollingCallbacks,
) (*RollingArrowWriter, error) {
	r := &RollingArrowWriter{
		fileType: fileType,
		target:   target,
		schema:   arrSchema,
		kvMeta:   kv,
		props:    DefaultParquetWriterProps(),
		cb:       cb,
	}
	if err := r.openNext(ctx); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *RollingArrowWriter) openNext(ctx context.Context) error {
	path, err := r.cb.Allocate(ctx, r.fileType)
	if err != nil {
		return fmt.Errorf("allocate: %s", err)
	}
	r.currentPath = path
	r.currentBuf = &bytes.Buffer{}
	w, err := NewParquetWriter(ctx, r.schema, r.currentBuf, r.props, r.kvMeta)
	if err != nil {
		return fmt.Errorf("new parquet writer: %s", err)
	}
	r.currentWr = w
	r.rowCount = 0
	return nil
}

func (r *RollingArrowWriter) WriteRecord(ctx context.Context, rec arrowlib.Record) error {
	if err := r.currentWr.WriteBuffered(rec); err != nil {
		return fmt.Errorf("write buffered: %s", err)
	}
	r.rowCount += rec.NumRows()
	size := int64(r.currentBuf.Len()) + r.currentWr.RowGroupTotalBytesWritten()
	if size < r.target {
		return nil
	}
	if err := r.flushCurrent(ctx); err != nil {
		return err
	}
	return r.openNext(ctx)
}

func (r *RollingArrowWriter) flushCurrent(ctx context.Context) error {
	if r.rowCount == 0 {
		_ = r.currentWr.Close()
		return nil
	}
	if err := r.currentWr.Close(); err != nil {
		return fmt.Errorf("close writer: %s", err)
	}
	data := r.currentBuf.Bytes()
	return r.cb.Persist(ctx, r.currentPath, r.fileType, data, r.rowCount, int64(len(data)))
}

func (r *RollingArrowWriter) CurrentRowCount() int64 { return r.rowCount }
func (r *RollingArrowWriter) CurrentPath() string    { return r.currentPath }

// Close forces a final flush. 0-row files are NOT persisted.
func (r *RollingArrowWriter) Close(ctx context.Context) error {
	return r.flushCurrent(ctx)
}

// ---------------------------------------------------------------------------
// arrowToParquetSchema — manual conversion that preserves field-ID metadata
// and correct logical types for Iceberg.
// ---------------------------------------------------------------------------

func arrowToParquetSchema(arrSchema *arrowlib.Schema) (*schema.Schema, error) {
	nodes := make(schema.FieldList, 0, arrSchema.NumFields())
	for _, field := range arrSchema.Fields() {
		node, err := arrowFieldToParquet(field)
		if err != nil {
			return nil, fmt.Errorf("convert field %s: %s", field.Name, err)
		}
		nodes = append(nodes, node)
	}
	root, err := schema.NewGroupNode("table", parquet.Repetitions.Required, nodes, -1)
	if err != nil {
		return nil, fmt.Errorf("create root group node: %s", err)
	}
	return schema.NewSchema(root), nil
}

func arrowFieldToParquet(field arrowlib.Field) (schema.Node, error) {
	repetition := parquet.Repetitions.Required
	if field.Nullable {
		repetition = parquet.Repetitions.Optional
	}

	fieldID := int32(-1)
	if field.Metadata.Len() > 0 {
		if idStr, ok := field.Metadata.GetValue("PARQUET:field_id"); ok {
			if id, err := strconv.ParseInt(idStr, 10, 32); err == nil {
				fieldID = int32(id)
			}
		}
	}

	var pqType parquet.Type
	var logicalType schema.LogicalType
	var typeLength int32 = -1

	switch field.Type.ID() {
	case arrowlib.BOOL:
		pqType = parquet.Types.Boolean
	case arrowlib.INT32:
		pqType = parquet.Types.Int32
	case arrowlib.INT64:
		pqType = parquet.Types.Int64
	case arrowlib.FLOAT32:
		pqType = parquet.Types.Float
	case arrowlib.FLOAT64:
		pqType = parquet.Types.Double
	case arrowlib.STRING:
		pqType = parquet.Types.ByteArray
		logicalType = schema.StringLogicalType{}
	case arrowlib.TIMESTAMP:
		pqType = parquet.Types.Int64
		tsType := field.Type.(*arrowlib.TimestampType)
		adjustedToUTC := tsType.TimeZone != ""
		logicalType = schema.NewTimestampLogicalType(adjustedToUTC, schema.TimeUnitMicros)
	default:
		pqType = parquet.Types.ByteArray
		logicalType = schema.StringLogicalType{}
	}

	if logicalType != nil {
		return schema.NewPrimitiveNodeLogical(field.Name, repetition, logicalType, pqType, int(typeLength), fieldID)
	}
	return schema.NewPrimitiveNode(field.Name, repetition, pqType, fieldID, typeLength)
}
