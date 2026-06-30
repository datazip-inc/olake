package parquetenc

import (
	"context"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/datazip-inc/olake/writers/encoder"
)

type parquetEncoder struct {
	baseWriter  *file.Writer
	counter     *countingWriter
	arrowSchema *arrow.Schema
	// arrowCtx carries pqarrow's writer context (scratch buffers + props). It is
	// created lazily on the first Write and reused across this file's row groups;
	// a Roller is single-writer, so sharing it is safe.
	arrowCtx context.Context
}

// countingWriter tallies bytes written to the sink and hides any io.Closer the
// wrapped writer implements. The latter matters because arrow-go's file.Writer
// closes its sink when the sink is an io.Closer — we keep sink closing/uploading
// with the Sink (Encoder.Close must not close the underlying writer).
type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// New builds an encoder.NewEncoder backed by arrow-go's parquet writer. Bind
// schema/opts/kvMeta once and hand the result to roller.NewRoller, which calls it
// for every file it opens:
//
//	enc := parquetenc.New(arrowSchema, opts, kvMeta)
//	r   := roller.NewRoller(sink, enc, convert, cfg)
func New(arrowSchema *arrow.Schema, writerOpts []parquet.WriterProperty, kvMeta metadata.KeyValueMetadata) encoder.NewEncoder {
	return func(output io.Writer) (encoder.Encoder, error) {
		props := parquet.NewWriterProperties(writerOpts...)
		parquetSchema, err := arrowToParquetSchema(arrowSchema)
		if err != nil {
			return nil, fmt.Errorf("failed to convert arrow schema to parquet schema: %w", err)
		}

		counter := &countingWriter{w: output}
		baseWriter := file.NewParquetWriter(counter, parquetSchema.Root(),
			file.WithWriterProps(props),
			file.WithWriteMetadata(kvMeta))

		return &parquetEncoder{
			baseWriter:  baseWriter,
			counter:     counter,
			arrowSchema: arrowSchema,
		}, nil
	}
}

// Write encodes the record as one parquet row group and closes it immediately,
// flushing its compressed bytes to the sink. This is what makes Size() a real
// signal — arrow-go reports 0 bytes for an open buffered row group until close —
// and it bounds memory to a single row group (one chunk) rather than the whole
// file. The Roller feeds one chunk per call, so one chunk == one row group.
func (p *parquetEncoder) Write(ctx context.Context, record arrow.Record) error {
	if record.NumRows() == 0 {
		return nil
	}
	if p.arrowCtx == nil {
		p.arrowCtx = pqarrow.NewArrowWriteContext(ctx, nil)
	}

	rgw := p.baseWriter.AppendBufferedRowGroup()
	numRows := int(record.NumRows())
	for colIdx := 0; colIdx < int(record.NumCols()); colIdx++ {
		col := record.Column(colIdx)
		field := p.arrowSchema.Field(colIdx)

		cw, err := rgw.Column(colIdx)
		if err != nil {
			return fmt.Errorf("failed to get column writer %d: %w", colIdx, err)
		}

		// for flat schemas, generate definition levels for nullable columns
		// 0 == value is NULL, 1 == value is present
		var defLevels []int16
		if field.Nullable {
			defLevels = make([]int16, numRows)
			for i := range numRows {
				if !col.IsNull(i) {
					defLevels[i] = 1
				}
			}
		}

		if err := pqarrow.WriteArrowToColumn(p.arrowCtx, cw, col, defLevels, nil, field.Nullable); err != nil {
			return fmt.Errorf("failed to write column %d (%s): %w", colIdx, field.Name, err)
		}
	}

	if err := rgw.Close(); err != nil {
		return fmt.Errorf("failed to close row group: %w", err)
	}
	return nil
}

// Size returns the compressed bytes flushed to the sink so far. Because Write
// closes each row group, this reflects the real on-disk file size as it grows.
func (p *parquetEncoder) Size() int64 {
	return p.counter.n
}

func (p *parquetEncoder) Close() error {
	return p.baseWriter.Close()
}
