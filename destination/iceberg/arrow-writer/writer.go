package arrowwriter

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination/iceberg/internal"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/typeutils"
)

// ArrowWriter is the per-thread writer for the iceberg destination's
// arrow-writer code path. It produces parquet files locally via Arrow's
// Parquet writer (unchanged from the legacy Java-backed flow) and then
// uses an iceberg-go Backend (see iceberg_backend.go) to upload the
// bytes and commit the resulting iceberg.DataFile descriptors. No Java
// sink is involved on this path.
type ArrowWriter struct {
	backend       *Backend
	schema        map[string]string        // OLake column->iceberg type map
	arrowSchema   map[string]*arrow.Schema // file type -> arrow schema
	allocator     memory.Allocator
	stream        types.StreamInterface
	partitionInfo []internal.PartitionInfo
	writers       map[string]*partitionWriters
	upsertMode    bool

	// Identifier (typically _olake_id) field ID, used by the equality-
	// delete data-file builder.
	identifierFieldID int
}

// partitionWriters holds the rolling writers and pending records for a
// single partition key.
type partitionWriters struct {
	dataWriter             *RollingWriter
	equalityDeleteWriter   *RollingWriter
	positionalDeleteWriter *RollingWriter
	// olakeIDPosition tracks the latest position recorded for an
	// _olake_id across all flushed data files of this partition. Kept
	// across flushes so duplicates emitted later still produce the
	// correct positional delete tuples.
	olakeIDPosition   map[string]PositionalDelete
	data              []types.RawRecord
	equalityDeletes   []string
	positionalDeletes []PositionalDelete
}

// RollingWriter wraps a single in-flight parquet writer and the buffer
// it streams its bytes into.
type RollingWriter struct {
	fileType        string
	filePath        string
	currentWriter   *parquetWriter
	currentBuffer   *bytes.Buffer
	currentRowCount int64
	partitionValues []any
}

// PositionalDelete is the (data-file path, row-position) tuple emitted
// for an upsert deletion of an _olake_id that was previously written to
// the same partition.
type PositionalDelete struct {
	FilePath string
	Position int64
}

// New constructs an ArrowWriter wired to the supplied iceberg-go Backend.
//
// schema is the column->iceberg-type map for this thread (typically the
// one Backend.ColumnTypeMap() returned at setup time). The writer takes
// a defensive copy so subsequent EvolveSchema calls do not mutate the
// caller's view.
func New(
	ctx context.Context,
	partitionInfo []internal.PartitionInfo,
	schema map[string]string,
	stream types.StreamInterface,
	backend *Backend,
	upsertMode bool,
	identifierFieldID int,
) (*ArrowWriter, error) {
	w := &ArrowWriter{
		backend:           backend,
		partitionInfo:     partitionInfo,
		schema:            copySchemaMap(schema),
		stream:            stream,
		upsertMode:        upsertMode,
		identifierFieldID: identifierFieldID,
		arrowSchema:       make(map[string]*arrow.Schema),
		writers:           make(map[string]*partitionWriters),
	}
	if err := w.initialize(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize arrow writer: %s", err)
	}
	return w, nil
}

// Backend exposes the underlying iceberg-go backend so the parent
// iceberg package can drive lifecycle operations (commit, evolve)
// without poking at private fields.
func (w *ArrowWriter) Backend() *Backend { return w.backend }

// getRecordPartition computes both the partition path string and the
// typed partition value tuple for a single record.
func (w *ArrowWriter) getRecordPartition(record types.RawRecord, olakeTimestamp time.Time) (string, []any, error) {
	if len(w.partitionInfo) == 0 {
		return "", nil, nil
	}

	paths := make([]string, 0, len(w.partitionInfo))
	values := make([]any, 0, len(w.partitionInfo))

	for _, pInfo := range w.partitionInfo {
		colType, ok := w.schema[pInfo.SchemaField]
		if !ok {
			return "", nil, fmt.Errorf("partition field %q not in schema", pInfo.SchemaField)
		}

		fieldValue := utils.Ternary(pInfo.Field == constants.OlakeTimestamp, olakeTimestamp, record.Data[pInfo.SchemaField])
		if colType == "timestamptz" {
			if ts, err := typeutils.ReformatDate(fieldValue, true); err == nil {
				fieldValue = ts
			}
		}

		pathStr, typedVal, err := TransformValue(fieldValue, pInfo.Transform, colType)
		if err != nil {
			return "", nil, fmt.Errorf("failed to get transformed value: %s", err)
		}

		paths = append(paths, ConstructColPath(pathStr, pInfo.SchemaField, pInfo.Transform))
		values = append(values, typedVal)
	}

	return strings.Join(paths, "/"), values, nil
}

func (w *ArrowWriter) getOrCreateWriter(ctx context.Context, pKey string, values []any) (*partitionWriters, error) {
	pw, exists := w.writers[pKey]
	if !exists {
		pw = &partitionWriters{olakeIDPosition: make(map[string]PositionalDelete)}
	}

	var err error
	if pw.dataWriter == nil {
		if pw.dataWriter, err = w.createWriter(ctx, pKey, values, w.arrowSchema[fileTypeData], fileTypeData); err != nil {
			return nil, err
		}
	}

	if w.upsertMode {
		if pw.equalityDeleteWriter == nil {
			if pw.equalityDeleteWriter, err = w.createWriter(ctx, pKey, values, w.arrowSchema[fileTypeEqualityDelete], fileTypeEqualityDelete); err != nil {
				return nil, err
			}
		}
		if pw.positionalDeleteWriter == nil {
			if pw.positionalDeleteWriter, err = w.createWriter(ctx, pKey, values, w.arrowSchema[fileTypePositionalDelete], fileTypePositionalDelete); err != nil {
				return nil, err
			}
		}
	}

	w.writers[pKey] = pw
	return pw, nil
}

// extract partitions records and tracks deletes for upsert mode
// ("d"/"u"/"i" only; "c"/"r" skip dedup).
func (w *ArrowWriter) extract(ctx context.Context, records []types.RawRecord) error {
	for _, rec := range records {
		pKey, values, err := w.getRecordPartition(rec, rec.OlakeColumns[constants.OlakeTimestamp].(time.Time))
		if err != nil {
			return err
		}

		pw, err := w.getOrCreateWriter(ctx, pKey, values)
		if err != nil {
			return err
		}

		pw.data = append(pw.data, rec)
		recordOpType := rec.OlakeColumns[constants.OpType].(string)
		recordOlakeID := rec.OlakeColumns[constants.OlakeID].(string)
		if w.upsertMode && (recordOpType == "d" || recordOpType == "u" || recordOpType == "i") {
			filePosition := pw.dataWriter.currentRowCount + int64(len(pw.data)-1)

			if _, exists := pw.olakeIDPosition[recordOlakeID]; !exists {
				pw.equalityDeletes = append(pw.equalityDeletes, recordOlakeID)
				pw.olakeIDPosition[recordOlakeID] = PositionalDelete{
					FilePath: pw.dataWriter.filePath,
					Position: filePosition,
				}
			} else {
				prev := pw.olakeIDPosition[recordOlakeID]
				pw.positionalDeletes = append(pw.positionalDeletes, PositionalDelete{
					FilePath: prev.FilePath,
					Position: prev.Position,
				})
				pw.olakeIDPosition[recordOlakeID] = PositionalDelete{
					FilePath: pw.dataWriter.filePath,
					Position: filePosition,
				}
			}
		}

		// Normalise "i" → "c" so downstream consumers see a consistent op type.
		if recordOpType == "i" {
			rec.OlakeColumns[constants.OpType] = "c"
		}
	}
	return nil
}

func (w *ArrowWriter) Write(ctx context.Context, records []types.RawRecord) error {
	var err error

	if err := w.extract(ctx, records); err != nil {
		return fmt.Errorf("failed to partition data: %s", err)
	}

	for pKey, pw := range w.writers {
		if len(pw.data) == 0 {
			continue
		}

		if w.upsertMode {
			posRecord := createPositionalDeleteArrowRecord(pw.positionalDeletes, w.allocator, w.arrowSchema[fileTypePositionalDelete])
			if err := pw.positionalDeleteWriter.currentWriter.WriteBuffered(posRecord); err != nil {
				posRecord.Release()
				return fmt.Errorf("failed to write positional delete record: %s", err)
			}
			pw.positionalDeleteWriter.currentRowCount += posRecord.NumRows()
			posRecord.Release()
			if pw.positionalDeleteWriter, err = w.checkAndFlush(ctx, pw.positionalDeleteWriter, pKey); err != nil {
				return err
			}

			eqRecord := createDeleteArrowRecord(pw.equalityDeletes, w.allocator, w.arrowSchema[fileTypeEqualityDelete])
			if err := pw.equalityDeleteWriter.currentWriter.WriteBuffered(eqRecord); err != nil {
				eqRecord.Release()
				return fmt.Errorf("failed to write equality delete record: %s", err)
			}
			pw.equalityDeleteWriter.currentRowCount += eqRecord.NumRows()
			eqRecord.Release()
			if pw.equalityDeleteWriter, err = w.checkAndFlush(ctx, pw.equalityDeleteWriter, pKey); err != nil {
				return err
			}
		}

		dataRecord, err := createArrowRecord(pw.data, w.allocator, w.arrowSchema[fileTypeData])
		if err != nil {
			return fmt.Errorf("failed to create arrow record: %s", err)
		}
		if err := pw.dataWriter.currentWriter.WriteBuffered(dataRecord); err != nil {
			dataRecord.Release()
			return fmt.Errorf("failed to write data record: %s", err)
		}
		pw.dataWriter.currentRowCount += dataRecord.NumRows()
		dataRecord.Release()

		if pw.dataWriter, err = w.checkAndFlush(ctx, pw.dataWriter, pKey); err != nil {
			return err
		}

		pw.data = pw.data[:0]
		pw.equalityDeletes = pw.equalityDeletes[:0]
		pw.positionalDeletes = pw.positionalDeletes[:0]
	}

	return nil
}

// checkAndFlush flushes the rolling writer when its accumulated bytes
// hit the configured target size and rotates in a fresh writer.
func (w *ArrowWriter) checkAndFlush(ctx context.Context, rw *RollingWriter, partitionKey string) (*RollingWriter, error) {
	sizeSoFar := int64(rw.currentBuffer.Len()) + rw.currentWriter.RowGroupTotalBytesWritten()
	targetSize := utils.Ternary(rw.fileType == fileTypeData, targetDataFileSize, targetDeleteFileSize).(int64)
	if sizeSoFar < targetSize {
		return rw, nil
	}

	if err := w.flush(ctx, rw, partitionKey); err != nil {
		return nil, err
	}

	newWriter, err := w.newRollingWriter(ctx, w.arrowSchema[rw.fileType], rw.fileType, rw.partitionValues, "")
	if err != nil {
		return nil, fmt.Errorf("failed to create new rolling writer after flush: %s", err)
	}
	return newWriter, nil
}

// flush closes the in-flight parquet writer, uploads the parquet bytes
// to object storage via the backend, and registers a iceberg.DataFile
// descriptor for later commit.
func (w *ArrowWriter) flush(ctx context.Context, rw *RollingWriter, partitionKey string) error {
	_ = partitionKey // path is already encoded into rw.filePath; key kept for symmetry / future logging.

	if rw.currentRowCount == 0 {
		_ = rw.currentWriter.Close()
		return nil
	}

	if err := rw.currentWriter.Close(); err != nil {
		return fmt.Errorf("failed to close writer during flush: %s", err)
	}

	pqMeta, err := rw.currentWriter.FileMetadata()
	if err != nil {
		return fmt.Errorf("failed to read parquet file metadata: %s", err)
	}

	parquetBytes := rw.currentBuffer.Bytes()
	if err := w.backend.UploadFile(rw.filePath, parquetBytes); err != nil {
		return fmt.Errorf("failed to upload parquet during flush: %s", err)
	}

	switch rw.fileType {
	case fileTypeData:
		if err := w.backend.AddDataFile(rw.filePath, int64(len(parquetBytes)), pqMeta, rw.partitionValues); err != nil {
			return fmt.Errorf("failed to register data file: %s", err)
		}
	case fileTypeEqualityDelete:
		eqIDs := []int{w.identifierFieldID}
		if err := w.backend.AddEqualityDeleteFile(rw.filePath, int64(len(parquetBytes)), pqMeta, rw.partitionValues, eqIDs); err != nil {
			return fmt.Errorf("failed to register equality delete file: %s", err)
		}
	case fileTypePositionalDelete:
		if err := w.backend.AddPositionalDeleteFile(rw.filePath, int64(len(parquetBytes)), pqMeta, rw.partitionValues); err != nil {
			return fmt.Errorf("failed to register positional delete file: %s", err)
		}
	default:
		return fmt.Errorf("unknown rolling writer file type %q", rw.fileType)
	}
	return nil
}

// EvolveSchema flushes the in-flight writers, asks the backend to
// evolve the table schema, then re-initializes Arrow schemas from the
// updated table.
func (w *ArrowWriter) EvolveSchema(ctx context.Context, newSchema map[string]string) error {
	if err := w.completeWriters(ctx); err != nil {
		return fmt.Errorf("failed to flush writers during schema evolution: %s", err)
	}

	if err := w.backend.EvolveSchema(ctx, newSchema); err != nil {
		return fmt.Errorf("failed to evolve iceberg-go schema: %s", err)
	}

	w.schema = w.backend.ColumnTypeMap()
	if err := w.initialize(ctx); err != nil {
		return fmt.Errorf("failed to reinitialize with evolved schema: %s", err)
	}
	return nil
}

// Close flushes any open writers and commits accumulated data/delete
// files via the backend (single iceberg-go transaction).
func (w *ArrowWriter) Close(ctx context.Context, finalMetadataState any) error {
	_ = finalMetadataState // captured-cdc-pos is not yet persisted on the iceberg-go path.

	if err := w.completeWriters(ctx); err != nil {
		return fmt.Errorf("failed to close arrow writers: %s", err)
	}
	if err := w.backend.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit arrow files: %s", err)
	}
	return nil
}

func (w *ArrowWriter) completeWriters(ctx context.Context) error {
	for partitionKey, pw := range w.writers {
		if pw == nil {
			continue
		}

		if w.upsertMode {
			if pw.equalityDeleteWriter != nil {
				if err := w.flush(ctx, pw.equalityDeleteWriter, partitionKey); err != nil {
					return err
				}
				pw.equalityDeleteWriter = nil
			}
			if pw.positionalDeleteWriter != nil {
				if err := w.flush(ctx, pw.positionalDeleteWriter, partitionKey); err != nil {
					return err
				}
				pw.positionalDeleteWriter = nil
			}
		}

		if pw.dataWriter != nil {
			if err := w.flush(ctx, pw.dataWriter, partitionKey); err != nil {
				return err
			}
			pw.dataWriter = nil
		}

		// Reset state but keep the partition entry so olakeIDPosition
		// continues to track positions across schema-evolution boundaries.
		pw.data = nil
		pw.equalityDeletes = nil
		pw.positionalDeletes = nil
	}
	return nil
}

// initialize builds the arrow schemas (data + delete) from the
// iceberg-go table schema. The data schema's PARQUET:field_id metadata
// is sourced directly from the table's NestedFields.
func (w *ArrowWriter) initialize(ctx context.Context) error {
	_ = ctx

	w.allocator = memory.NewGoAllocator()

	dataFieldIDs := w.dataFieldIDs()
	w.arrowSchema[fileTypeData] = arrow.NewSchema(createFields(w.schema, dataFieldIDs), nil)

	if w.upsertMode {
		if err := w.initializeDeleteSchemas(); err != nil {
			return err
		}
	}
	return nil
}

func (w *ArrowWriter) dataFieldIDs() map[string]int32 {
	out := make(map[string]int32)
	for _, f := range w.backend.IcebergSchema().Fields() {
		out[f.Name] = int32(f.ID)
	}
	return out
}

func (w *ArrowWriter) initializeDeleteSchemas() error {
	if w.identifierFieldID == 0 {
		return fmt.Errorf("upsert mode requires an identifier field id")
	}

	// Equality delete schema: just _olake_id, with the actual table
	// field ID stamped into the parquet metadata.
	w.arrowSchema[fileTypeEqualityDelete] = arrow.NewSchema([]arrow.Field{
		{
			Name:     constants.OlakeID,
			Type:     arrow.BinaryTypes.String,
			Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"PARQUET:field_id": fmt.Sprintf("%d", w.identifierFieldID),
			}),
		},
	}, nil)

	// Positional delete schema with Iceberg reserved field IDs:
	// https://github.com/apache/iceberg/blob/38cc88136684a57b61be4ae0d2c1886eff742a28/core/src/main/java/org/apache/iceberg/MetadataColumns.java#L63-L75
	const (
		posDeleteFilePathFieldID = 2147483546
		posDeletePosFieldID      = 2147483545
	)
	w.arrowSchema[fileTypePositionalDelete] = arrow.NewSchema([]arrow.Field{
		{
			Name:     "file_path",
			Type:     arrow.BinaryTypes.String,
			Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"PARQUET:field_id": fmt.Sprintf("%d", posDeleteFilePathFieldID),
			}),
		},
		{
			Name:     "pos",
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"PARQUET:field_id": fmt.Sprintf("%d", posDeletePosFieldID),
			}),
		},
	}, nil)

	return nil
}

func (w *ArrowWriter) createWriter(ctx context.Context, pKey string, values []any, schema *arrow.Schema, fileType string) (*RollingWriter, error) {
	rw, err := w.newRollingWriter(ctx, schema, fileType, values, pKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create rolling writer: %s", err)
	}
	return rw, nil
}

// newRollingWriter creates a fresh in-memory parquet writer destined for
// a freshly-allocated <table>/data/<partition>/<uuid>.parquet path.
//
// pKeyHint is only used when allocating a brand-new path; pass empty
// string to derive the partition path from values.
func (w *ArrowWriter) newRollingWriter(ctx context.Context, arrowSchema *arrow.Schema, fileType string, values []any, pKeyHint string) (*RollingWriter, error) {
	_ = pKeyHint
	buf := &bytes.Buffer{}

	kvMeta := make(metadata.KeyValueMetadata, 0)
	if js := w.backend.SchemaJSON(fileType); js != "" {
		_ = kvMeta.Append("iceberg.schema", js)
	}

	if fileType == fileTypeEqualityDelete {
		_ = kvMeta.Append("delete-type", "equality")
		fieldIDStr, _ := arrowSchema.Field(0).Metadata.GetValue("PARQUET:field_id")
		_ = kvMeta.Append("delete-field-ids", fieldIDStr)
	}

	pqWriter, err := newParquetWriter(ctx, arrowSchema, buf, getDefaultWriterProps(), kvMeta)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet writer: %s", err)
	}

	partitionPath := w.backend.PartitionPath(values)
	filePath := w.backend.AllocateFilePath(partitionPath, fileType)

	return &RollingWriter{
		fileType:        fileType,
		filePath:        filePath,
		currentWriter:   pqWriter,
		currentBuffer:   buf,
		currentRowCount: 0,
		partitionValues: values,
	}, nil
}

func copySchemaMap(src map[string]string) map[string]string {
	out := make(map[string]string, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

