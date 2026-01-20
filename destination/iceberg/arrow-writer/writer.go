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
	"github.com/datazip-inc/olake/destination/iceberg/proto"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/typeutils"
)

type ArrowWriter struct {
	fileschemajson map[string]string // file type -> iceberg schema JSON
	schema         map[string]string
	arrowSchema    map[string]*arrow.Schema // file type -> arrow schema
	allocator      memory.Allocator
	stream         types.StreamInterface
	server         internal.ServerClient
	partitionInfo  []internal.PartitionInfo
	writers        map[string]*Writer
	createdFiles   map[string]*PartitionFiles
	upsertMode     bool
}

type Writer struct {
	dataWriter             *RollingWriter
	equalityDeleteWriter   *RollingWriter
	positionalDeleteWriter *RollingWriter
	// stores the latest position of olake_id (thread level property)
	// a data writer might flush multiple data files during cdc, to properly handle duplicate _olake_id values across multiple data files, we only empty it when the thread closes
	olakeIDPosition   map[string]PositionalDelete
	data              []types.RawRecord
	equalityDeletes   []string
	positionalDeletes []PositionalDelete
}

type RollingWriter struct {
	fileType        string
	filePath        string
	currentWriter   *parquetWriter
	currentBuffer   *bytes.Buffer
	currentRowCount int64
	partitionValues []any
}

type PartitionFiles struct {
	DataFiles      []*proto.ArrowPayload_FileMetadata
	EqDeleteFiles  []*proto.ArrowPayload_FileMetadata
	PosDeleteFiles []*proto.ArrowPayload_FileMetadata
}

type PositionalDelete struct {
	FilePath string
	Position int64
}

func New(ctx context.Context, partitionInfo []internal.PartitionInfo, schema map[string]string, stream types.StreamInterface, server internal.ServerClient, upsertMode bool) (*ArrowWriter, error) {
	writer := &ArrowWriter{
		partitionInfo: partitionInfo,
		schema:        schema,
		stream:        stream,
		server:        server,
		arrowSchema:   make(map[string]*arrow.Schema),
		writers:       make(map[string]*Writer),
		createdFiles:  make(map[string]*PartitionFiles),
		upsertMode:    upsertMode,
	}

	if err := writer.initialize(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize: %s", err)
	}

	return writer, nil
}

// computes both partition key and typed values
func (w *ArrowWriter) getRecordPartition(record types.RawRecord, olakeTimestamp time.Time) (string, []any, error) {
	if len(w.partitionInfo) == 0 {
		return "", nil, nil
	}

	paths := make([]string, 0, len(w.partitionInfo))
	values := make([]any, 0, len(w.partitionInfo))

	for _, pInfo := range w.partitionInfo {
		colType, ok := w.schema[pInfo.Field]
		if !ok {
			return "", nil, fmt.Errorf("partition field %q not in schema", pInfo.Field)
		}

		fieldValue := utils.Ternary(pInfo.Field == constants.OlakeTimestamp, olakeTimestamp, record.Data[pInfo.Field])
		if colType == "timestamptz" {
			if ts, err := typeutils.ReformatDate(fieldValue, true); err == nil {
				fieldValue = ts
			}
		}

		pathStr, typedVal, err := TransformValue(fieldValue, pInfo.Transform, colType)
		if err != nil {
			return "", nil, fmt.Errorf("failed to get transformed value: %s", err)
		}

		paths = append(paths, ConstructColPath(pathStr, pInfo.Field, pInfo.Transform))
		values = append(values, typedVal)
	}

	return strings.Join(paths, "/"), values, nil
}

// getOrCreateWriter retrieves an existing writer for the partition key or creates a new one with all necessary rolling writers.
func (w *ArrowWriter) getOrCreateWriter(ctx context.Context, pKey string, values []any) (*Writer, error) {
	writer, exists := w.writers[pKey]
	if exists {
		return writer, nil
	}

	writer = &Writer{
		olakeIDPosition: make(map[string]PositionalDelete),
	}

	var err error
	if writer.dataWriter, err = w.createWriter(ctx, pKey, values, *w.arrowSchema[fileTypeData], fileTypeData); err != nil {
		return nil, err
	}

	if w.upsertMode {
		if writer.equalityDeleteWriter, err = w.createWriter(ctx, pKey, values, *w.arrowSchema[fileTypeEqualityDelete], fileTypeEqualityDelete); err != nil {
			return nil, err
		}
		if writer.positionalDeleteWriter, err = w.createWriter(ctx, pKey, values, *w.arrowSchema[fileTypePositionalDelete], fileTypePositionalDelete); err != nil {
			return nil, err
		}
	}

	w.writers[pKey] = writer
	return writer, nil
}

// extract partitions records and tracks deletes for upsert mode.
func (w *ArrowWriter) extract(ctx context.Context, records []types.RawRecord, olakeTimestamp time.Time) error {
	for _, rec := range records {
		pKey, values, err := w.getRecordPartition(rec, olakeTimestamp)
		if err != nil {
			return err
		}

		writer, err := w.getOrCreateWriter(ctx, pKey, values)
		if err != nil {
			return err
		}

		writer.data = append(writer.data, rec)

		// Track deletes for upsert operations (d, u, c all need delete handling)
		if w.upsertMode && (rec.OperationType == "d" || rec.OperationType == "u" || rec.OperationType == "c") {
			filePosition := writer.dataWriter.currentRowCount + int64(len(writer.data)-1)

			if _, exists := writer.olakeIDPosition[rec.OlakeID]; !exists {
				// first time, add to equality deletes and track position
				writer.equalityDeletes = append(writer.equalityDeletes, rec.OlakeID)
				writer.olakeIDPosition[rec.OlakeID] = PositionalDelete{
					FilePath: writer.dataWriter.filePath,
					Position: filePosition,
				}
			} else {
				// duplicates, add prev position to positional deletes (n-1 logic)
				// the latest (nth) occurrence is kept in the map but not added to deletes
				prev := writer.olakeIDPosition[rec.OlakeID]
				writer.positionalDeletes = append(writer.positionalDeletes, PositionalDelete{
					FilePath: prev.FilePath,
					Position: prev.Position,
				})
				writer.olakeIDPosition[rec.OlakeID] = PositionalDelete{
					FilePath: writer.dataWriter.filePath,
					Position: filePosition,
				}
			}
		}
	}

	return nil
}

func (w *ArrowWriter) Write(ctx context.Context, records []types.RawRecord) error {
	olakeTimestamp := time.Now().UTC() // for olake timestamp, set current timestamp
	var err error

	if err := w.extract(ctx, records, olakeTimestamp); err != nil {
		return fmt.Errorf("failed to partition data: %s", err)
	}

	for pKey, writer := range w.writers {
		if len(writer.data) == 0 {
			continue
		}

		if w.upsertMode {
			posRecord := createPositionalDeleteArrowRecord(writer.positionalDeletes, w.allocator, w.arrowSchema[fileTypePositionalDelete])
			if err := writer.positionalDeleteWriter.currentWriter.WriteBuffered(posRecord); err != nil {
				posRecord.Release()

				return fmt.Errorf("failed to write positional delete record: %s", err)
			}

			writer.positionalDeleteWriter.currentRowCount += posRecord.NumRows()
			posRecord.Release()

			if writer.positionalDeleteWriter, err = w.checkAndFlush(ctx, writer.positionalDeleteWriter, pKey); err != nil {
				return err
			}

			record := createDeleteArrowRecord(writer.equalityDeletes, w.allocator, w.arrowSchema[fileTypeEqualityDelete])
			if err := writer.equalityDeleteWriter.currentWriter.WriteBuffered(record); err != nil {
				record.Release()

				return fmt.Errorf("failed to write equality delete record: %s", err)
			}

			writer.equalityDeleteWriter.currentRowCount += record.NumRows()
			record.Release()

			if writer.equalityDeleteWriter, err = w.checkAndFlush(ctx, writer.equalityDeleteWriter, pKey); err != nil {
				return err
			}
		}

		record, err := createArrowRecord(writer.data, w.allocator, w.arrowSchema[fileTypeData], w.stream.NormalizationEnabled(), olakeTimestamp)
		if err != nil {
			return fmt.Errorf("failed to create arrow record: %s", err)
		}

		if err := writer.dataWriter.currentWriter.WriteBuffered(record); err != nil {
			record.Release()

			return fmt.Errorf("failed to write data record: %s", err)
		}

		writer.dataWriter.currentRowCount += record.NumRows()
		record.Release()

		if writer.dataWriter, err = w.checkAndFlush(ctx, writer.dataWriter, pKey); err != nil {
			return err
		}

		writer.data = writer.data[:0]
		writer.equalityDeletes = writer.equalityDeletes[:0]
		writer.positionalDeletes = writer.positionalDeletes[:0]
	}

	return nil
}

// checkAndFlush checks if file size threshold is reached and flushes if needed.
func (w *ArrowWriter) checkAndFlush(ctx context.Context, rw *RollingWriter, partitionKey string) (*RollingWriter, error) {
	// logic, sizeSoFar := actual File Size + current compressed data (RowGroupTotalBytesWritten())
	// we can find out current row group's compressed size even before completing the entire row group
	sizeSoFar := int64(rw.currentBuffer.Len()) + rw.currentWriter.RowGroupTotalBytesWritten()
	targetSize := utils.Ternary(rw.fileType == fileTypeData, targetDataFileSize, targetDeleteFileSize).(int64)
	if sizeSoFar < targetSize {
		return rw, nil
	}

	if err := w.flush(ctx, rw, partitionKey); err != nil {
		return nil, err
	}

	newWriter, err := w.newRollingWriter(ctx, *w.arrowSchema[rw.fileType], rw.fileType, rw.partitionValues, rw.filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create new rolling writer after flush: %s", err)
	}

	newFilePath, err := w.allocateFilePath(ctx, partitionKey)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate new file path after flush: %s", err)
	}
	newWriter.filePath = newFilePath

	return newWriter, nil
}

func (w *ArrowWriter) flush(ctx context.Context, rw *RollingWriter, partitionKey string) error {
	if rw.currentRowCount == 0 {
		_ = rw.currentWriter.Close()
		return nil
	}

	if err := rw.currentWriter.Close(); err != nil {
		return fmt.Errorf("failed to close writer during flush: %s", err)
	}

	if err := w.uploadFile(ctx, rw, partitionKey); err != nil {
		return fmt.Errorf("failed to upload parquet during flush: %s", err)
	}

	return nil
}

func (w *ArrowWriter) EvolveSchema(ctx context.Context, newSchema map[string]string) error {
	if err := w.completeWriters(ctx); err != nil {
		return fmt.Errorf("failed to flush writers during schema evolution: %s", err)
	}

	w.schema = newSchema

	if err := w.initialize(ctx); err != nil {
		return fmt.Errorf("failed to reinitialize with evolved schema: %s", err)
	}

	return nil
}

// Close flushes all writers and commits files to Iceberg.
func (w *ArrowWriter) Close(ctx context.Context) error {
	if err := w.completeWriters(ctx); err != nil {
		return fmt.Errorf("failed to close arrow writers: %s", err)
	}

	// Build ordered file list: equality deletes → data → positional deletes
	var orderedFiles []*proto.ArrowPayload_FileMetadata
	for _, pf := range w.createdFiles {
		orderedFiles = append(orderedFiles, pf.EqDeleteFiles...)
		orderedFiles = append(orderedFiles, pf.DataFiles...)
		orderedFiles = append(orderedFiles, pf.PosDeleteFiles...)
	}

	commitRequest := &proto.ArrowPayload{
		Type: proto.ArrowPayload_REGISTER_AND_COMMIT,
		Metadata: &proto.ArrowPayload_Metadata{
			ThreadId:      w.server.ServerID(),
			DestTableName: w.stream.GetDestinationTable(),
			FileMetadata:  orderedFiles,
		},
	}

	commitCtx, cancel := context.WithTimeout(ctx, constants.GRPCRequestTimeout)
	defer cancel()

	if _, err := w.server.SendClientRequest(commitCtx, commitRequest); err != nil {
		return fmt.Errorf("failed to commit arrow files: %s", err)
	}

	return nil
}

func (w *ArrowWriter) completeWriters(ctx context.Context) error {
	for partitionKey, writer := range w.writers {
		if writer == nil {
			continue
		}

		if w.upsertMode {
			if writer.equalityDeleteWriter != nil {
				if err := w.flush(ctx, writer.equalityDeleteWriter, partitionKey); err != nil {
					return err
				}
				writer.equalityDeleteWriter = nil
			}
			if writer.positionalDeleteWriter != nil {
				if err := w.flush(ctx, writer.positionalDeleteWriter, partitionKey); err != nil {
					return err
				}
				writer.positionalDeleteWriter = nil
			}
		}

		if writer.dataWriter != nil {
			if err := w.flush(ctx, writer.dataWriter, partitionKey); err != nil {
				return err
			}
			writer.dataWriter = nil
		}

		// Clear the writer state but keep the partition entry
		writer.data = nil
		writer.equalityDeletes = nil
		writer.positionalDeletes = nil
		writer.olakeIDPosition = make(map[string]PositionalDelete)
	}

	return nil
}

func (w *ArrowWriter) initialize(ctx context.Context) error {
	if err := w.fetchFileSchemaJSON(ctx); err != nil {
		return err
	}

	dataFieldIDs, err := parseFieldIDsFromIcebergSchema(w.fileschemajson[fileTypeData])
	if err != nil {
		return fmt.Errorf("failed to parse data schema field IDs: %s", err)
	}

	w.allocator = memory.NewGoAllocator()
	w.arrowSchema[fileTypeData] = arrow.NewSchema(createFields(w.schema, dataFieldIDs), nil)

	if w.upsertMode {
		if err := w.initializeDeleteSchemas(); err != nil {
			return err
		}
	}

	return nil
}

func (w *ArrowWriter) initializeDeleteSchemas() error {
	deleteFieldIDs, err := parseFieldIDsFromIcebergSchema(w.fileschemajson[fileTypeEqualityDelete])
	if err != nil {
		return fmt.Errorf("failed to parse delete schema field IDs: %s", err)
	}

	olakeIDFieldID, ok := deleteFieldIDs[constants.OlakeID]
	if !ok {
		return fmt.Errorf("_olake_id field not found in delete schema")
	}

	// Equality delete schema: just _olake_id
	w.arrowSchema[fileTypeEqualityDelete] = arrow.NewSchema([]arrow.Field{
		{
			Name:     constants.OlakeID,
			Type:     arrow.BinaryTypes.String,
			Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"PARQUET:field_id": fmt.Sprintf("%d", olakeIDFieldID),
			}),
		},
	}, nil)

	// Positional delete schema with Iceberg reserved field IDs
	// https://github.com/apache/iceberg/blob/38cc88136684a57b61be4ae0d2c1886eff742a28/core/src/main/java/org/apache/iceberg/MetadataColumns.java#L63-L75
	const (
		posDeleteFilePathFieldID = 2147483546 // Integer.MAX_VALUE - 101
		posDeletePosFieldID      = 2147483545 // Integer.MAX_VALUE - 102
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

func (w *ArrowWriter) createWriter(ctx context.Context, pKey string, values []any, schema arrow.Schema, fileType string) (*RollingWriter, error) {
	filePath, err := w.allocateFilePath(ctx, pKey)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate file path: %s", err)
	}

	rw, err := w.newRollingWriter(ctx, schema, fileType, values, filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create rolling writer: %s", err)
	}

	return rw, nil
}

func (w *ArrowWriter) newRollingWriter(ctx context.Context, arrowSchema arrow.Schema, fileType string, values []any, filePath string) (*RollingWriter, error) {
	buf := &bytes.Buffer{}

	kvMeta := make(metadata.KeyValueMetadata, 0)
	_ = kvMeta.Append("iceberg.schema", w.fileschemajson[fileType])

	if fileType == fileTypeEqualityDelete {
		_ = kvMeta.Append("delete-type", "equality")
		fieldIDStr, _ := arrowSchema.Field(0).Metadata.GetValue("PARQUET:field_id")
		_ = kvMeta.Append("delete-field-ids", fieldIDStr)
	}

	pqWriter, err := newParquetWriter(ctx, &arrowSchema, buf, getDefaultWriterProps(), kvMeta)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet writer: %s", err)
	}

	return &RollingWriter{
		fileType:        fileType,
		filePath:        filePath,
		currentWriter:   pqWriter,
		currentBuffer:   buf,
		currentRowCount: 0,
		partitionValues: values,
	}, nil
}

func (w *ArrowWriter) allocateFilePath(ctx context.Context, partitionKey string) (string, error) {
	request := &proto.ArrowPayload{
		Type: proto.ArrowPayload_FILEPATH,
		Metadata: &proto.ArrowPayload_Metadata{
			DestTableName: w.stream.GetDestinationTable(),
			ThreadId:      w.server.ServerID(),
		},
	}

	reqCtx, cancel := context.WithTimeout(ctx, constants.GRPCRequestTimeout)
	defer cancel()

	resp, err := w.server.SendClientRequest(reqCtx, request)
	if err != nil {
		return "", fmt.Errorf("failed to allocate file path: %s", err)
	}

	basePath := resp.(*proto.ArrowIngestResponse).GetResult()

	if partitionKey != "" {
		// Insert partition key into path: dir/file.parquet → dir/partitionKey/file.parquet
		idx := strings.LastIndex(basePath, "/")
		return basePath[:idx] + "/" + partitionKey + "/" + basePath[idx+1:], nil
	}

	return basePath, nil
}

func (w *ArrowWriter) uploadFile(ctx context.Context, rw *RollingWriter, partitionKey string) error {
	request := &proto.ArrowPayload{
		Type: proto.ArrowPayload_UPLOAD_FILE,
		Metadata: &proto.ArrowPayload_Metadata{
			DestTableName: w.stream.GetDestinationTable(),
			ThreadId:      w.server.ServerID(),
			FileUpload: &proto.ArrowPayload_FileUploadRequest{
				FileData: rw.currentBuffer.Bytes(),
				FilePath: rw.filePath,
			},
		},
	}

	uploadCtx, cancel := context.WithTimeout(ctx, constants.GRPCRequestTimeout)
	defer cancel()

	if _, err := w.server.SendClientRequest(uploadCtx, request); err != nil {
		return fmt.Errorf("failed to upload %s file: %s", rw.fileType, err)
	}

	protoPartitionValues, err := toProtoPartitionValues(rw.partitionValues)
	if err != nil {
		return fmt.Errorf("failed to convert partition values: %s", err)
	}

	fileMeta := &proto.ArrowPayload_FileMetadata{
		FileType:        rw.fileType,
		FilePath:        rw.filePath,
		RecordCount:     rw.currentRowCount,
		PartitionValues: protoPartitionValues,
	}

	pf, exists := w.createdFiles[partitionKey]
	if !exists {
		pf = &PartitionFiles{}
		w.createdFiles[partitionKey] = pf
	}

	switch rw.fileType {
	case fileTypeData:
		pf.DataFiles = append(pf.DataFiles, fileMeta)
	case fileTypeEqualityDelete:
		pf.EqDeleteFiles = append(pf.EqDeleteFiles, fileMeta)
	case fileTypePositionalDelete:
		pf.PosDeleteFiles = append(pf.PosDeleteFiles, fileMeta)
	}

	return nil
}

func (w *ArrowWriter) fetchFileSchemaJSON(ctx context.Context) error {
	request := &proto.ArrowPayload{
		Type: proto.ArrowPayload_JSONSCHEMA,
		Metadata: &proto.ArrowPayload_Metadata{
			DestTableName: w.stream.GetDestinationTable(),
			ThreadId:      w.server.ServerID(),
		},
	}

	schemaCtx, cancel := context.WithTimeout(ctx, constants.GRPCRequestTimeout)
	defer cancel()

	resp, err := w.server.SendClientRequest(schemaCtx, request)
	if err != nil {
		return fmt.Errorf("failed to fetch schema JSON from server: %s", err)
	}

	w.fileschemajson = resp.(*proto.ArrowIngestResponse).GetIcebergSchemas()
	return nil
}
