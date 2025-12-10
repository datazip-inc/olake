package arrowwriter

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination/iceberg/internal"
	"github.com/datazip-inc/olake/destination/iceberg/proto"
	"github.com/datazip-inc/olake/types"
)

type ArrowWriter struct {
	icebergSchemaJSON string
	fields            []arrow.Field
	fieldIDs          map[string]int32
	partitionInfo     []internal.PartitionInfo
	schemaID          int
	schema            map[string]string
	stream            types.StreamInterface
	server            internal.ArrowServerClient
	createdFilePaths  []FileMetdata
	writers           sync.Map
}

type RollingWriter struct {
	currentWriter         *pqarrow.FileWriter
	currentBuffer         *bytes.Buffer
	currentRowCount       int64
	currentCompressedSize int64
}

type FileMetdata struct {
	FileType        string
	FilePath        string
	RecordCount     int64
	EqualityFieldID *int32 // Only setting for equality delete files
}

type FileUploadData struct {
	FileType        string
	FileData        []byte
	PartitionKey    string
	EqualityFieldID int32
	RecordCount     int64
}

func New(ctx context.Context, partitionInfo []internal.PartitionInfo, schema map[string]string, stream types.StreamInterface, server internal.ArrowServerClient) (*ArrowWriter, error) {
	writer := &ArrowWriter{
		partitionInfo: partitionInfo,
		schema:        schema,
		stream:        stream,
		server:        server,
	}

	schemaID, err := writer.getschemaID(ctx)
	if err != nil {
		return nil, err
	}

	writer.schemaID = schemaID
	if err := writer.initializeFields(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize fields: %w", err)
	}

	return writer, nil
}

func (w *ArrowWriter) createPartitionKey(record types.RawRecord) (string, error) {
	paths := make([]string, 0, len(w.partitionInfo))

	for _, partition := range w.partitionInfo {
		field, transform := partition.Field, partition.Transform
		colType := w.schema[field]

		transformedVal, err := internal.TransformValue(record.Data[field], transform, colType)
		if err != nil {
			return "", err
		}

		colPath := internal.ConstructColPath(transformedVal.(string), field, transform)
		paths = append(paths, colPath)
	}

	partitionKey := strings.Join(paths, "/")

	return partitionKey, nil
}

func (w *ArrowWriter) extract(records []types.RawRecord) (map[string][]types.RawRecord, map[string][]types.RawRecord, error) {
	// data and delete record maps to store (file path : type.RawRecord)
	data := make(map[string][]types.RawRecord)
	deletes := make(map[string][]types.RawRecord)

	for _, rec := range records {
		pKey := ""
		if len(w.partitionInfo) != 0 {
			key, err := w.createPartitionKey(rec)
			if err != nil {
				return nil, nil, err
			}
			pKey = key
		}

		if rec.OperationType == "d" || rec.OperationType == "u" || rec.OperationType == "c" {
			del := extractDeleteRecord(rec)
			deletes[pKey] = append(deletes[pKey], del)
		}

		data[pKey] = append(data[pKey], rec)
	}

	return data, deletes, nil
}

func (w *ArrowWriter) Write(ctx context.Context, records []types.RawRecord) error {
	data, deletes, err := w.extract(records)
	if err != nil {
		return fmt.Errorf("failed to partition data: %v", err)
	}

	for pKey, record := range deletes {
		record, err := createDeleteArrowRec(record, w.fieldIDs[constants.OlakeID])
		if err != nil {
			return fmt.Errorf("failed to create arrow record: %v", err)
		}

		writer, err := w.getOrCreateWriter(pKey, *record.Schema(), "delete")
		if err != nil {
			record.Release()
			return fmt.Errorf("failed to get or create writer for delete: %w", err)
		}
		if err := writer.currentWriter.WriteBuffered(record); err != nil {
			record.Release()
			return fmt.Errorf("failed to write delete record: %v", err)
		}
		writer.currentRowCount += record.NumRows()
		record.Release()

		if shouldFlush, err := w.checkAndFlush(ctx, writer, pKey, "delete"); err != nil {
			return err
		} else if shouldFlush {
			w.writers.Delete("delete:" + pKey)
		}
	}

	for pKey, record := range data {
		record, err := CreateArrowRec(record, w.fields, w.stream.NormalizationEnabled())
		if err != nil {
			return fmt.Errorf("failed to create arrow record: %v", err)
		}

		writer, err := w.getOrCreateWriter(pKey, *record.Schema(), "data")
		if err != nil {
			record.Release()
			return fmt.Errorf("failed to get or create writer for data: %w", err)
		}
		if err := writer.currentWriter.WriteBuffered(record); err != nil {
			record.Release()
			return fmt.Errorf("failed to write data record: %w", err)
		}
		writer.currentRowCount += record.NumRows()
		record.Release()

		if shouldFlush, err := w.checkAndFlush(ctx, writer, pKey, "data"); err != nil {
			return err
		} else if shouldFlush {
			w.writers.Delete("data:" + pKey)
		}
	}

	return nil
}

func (w *ArrowWriter) checkAndFlush(ctx context.Context, writer *RollingWriter, partitionKey string, fileType string) (bool, error) {
	// current size: all previously flushed row groups (in buffer) + current in-progress row group (buffered in memory)
	sizeSoFar := int64(writer.currentBuffer.Len()) + writer.currentWriter.RowGroupTotalBytesWritten()
	writer.currentCompressedSize = sizeSoFar

	targetFileSize := targetDataFileSize
	if fileType == "delete" {
		targetFileSize = targetDeleteFileSize
	}

	if writer.currentCompressedSize >= targetFileSize {
		if err := writer.currentWriter.Close(); err != nil {
			return false, fmt.Errorf("failed to close writer during flush: %w", err)
		}

		fileData := make([]byte, writer.currentBuffer.Len())
		copy(fileData, writer.currentBuffer.Bytes())

		uploadData := &FileUploadData{
			FileType:        fileType,
			FileData:        fileData,
			PartitionKey:    partitionKey,
			EqualityFieldID: int32(w.fieldIDs[constants.OlakeID]),
			RecordCount:     writer.currentRowCount,
		}

		if err := w.uploadFile(ctx, uploadData); err != nil {
			return false, fmt.Errorf("failed to upload parquet during flush: %w", err)
		}

		return true, nil
	}

	return false, nil
}

func (w *ArrowWriter) Close(ctx context.Context) error {
	err := w.closeWriters(ctx)
	if err != nil {
		return fmt.Errorf("failed to close arrow writers: %v", err)
	}

	fileMetadata := make([]*proto.ArrowPayload_FileMetadata, 0, len(w.createdFilePaths))
	for _, fileMeta := range w.createdFilePaths {
		protoMeta := &proto.ArrowPayload_FileMetadata{
			FileType:    fileMeta.FileType,
			FilePath:    fileMeta.FilePath,
			RecordCount: fileMeta.RecordCount,
		}
		if fileMeta.EqualityFieldID != nil {
			protoMeta.EqualityFieldId = fileMeta.EqualityFieldID
		}
		fileMetadata = append(fileMetadata, protoMeta)
	}

	registerRequest := &proto.ArrowPayload{
		Type: proto.ArrowPayload_REGISTER,
		Metadata: &proto.ArrowPayload_Metadata{
			ThreadId:      w.server.ServerID(),
			DestTableName: w.stream.GetDestinationTable(),
			FileMetadata:  fileMetadata,
		},
	}

	// Send register request with timeout
	registerCtx, registerCancel := context.WithTimeout(ctx, 3600*time.Second)
	defer registerCancel()

	_, err = w.server.SendArrowRequest(registerCtx, registerRequest)
	if err != nil {
		return fmt.Errorf("failed to register arrow files: %s", err)
	}

	commitRequest := &proto.ArrowPayload{
		Type: proto.ArrowPayload_COMMIT,
		Metadata: &proto.ArrowPayload_Metadata{
			ThreadId:      w.server.ServerID(),
			DestTableName: w.stream.GetDestinationTable(),
		},
	}

	// Send commit request with timeout
	commitCtx, commitCancel := context.WithTimeout(ctx, 3600*time.Second)
	defer commitCancel()

	_, err = w.server.SendArrowRequest(commitCtx, commitRequest)
	if err != nil {
		return fmt.Errorf("failed to commit arrow files: %s", err)
	}

	return nil
}

func (w *ArrowWriter) closeWriters(ctx context.Context) error {
	var err error

	w.writers.Range(func(key, value interface{}) bool {
		mapKey := key.(string)
		writer, _ := value.(*RollingWriter)
		if closeErr := writer.currentWriter.Close(); closeErr != nil {
			err = fmt.Errorf("failed to close writer: %v", closeErr)

			return false
		}

		parts := strings.SplitN(mapKey, ":", 2)
		fileType := parts[0]
		partitionKey := parts[1]

		fileData := make([]byte, writer.currentBuffer.Len())
		copy(fileData, writer.currentBuffer.Bytes())

		uploadData := &FileUploadData{
			FileType:        fileType,
			FileData:        fileData,
			PartitionKey:    partitionKey,
			EqualityFieldID: int32(w.fieldIDs[constants.OlakeID]),
			RecordCount:     writer.currentRowCount,
		}

		if uploadErr := w.uploadFile(ctx, uploadData); uploadErr != nil {
			err = fmt.Errorf("failed to upload parquet: %v", uploadErr)

			return false
		}

		return true
	})

	return err
}

func (w *ArrowWriter) initializeFields(ctx context.Context) error {
	fieldIDs, err := w.getAllfieldIDs(ctx)
	if err != nil {
		return fmt.Errorf("failed to get all field IDs: %w", err)
	}

	w.fieldIDs = fieldIDs

	if w.stream.NormalizationEnabled() {
		w.fields = CreateNormFields(w.schema, w.fieldIDs)
	} else {
		w.fields = CreateDeNormFields(w.fieldIDs)
	}

	w.icebergSchemaJSON = buildIcebergSchemaJSON(w.schema, w.fieldIDs, w.schemaID)

	return nil
}

func (w *ArrowWriter) getOrCreateWriter(partitionKey string, schema arrow.Schema, fileType string) (*RollingWriter, error) {
	// differentiating data and delete file writers, "data:pk" : *writer, "delete:pk" : *writer
	key := fileType + ":" + partitionKey

	if existing, ok := w.writers.Load(key); ok {
		return existing.(*RollingWriter), nil
	}

	writer, err := w.createWriter(schema, fileType)
	if err != nil {
		return nil, fmt.Errorf("failed to create rolling writer: %v", err)
	}

	ww, ok := w.writers.LoadOrStore(key, writer)
	if ok {
		return ww.(*RollingWriter), nil
	}

	return writer, nil
}

func (w *ArrowWriter) createWriter(schema arrow.Schema, fileType string) (*RollingWriter, error) {
	// filename will be generated by Java server during upload
	baseProps := []parquet.WriterProperty{
		DefaultCompression,
		DefaultCompressionLevel,
		DefaultDataPageSize,
		DefaultDictionaryPageSizeLimit,
		DefaultDictionaryEncoding,
		DefaultBatchSize,
		DefaultStatsEnabled,
		DefaultParquetVersion,
		DefaultRootName,
		parquet.WithAllocator(memory.NewGoAllocator()),
	}

	currentBuffer := &bytes.Buffer{}
	writerProps := parquet.NewWriterProperties(baseProps...)

	arrowProps := pqarrow.NewArrowWriterProperties(
		pqarrow.WithStoreSchema(),
		pqarrow.WithNoMapLogicalType(),
	)

	writer, err := pqarrow.NewFileWriter(&schema, currentBuffer, writerProps, arrowProps)
	if err != nil {
		return nil, fmt.Errorf("failed to create new file writer: %v", err)
	}

	if fileType == "delete" {
		icebergSchemaJSON := fmt.Sprintf(`{"type":"struct","schema-id":%d,"fields":[{"id":%d,"name":"%s","required":true,"type":"string"}]}`, w.schemaID, w.fieldIDs[constants.OlakeID], constants.OlakeID)

		if err = writer.AppendKeyValueMetadata("delete-type", "equality"); err != nil {
			return nil, fmt.Errorf("failed to append key value metadata, delete-type equality: %v", err)
		}
		if err = writer.AppendKeyValueMetadata("delete-field-ids", fmt.Sprintf("%d", w.fieldIDs[constants.OlakeID])); err != nil {
			return nil, fmt.Errorf("failed to append key value metadata, delete-field-ids: %v", err)
		}
		if err = writer.AppendKeyValueMetadata("iceberg.schema", icebergSchemaJSON); err != nil {
			return nil, fmt.Errorf("failed to append iceberg schema json: %v", err)
		}
	} else if fileType == "data" {
		if err = writer.AppendKeyValueMetadata("iceberg.schema", w.icebergSchemaJSON); err != nil {
			return nil, fmt.Errorf("failed to append iceberg schema json: %v", err)
		}
	}

	return &RollingWriter{
		currentWriter:         writer,
		currentBuffer:         currentBuffer,
		currentRowCount:       0,
		currentCompressedSize: 0,
	}, nil
}

func buildIcebergSchemaJSON(schema map[string]string, fieldIDs map[string]int32, schemaID int) string {
	fieldNames := make([]string, 0, len(schema))
	for fieldName := range schema {
		fieldNames = append(fieldNames, fieldName)
	}

	sort.Strings(fieldNames)

	fieldsJSON := ""
	for i, fieldName := range fieldNames {
		icebergType := schema[fieldName]
		fieldID := fieldIDs[fieldName]

		typeStr := fmt.Sprintf(`"%s"`, icebergType)

		// OLakeID is a REQUIRED field, cannot be NULL
		required := fieldName == constants.OlakeID

		if i > 0 {
			fieldsJSON += ","
		}
		fieldsJSON += fmt.Sprintf(`{"id":%d,"name":"%s","required":%t,"type":%s}`,
			fieldID, fieldName, required, typeStr)
	}

	return fmt.Sprintf(`{"type":"struct","schema-id":%d,"fields":[%s]}`, schemaID, fieldsJSON)
}

func (w *ArrowWriter) uploadFile(ctx context.Context, uploadData *FileUploadData) error {
	filePath, err := w.uploadParquetFile(ctx, uploadData.FileData, uploadData.FileType, uploadData.PartitionKey, uploadData.EqualityFieldID)
	if err != nil {
		return fmt.Errorf("failed to upload %s file: %w", uploadData.FileType, err)
	}

	fileMeta := FileMetdata{
		FileType:    uploadData.FileType,
		FilePath:    filePath,
		RecordCount: uploadData.RecordCount,
	}
	if uploadData.FileType == "delete" {
		fileMeta.EqualityFieldID = &uploadData.EqualityFieldID
	}
	w.createdFilePaths = append(w.createdFilePaths, fileMeta)

	return nil
}

// equality field id is only required for delete files
func (w *ArrowWriter) uploadParquetFile(ctx context.Context, fileData []byte, fileType, partitionKey string, EqualityFieldID int32) (string, error) {
	request := proto.ArrowPayload{
		Type: proto.ArrowPayload_UPLOAD_FILE,
		Metadata: &proto.ArrowPayload_Metadata{
			DestTableName: w.stream.GetDestinationTable(),
			ThreadId:      w.server.ServerID(),
			FileUpload: &proto.ArrowPayload_FileUploadRequest{
				FileData:        fileData,
				FileType:        fileType,
				PartitionKey:    partitionKey,
				EqualityFieldId: EqualityFieldID,
			},
		},
	}

	// Send upload request with timeout
	uploadCtx, uploadCancel := context.WithTimeout(ctx, 3600*time.Second)
	defer uploadCancel()

	response, err := w.server.SendArrowRequest(uploadCtx, &request)
	if err != nil {
		return "", fmt.Errorf("failed to upload %s file via Iceberg FileIO: %v", fileType, err)
	}

	return response.GetResult(), nil
}

func (w *ArrowWriter) getAllfieldIDs(ctx context.Context) (map[string]int32, error) {
	request := proto.ArrowPayload{
		Type: proto.ArrowPayload_GET_ALL_FIELD_IDS,
		Metadata: &proto.ArrowPayload_Metadata{
			DestTableName: w.stream.GetDestinationTable(),
			ThreadId:      w.server.ServerID(),
		},
	}

	// Send request with timeout
	fieldsCtx, fieldsCancel := context.WithTimeout(ctx, 3600*time.Second)
	defer fieldsCancel()

	response, err := w.server.SendArrowRequest(fieldsCtx, &request)
	if err != nil {
		return nil, fmt.Errorf("failed to get all field IDs: %w", err)
	}

	fieldIDs := make(map[string]int32)
	for fieldName, fieldID := range response.GetFieldIDs() {
		fieldIDs[fieldName] = fieldID
	}

	return fieldIDs, nil
}

func (w *ArrowWriter) getschemaID(ctx context.Context) (int, error) {
	request := proto.ArrowPayload{
		Type: proto.ArrowPayload_GET_SCHEMA_ID,
		Metadata: &proto.ArrowPayload_Metadata{
			DestTableName: w.stream.GetDestinationTable(),
			ThreadId:      w.server.ServerID(),
		},
	}

	// Send request with timeout
	schemaCtx, schemaCancel := context.WithTimeout(ctx, 3600*time.Second)
	defer schemaCancel()

	response, err := w.server.SendArrowRequest(schemaCtx, &request)
	if err != nil {
		return -1, fmt.Errorf("failed to get schema ID: %w", err)
	}

	schemaID, err := strconv.Atoi(response.GetResult())
	if err != nil {
		return -1, fmt.Errorf("failed to parse schema ID from response '%s': %w", response.GetResult(), err)
	}

	return schemaID, nil
}
