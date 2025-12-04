package arrowwriter

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

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
	fieldIds          map[string]int
	partitionInfo     []internal.PartitionInfo
	schemaId          int
	schema            map[string]string
	stream            types.StreamInterface
	server            internal.ArrowServerClient
	createdFilePaths  []FileMetdata
	writers           sync.Map
}

type RollingWriter struct {
	currentWriter   *pqarrow.FileWriter
	currentBuffer   *bytes.Buffer
	currentRowCount int64
}

type FileMetdata struct {
	FileType        string
	FilePath        string
	RecordCount     int64
	EqualityFieldId *int32 // Only set for delete files
}

type FileUploadData struct {
	FileType        string
	FileData        []byte
	PartitionKey    string
	EqualityFieldId int
	RecordCount     int64
}

func New(ctx context.Context, partitionInfo []internal.PartitionInfo, schema map[string]string, stream types.StreamInterface, server internal.ArrowServerClient) (*ArrowWriter, error) {
	writer := &ArrowWriter{
		partitionInfo: partitionInfo,
		schema:        schema,
		stream:        stream,
		server:        server,
	}

	schemaId, err := writer.getSchemaId(ctx)
	if err != nil {
		return nil, err
	}

	writer.schemaId = schemaId
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
		record, err := createDeleteArrowRec(record, w.fieldIds[constants.OlakeID])
		if err != nil {
			return fmt.Errorf("failed to create arrow record: %v", err)
		}

		writer, err := w.getOrCreateWriter(ctx, pKey, *record.Schema(), "delete")
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
	}

	for pKey, record := range data {
		record, err := CreateArrowRecord(record, w.fields, w.stream.NormalizationEnabled())
		if err != nil {
			return fmt.Errorf("failed to create arrow record: %v", err)
		}

		writer, err := w.getOrCreateWriter(ctx, pKey, *record.Schema(), "data")
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
	}

	return nil
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
		if fileMeta.EqualityFieldId != nil {
			protoMeta.EqualityFieldId = fileMeta.EqualityFieldId
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

	_, err = w.server.SendArrowRequest(ctx, registerRequest)
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

	_, err = w.server.SendArrowRequest(ctx, commitRequest)
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
			EqualityFieldId: w.fieldIds[constants.OlakeID],
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
	fieldIds, err := w.getAllFieldIds(ctx)
	if err != nil {
		return fmt.Errorf("failed to get all field IDs: %w", err)
	}

	w.fieldIds = fieldIds

	if w.stream.NormalizationEnabled() {
		w.fields = CreateNormFields(w.schema, w.fieldIds)
	} else {
		w.fields = CreateDeNormFields(w.fieldIds)
	}

	w.icebergSchemaJSON = buildIcebergSchemaJSON(w.schema, w.fieldIds, w.schemaId)

	return nil
}

func (w *ArrowWriter) getOrCreateWriter(ctx context.Context, partitionKey string, schema arrow.Schema, fileType string) (*RollingWriter, error) {
	// differentiating data and delete file writers, "data:pk" : *writer, "delete:pk" : *writer
	key := fileType + ":" + partitionKey

	if existing, ok := w.writers.Load(key); ok {
		return existing.(*RollingWriter), nil
	}

	writer, err := w.createWriter(ctx, schema, fileType)
	if err != nil {
		return nil, fmt.Errorf("failed to create rolling writer: %v", err)
	}

	ww, ok := w.writers.LoadOrStore(key, writer)
	if ok {
		return ww.(*RollingWriter), nil
	}

	return writer, nil
}

func (w *ArrowWriter) createWriter(ctx context.Context, schema arrow.Schema, fileType string) (*RollingWriter, error) {
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
		icebergSchemaJSON := fmt.Sprintf(`{"type":"struct","schema-id":%d,"fields":[{"id":%d,"name":"%s","required":true,"type":"string"}]}`, w.schemaId, w.fieldIds[constants.OlakeID], constants.OlakeID)

		writer.AppendKeyValueMetadata("delete-type", "equality")
		writer.AppendKeyValueMetadata("delete-field-ids", fmt.Sprintf("%d", w.fieldIds[constants.OlakeID]))
		writer.AppendKeyValueMetadata("iceberg.schema", icebergSchemaJSON)
	} else if fileType == "data" {
		writer.AppendKeyValueMetadata("iceberg.schema", w.icebergSchemaJSON)
	}

	return &RollingWriter{
		currentWriter: writer,
		currentBuffer: currentBuffer,
	}, nil
}

func buildIcebergSchemaJSON(schema map[string]string, fieldIds map[string]int, schemaId int) string {
	fieldNames := make([]string, 0, len(schema))
	for fieldName := range schema {
		fieldNames = append(fieldNames, fieldName)
	}

	sort.Strings(fieldNames)

	fieldsJSON := ""
	for i, fieldName := range fieldNames {
		icebergType := schema[fieldName]
		fieldId := fieldIds[fieldName]

		typeStr := fmt.Sprintf(`"%s"`, icebergType)

		// OLakeID is a REQUIRED field, cannot be NULL
		required := fieldName == constants.OlakeID

		if i > 0 {
			fieldsJSON += ","
		}
		fieldsJSON += fmt.Sprintf(`{"id":%d,"name":"%s","required":%t,"type":%s}`,
			fieldId, fieldName, required, typeStr)
	}

	return fmt.Sprintf(`{"type":"struct","schema-id":%d,"fields":[%s]}`, schemaId, fieldsJSON)
}

func (w *ArrowWriter) uploadFile(ctx context.Context, uploadData *FileUploadData) error {
	filePath, err := w.uploadParquetFile(ctx, uploadData.FileData, uploadData.FileType, uploadData.PartitionKey, uploadData.EqualityFieldId)
	if err != nil {
		return fmt.Errorf("failed to upload %s file: %w", uploadData.FileType, err)
	}

	fileMeta := FileMetdata{
		FileType:    uploadData.FileType,
		FilePath:    filePath,
		RecordCount: uploadData.RecordCount,
	}
	if uploadData.FileType == "delete" {
		equalityFieldId := int32(uploadData.EqualityFieldId)
		fileMeta.EqualityFieldId = &equalityFieldId
	}
	w.createdFilePaths = append(w.createdFilePaths, fileMeta)

	return nil
}

// equality field id is only required for delete files
func (w *ArrowWriter) uploadParquetFile(ctx context.Context, fileData []byte, fileType, partitionKey string, equalityFieldId int) (string, error) {
	request := proto.ArrowPayload{
		Type: proto.ArrowPayload_UPLOAD_FILE,
		Metadata: &proto.ArrowPayload_Metadata{
			DestTableName: w.stream.GetDestinationTable(),
			ThreadId:      w.server.ServerID(),
			FileUpload: &proto.ArrowPayload_FileUploadRequest{
				FileData:        fileData,
				FileType:        fileType,
				PartitionKey:    partitionKey,
				EqualityFieldId: int32(equalityFieldId),
			},
		},
	}

	response, err := w.server.SendArrowRequest(ctx, &request)
	if err != nil {
		return "", fmt.Errorf("failed to upload %s file via Iceberg FileIO: %v", fileType, err)
	}

	return response.GetResult(), nil
}

func (w *ArrowWriter) getAllFieldIds(ctx context.Context) (map[string]int, error) {
	request := proto.ArrowPayload{
		Type: proto.ArrowPayload_GET_ALL_FIELD_IDS,
		Metadata: &proto.ArrowPayload_Metadata{
			DestTableName: w.stream.GetDestinationTable(),
			ThreadId:      w.server.ServerID(),
		},
	}

	response, err := w.server.SendArrowRequest(ctx, &request)
	if err != nil {
		return nil, fmt.Errorf("failed to get all field IDs: %w", err)
	}

	fieldIds := make(map[string]int)
	for fieldName, fieldId := range response.GetFieldIds() {
		fieldIds[fieldName] = int(fieldId)
	}

	return fieldIds, nil
}

func (w *ArrowWriter) getSchemaId(ctx context.Context) (int, error) {
	request := proto.ArrowPayload{
		Type: proto.ArrowPayload_GET_SCHEMA_ID,
		Metadata: &proto.ArrowPayload_Metadata{
			DestTableName: w.stream.GetDestinationTable(),
			ThreadId:      w.server.ServerID(),
		},
	}

	response, err := w.server.SendArrowRequest(ctx, &request)
	if err != nil {
		return -1, fmt.Errorf("failed to get schema ID: %w", err)
	}

	schemaId, err := strconv.Atoi(response.GetResult())
	if err != nil {
		return -1, fmt.Errorf("failed to parse schema ID from response '%s': %w", response.GetResult(), err)
	}

	return schemaId, nil
}
