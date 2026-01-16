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
)

type ArrowWriter struct {
	fileschemajson   map[string]string
	schema           map[string]string
	arrowSchema      map[string]*arrow.Schema
	allocator        memory.Allocator
	stream           types.StreamInterface
	server           internal.ServerClient
	partitionInfo    []internal.PartitionInfo
	createdFilePaths []*proto.ArrowPayload_FileMetadata
	writers          map[string]*RollingWriter
	upsertMode       bool
}

type RollingWriter struct {
	currentWriter   *parquetWriter
	currentBuffer   *bytes.Buffer
	currentRowCount int64
	partitionValues []any
}

type PartitionedRecords struct {
	Records         []types.RawRecord
	PartitionKey    string
	PartitionValues []any
}

type FileUploadData struct {
	FileType        string
	FileData        []byte
	PartitionKey    string
	PartitionValues []any
	RecordCount     int64
}

func New(ctx context.Context, partitionInfo []internal.PartitionInfo, schema map[string]string, stream types.StreamInterface, server internal.ServerClient, upsertMode bool) (*ArrowWriter, error) {
	writer := &ArrowWriter{
		partitionInfo: partitionInfo,
		schema:        schema,
		stream:        stream,
		server:        server,
		arrowSchema:   make(map[string]*arrow.Schema),
		writers:       make(map[string]*RollingWriter),
		upsertMode:    upsertMode,
	}

	if err := writer.initialize(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize fields: %s", err)
	}

	return writer, nil
}

func (w *ArrowWriter) getRecordPartitionKey(record types.RawRecord, olakeTimestamp time.Time) (string, error) {
	paths := make([]string, 0, len(w.partitionInfo))

	for _, partition := range w.partitionInfo {
		field, transform := partition.Field, partition.Transform
		colType, ok := w.schema[field]
		if !ok {
			return "", fmt.Errorf("partition field %q does not exist in schema", field)
		}

		fieldValue := utils.Ternary(field == constants.OlakeTimestamp, olakeTimestamp, record.Data[field])

		pathStr, _, err := TransformValue(fieldValue, transform, colType)
		if err != nil {
			return "", err
		}

		colPath := ConstructColPath(pathStr, field, transform)
		paths = append(paths, colPath)
	}

	return strings.Join(paths, "/"), nil
}

func (w *ArrowWriter) getRecordPartitionValues(record types.RawRecord, olakeTimestamp time.Time) ([]any, error) {
	typedValues := make([]any, 0, len(w.partitionInfo))

	for _, partition := range w.partitionInfo {
		field, transform := partition.Field, partition.Transform
		colType, ok := w.schema[field]
		if !ok {
			return nil, fmt.Errorf("partition field %q does not exist in schema", field)
		}

		fieldValue := utils.Ternary(field == constants.OlakeTimestamp, olakeTimestamp, record.Data[field])

		_, typedVal, err := TransformValue(fieldValue, transform, colType)
		if err != nil {
			return nil, err
		}

		typedValues = append(typedValues, typedVal)
	}

	return typedValues, nil
}

func (w *ArrowWriter) extract(records []types.RawRecord, olakeTimestamp time.Time) (map[string]*PartitionedRecords, map[string]*PartitionedRecords, error) {
	data := make(map[string]*PartitionedRecords)
	deletes := make(map[string]*PartitionedRecords)

	for _, rec := range records {
		pKey := ""
		if len(w.partitionInfo) != 0 {
			key, err := w.getRecordPartitionKey(rec, olakeTimestamp)
			if err != nil {
				return nil, nil, err
			}
			pKey = key
		}

		if rec.OperationType == "d" || rec.OperationType == "u" || rec.OperationType == "c" {
			del := types.RawRecord{OlakeID: rec.OlakeID}
			if deletes[pKey] == nil {
				var pValues []any
				if len(w.partitionInfo) != 0 {
					values, err := w.getRecordPartitionValues(rec, olakeTimestamp)
					if err != nil {
						return nil, nil, err
					}
					pValues = values
				}
				deletes[pKey] = &PartitionedRecords{
					PartitionKey:    pKey,
					PartitionValues: pValues,
				}
			}
			deletes[pKey].Records = append(deletes[pKey].Records, del)
		}

		if data[pKey] == nil {
			var pValues []any
			if len(w.partitionInfo) != 0 {
				values, err := w.getRecordPartitionValues(rec, olakeTimestamp)
				if err != nil {
					return nil, nil, err
				}
				pValues = values
			}
			data[pKey] = &PartitionedRecords{
				PartitionKey:    pKey,
				PartitionValues: pValues,
			}
		}
		data[pKey].Records = append(data[pKey].Records, rec)
	}

	return data, deletes, nil
}

func (w *ArrowWriter) Write(ctx context.Context, records []types.RawRecord) error {
	olakeTimestamp := time.Now().UTC() // for olake timestamp, set current timestamp

	data, deletes, err := w.extract(records, olakeTimestamp)
	if err != nil {
		return fmt.Errorf("failed to partition data: %s", err)
	}

	if w.upsertMode {
		for _, partitioned := range deletes {
			record, err := createDeleteArrowRecord(partitioned.Records, w.allocator, w.arrowSchema[fileTypeDelete])
			if err != nil {
				return fmt.Errorf("failed to create arrow record: %s", err)
			}
			defer record.Release()

			writer, err := w.getOrCreateWriter(partitioned.PartitionKey, *record.Schema(), fileTypeDelete, partitioned.PartitionValues)
			if err != nil {
				return fmt.Errorf("failed to get or create writer for %s: %s", fileTypeDelete, err)
			}
			if err := writer.currentWriter.WriteBuffered(record); err != nil {
				return fmt.Errorf("failed to write delete record: %s", err)
			}
			writer.currentRowCount += record.NumRows()

			if err := w.checkAndFlush(ctx, writer, partitioned.PartitionKey, fileTypeDelete); err != nil {
				return err
			}
		}
	}

	for _, partitioned := range data {
		record, err := createArrowRecord(partitioned.Records, w.allocator, w.arrowSchema[fileTypeData], w.stream.NormalizationEnabled(), olakeTimestamp)
		if err != nil {
			return fmt.Errorf("failed to create arrow record: %s", err)
		}
		defer record.Release()

		writer, err := w.getOrCreateWriter(partitioned.PartitionKey, *record.Schema(), fileTypeData, partitioned.PartitionValues)
		if err != nil {
			return fmt.Errorf("failed to get or create writer for data: %s", err)
		}
		if err := writer.currentWriter.WriteBuffered(record); err != nil {
			return fmt.Errorf("failed to write data record: %s", err)
		}
		writer.currentRowCount += record.NumRows()

		if err := w.checkAndFlush(ctx, writer, partitioned.PartitionKey, fileTypeData); err != nil {
			return err
		}
	}

	return nil
}

func (w *ArrowWriter) checkAndFlush(ctx context.Context, writer *RollingWriter, partitionKey string, fileType string) error {
	// logic, sizeSoFar := actual File Size + current compressed data (RowGroupTotalBytesWritten())
	// we can find out current row group's compressed size even before completing the entire row group
	sizeSoFar := int64(writer.currentBuffer.Len()) + writer.currentWriter.RowGroupTotalBytesWritten()
	targetFileSize := utils.Ternary(fileType == fileTypeDelete, targetDeleteFileSize, targetDataFileSize).(int64)

	if sizeSoFar >= targetFileSize {
		if err := writer.currentWriter.Close(); err != nil {
			return fmt.Errorf("failed to close writer during flush: %s", err)
		}

		uploadData := &FileUploadData{
			FileType:        fileType,
			FileData:        writer.currentBuffer.Bytes(),
			PartitionKey:    partitionKey,
			PartitionValues: writer.partitionValues,
			RecordCount:     writer.currentRowCount,
		}

		if err := w.uploadFile(ctx, uploadData); err != nil {
			return fmt.Errorf("failed to upload parquet during flush: %s", err)
		}

		key := getPartitionKey(fileType, partitionKey)
		delete(w.writers, key)
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

func (w *ArrowWriter) Close(ctx context.Context) error {
	err := w.completeWriters(ctx)
	if err != nil {
		return fmt.Errorf("failed to close arrow writers: %s", err)
	}

	commitRequest := &proto.ArrowPayload{
		Type: proto.ArrowPayload_REGISTER_AND_COMMIT,
		Metadata: &proto.ArrowPayload_Metadata{
			ThreadId:      w.server.ServerID(),
			DestTableName: w.stream.GetDestinationTable(),
			FileMetadata:  w.createdFilePaths,
		},
	}

	commitCtx, commitCancel := context.WithTimeout(ctx, 3600*time.Second)
	defer commitCancel()

	_, err = w.server.SendClientRequest(commitCtx, commitRequest)
	if err != nil {
		return fmt.Errorf("failed to commit arrow files: %s", err)
	}

	return nil
}

func (w *ArrowWriter) completeWriters(ctx context.Context) error {
	for pKey, writer := range w.writers {
		if err := writer.currentWriter.Close(); err != nil {
			return fmt.Errorf("failed to close writer: %s", err)
		}

		parts := strings.SplitN(pKey, ":", 2)
		fileType := parts[0]
		partitionKey := parts[1]

		fileData := make([]byte, writer.currentBuffer.Len())
		copy(fileData, writer.currentBuffer.Bytes())

		uploadData := &FileUploadData{
			FileType:        fileType,
			FileData:        fileData,
			PartitionKey:    partitionKey,
			PartitionValues: writer.partitionValues,
			RecordCount:     writer.currentRowCount,
		}

		if err := w.uploadFile(ctx, uploadData); err != nil {
			return fmt.Errorf("failed to upload parquet: %s", err)
		}
	}

	w.writers = make(map[string]*RollingWriter)
	return nil
}

func (w *ArrowWriter) initialize(ctx context.Context) error {
	if err := w.fetchAndUpdateIcebergSchema(ctx); err != nil {
		return err
	}

	dataFieldIDs, err := parseFieldIDsFromIcebergSchema(w.fileschemajson[fileTypeData])
	if err != nil {
		return fmt.Errorf("failed to parse data schema field IDs: %s", err)
	}

	w.allocator = memory.NewGoAllocator()
	fields := createFields(w.schema, dataFieldIDs)
	w.arrowSchema[fileTypeData] = arrow.NewSchema(fields, nil)

	if w.upsertMode {
		deleteFieldIDs, err := parseFieldIDsFromIcebergSchema(w.fileschemajson[fileTypeDelete])
		if err != nil {
			return fmt.Errorf("failed to parse delete schema field IDs: %s", err)
		}

		olakeIDFieldID, ok := deleteFieldIDs[constants.OlakeID]
		if !ok {
			return fmt.Errorf("_olake_id field not found in delete schema")
		}

		w.arrowSchema[fileTypeDelete] = arrow.NewSchema([]arrow.Field{
			{
				Name:     constants.OlakeID,
				Type:     arrow.BinaryTypes.String,
				Nullable: false,
				// Add PARQUET:field_id metadata for Iceberg Query Engines compatibility
				Metadata: arrow.MetadataFrom(map[string]string{
					"PARQUET:field_id": fmt.Sprintf("%d", olakeIDFieldID),
				}),
			},
		}, nil)
	}

	return nil
}

func (w *ArrowWriter) getOrCreateWriter(partitionKey string, schema arrow.Schema, fileType string, partitionValues []any) (*RollingWriter, error) {
	key := getPartitionKey(fileType, partitionKey)

	if writer, exists := w.writers[key]; exists {
		return writer, nil
	}

	writer, err := w.createWriter(schema, fileType, partitionValues)
	if err != nil {
		return nil, fmt.Errorf("failed to create rolling writer: %s", err)
	}

	w.writers[key] = writer
	return writer, nil
}

func (w *ArrowWriter) createWriter(arrowSchema arrow.Schema, fileType string, partitionValues []any) (*RollingWriter, error) {
	baseProps := getDefaultWriterProps()
	currentBuffer := &bytes.Buffer{}

	// build the key-value metadata
	kvMeta := make(metadata.KeyValueMetadata, 0)
	kvMeta.Append("iceberg.schema", w.fileschemajson[fileType])

	if fileType == fileTypeDelete {
		kvMeta.Append("delete-type", "equality")
		// Extract field ID from _olake_id field metadata
		olakeIDField := arrowSchema.Field(0)
		fieldIDStr, _ := olakeIDField.Metadata.GetValue("PARQUET:field_id")
		kvMeta.Append("delete-field-ids", fieldIDStr)
	}

	writer, err := newParquetWriter(&arrowSchema, currentBuffer, baseProps, kvMeta)
	if err != nil {
		return nil, fmt.Errorf("failed to create iceberg parquet writer: %s", err)
	}

	return &RollingWriter{
		currentWriter:   writer,
		currentBuffer:   currentBuffer,
		currentRowCount: 0,
		partitionValues: partitionValues,
	}, nil
}

func (w *ArrowWriter) uploadFile(ctx context.Context, uploadData *FileUploadData) error {
	request := proto.ArrowPayload{
		Type: proto.ArrowPayload_UPLOAD_FILE,
		Metadata: &proto.ArrowPayload_Metadata{
			DestTableName: w.stream.GetDestinationTable(),
			ThreadId:      w.server.ServerID(),
			FileUpload: &proto.ArrowPayload_FileUploadRequest{
				FileData:     uploadData.FileData,
				PartitionKey: uploadData.PartitionKey,
			},
		},
	}

	// Send upload request with timeout
	uploadCtx, uploadCancel := context.WithTimeout(ctx, 3600*time.Second)
	defer uploadCancel()

	resp, err := w.server.SendClientRequest(uploadCtx, &request)
	if err != nil {
		return fmt.Errorf("failed to upload %s file via Iceberg FileIO: %s", uploadData.FileType, err)
	}

	arrowResponse := resp.(*proto.ArrowIngestResponse)
	protoPartitionValues, err := toProtoPartitionValues(uploadData.PartitionValues)
	if err != nil {
		return fmt.Errorf("failed to convert partition values: %s", err)
	}

	fileMeta := &proto.ArrowPayload_FileMetadata{
		FileType:        uploadData.FileType,
		FilePath:        arrowResponse.GetResult(),
		RecordCount:     uploadData.RecordCount,
		PartitionValues: protoPartitionValues,
	}

	w.createdFilePaths = append(w.createdFilePaths, fileMeta)

	return nil
}

func (w *ArrowWriter) fetchAndUpdateIcebergSchema(ctx context.Context) error {
	request := &proto.ArrowPayload{
		Type: proto.ArrowPayload_JSONSCHEMA,
		Metadata: &proto.ArrowPayload_Metadata{
			DestTableName: w.stream.GetDestinationTable(),
			ThreadId:      w.server.ServerID(),
		},
	}

	schemaCtx, cancel := context.WithTimeout(ctx, 3600*time.Second)
	defer cancel()

	resp, err := w.server.SendClientRequest(schemaCtx, request)
	if err != nil {
		return fmt.Errorf("failed to fetch schema JSON from server: %s", err)
	}

	arrowResp := resp.(*proto.ArrowIngestResponse)
	w.fileschemajson = arrowResp.GetIcebergSchemas()

	return nil
}
