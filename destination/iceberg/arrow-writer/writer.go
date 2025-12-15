package arrowwriter

import (
	"bytes"
	"context"
	"fmt"
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
	"github.com/datazip-inc/olake/utils"
)

// need to check on the number of structs being used here

type ArrowWriter struct {
	fileschemajson   map[string]string
	schema           map[string]string
	arrowSchema      map[string]*arrow.Schema
	allocator        memory.Allocator
	stream           types.StreamInterface
	server           internal.ServerClient
	fields           []arrow.Field
	partitionInfo    []internal.PartitionInfo
	createdFilePaths []*proto.ArrowPayload_FileMetadata
	writers          sync.Map
	upsertMode       bool
}

type RollingWriter struct {
	currentWriter   *pqarrow.FileWriter
	currentBuffer   *bytes.Buffer
	currentRowCount int64
	partitionValues []string
}

type FileUploadData struct {
	FileType        string
	FileData        []byte
	PartitionKey    string
	PartitionValues []string
	RecordCount     int64
}

func New(ctx context.Context, partitionInfo []internal.PartitionInfo, schema map[string]string, stream types.StreamInterface, server internal.ServerClient, upsertMode bool) (*ArrowWriter, error) {
	writer := &ArrowWriter{
		partitionInfo: partitionInfo,
		schema:        schema,
		stream:        stream,
		server:        server,
		arrowSchema:   make(map[string]*arrow.Schema),
		upsertMode:    upsertMode,
	}

	if err := writer.initialize(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize fields: %w", err)
	}

	return writer, nil
}

func (w *ArrowWriter) createPartitionKey(record types.RawRecord) (string, []string, error) {
	paths := make([]string, 0, len(w.partitionInfo))
	values := make([]string, 0, len(w.partitionInfo))

	for _, partition := range w.partitionInfo {
		field, transform := partition.Field, partition.Transform
		colType, ok := w.schema[field]
		if !ok {
			return "", nil, fmt.Errorf("partition field %q does not exist in schema", field)
		}

		transformedVal, err := TransformValue(record.Data[field], transform, colType)
		if err != nil {
			return "", nil, err
		}

		valueStr := transformedVal.(string)
		colPath := ConstructColPath(valueStr, field, transform)
		paths = append(paths, colPath)
		values = append(values, valueStr)
	}

	partitionKey := strings.Join(paths, "/")

	return partitionKey, values, nil
}

func (w *ArrowWriter) extract(records []types.RawRecord) (map[string][]types.RawRecord, map[string][]types.RawRecord, error) {
	// data and delete record maps to store (file path : type.RawRecord)
	data := make(map[string][]types.RawRecord)
	deletes := make(map[string][]types.RawRecord)

	for _, rec := range records {
		pKey := ""
		if len(w.partitionInfo) != 0 {
			key, _, err := w.createPartitionKey(rec)
			if err != nil {
				return nil, nil, err
			}
			pKey = key
		}

		// In OLake, for equality deletes, we use OlakeID as the id
		// we create equality delete files for:
		// "d" : delete operation
		// "u" : update operation
		// "c" : insert operation
		if rec.OperationType == "d" || rec.OperationType == "u" || rec.OperationType == "c" {
			del := types.RawRecord{OlakeID: rec.OlakeID}
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

	if w.upsertMode {
		for pKey, rec := range deletes {
			record, err := createDeleteArrowRecord(rec, w.allocator, w.arrowSchema[fileTypeDelete])
			if err != nil {
				return fmt.Errorf("failed to create arrow record: %v", err)
			}

			partitionValues := w.getPartitionValues(rec)
			writer, err := w.getOrCreateWriter(pKey, *record.Schema(), fileTypeDelete, partitionValues)
			if err != nil {
				record.Release()
				return fmt.Errorf("failed to get or create writer for %s: %w", fileTypeDelete, err)
			}
			if err := writer.currentWriter.WriteBuffered(record); err != nil {
				record.Release()
				return fmt.Errorf("failed to write delete record: %v", err)
			}
			writer.currentRowCount += record.NumRows()
			record.Release()

			if shouldFlush, err := w.checkAndFlush(ctx, writer, pKey, fileTypeDelete); err != nil {
				return err
			} else if shouldFlush {
				w.writers.Delete(fileTypeDelete + ":" + pKey)
			}
		}
	}

	for pKey, rec := range data {
		record, err := createArrowRecord(rec, w.allocator, w.arrowSchema[fileTypeData], w.stream.NormalizationEnabled())
		if err != nil {
			return fmt.Errorf("failed to create arrow record: %v", err)
		}

		partitionValues := w.getPartitionValues(rec)
		writer, err := w.getOrCreateWriter(pKey, *record.Schema(), fileTypeData, partitionValues)
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

		if shouldFlush, err := w.checkAndFlush(ctx, writer, pKey, fileTypeData); err != nil {
			return err
		} else if shouldFlush {
			w.writers.Delete(fileTypeData + ":" + pKey)
		}
	}

	return nil
}

func (w *ArrowWriter) getPartitionValues(records []types.RawRecord) []string {
	if len(records) == 0 || len(w.partitionInfo) == 0 {
		return nil
	}
	_, partitionValues, _ := w.createPartitionKey(records[0])

	return partitionValues
}

func (w *ArrowWriter) checkAndFlush(ctx context.Context, writer *RollingWriter, partitionKey string, fileType string) (bool, error) {
	// current size: all previously flushed row groups (in buffer) + current in-progress row group (buffered in memory)
	sizeSoFar := int64(writer.currentBuffer.Len()) + writer.currentWriter.RowGroupTotalBytesWritten()
	targetFileSize := utils.Ternary(fileType == fileTypeDelete, targetDeleteFileSize, targetDataFileSize).(int64)

	if sizeSoFar >= targetFileSize {
		if err := writer.currentWriter.Close(); err != nil {
			return false, fmt.Errorf("failed to close writer during flush: %w", err)
		}

		uploadData := &FileUploadData{
			FileType:        fileType,
			FileData:        writer.currentBuffer.Bytes(),
			PartitionKey:    partitionKey,
			PartitionValues: writer.partitionValues,
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

	commitRequest := &proto.ArrowPayload{
		Type: proto.ArrowPayload_REGISTER,
		Metadata: &proto.ArrowPayload_Metadata{
			ThreadId:      w.server.ServerID(),
			DestTableName: w.stream.GetDestinationTable(),
			FileMetadata:  w.createdFilePaths,
		},
	}

	commitCtx, commitCancel := context.WithTimeout(ctx, 3600*time.Second)
	defer commitCancel()

	_, _, err = w.server.SendClientRequest(commitCtx, commitRequest)
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
			PartitionValues: writer.partitionValues,
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

func (w *ArrowWriter) initialize(ctx context.Context) error {
	if err := w.fetchIcebergSchemas(ctx); err != nil {
		return err
	}

	dataFieldIDs, err := parseFieldIDsFromIcebergSchema(w.fileschemajson[fileTypeData])
	if err != nil {
		return fmt.Errorf("failed to parse data schema field IDs: %w", err)
	}

	w.allocator = memory.NewGoAllocator()
	w.fields = createFields(w.schema, dataFieldIDs)
	w.arrowSchema[fileTypeData] = arrow.NewSchema(w.fields, nil)

	if w.upsertMode {
		deleteFieldIDs, err := parseFieldIDsFromIcebergSchema(w.fileschemajson[fileTypeDelete])
		if err != nil {
			return fmt.Errorf("failed to parse delete schema field IDs: %w", err)
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

func (w *ArrowWriter) getOrCreateWriter(partitionKey string, schema arrow.Schema, fileType string, partitionValues []string) (*RollingWriter, error) {
	// differentiating data and delete file writers, "data:pk" : *writer, "delete:pk" : *writer
	key := fileType + ":" + partitionKey

	if existing, ok := w.writers.Load(key); ok {
		return existing.(*RollingWriter), nil
	}

	writer, err := w.createWriter(schema, fileType, partitionValues)
	if err != nil {
		return nil, fmt.Errorf("failed to create rolling writer: %v", err)
	}

	ww, ok := w.writers.LoadOrStore(key, writer)
	if ok {
		return ww.(*RollingWriter), nil
	}

	return writer, nil
}

func (w *ArrowWriter) createWriter(schema arrow.Schema, fileType string, partitionValues []string) (*RollingWriter, error) {
	baseProps := getDefaultWriterProps()
	baseProps = append(baseProps, parquet.WithAllocator(memory.NewGoAllocator()))

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

	if fileType == fileTypeDelete {
		if err = writer.AppendKeyValueMetadata("delete-type", "equality"); err != nil {
			return nil, fmt.Errorf("failed to append key value metadata, delete-type equality: %v", err)
		}

		// Extract field ID from _olake_id field metadata
		olakeIDField := schema.Field(0)
		fieldIDStr, _ := olakeIDField.Metadata.GetValue("PARQUET:field_id")
		if err = writer.AppendKeyValueMetadata("delete-field-ids", fieldIDStr); err != nil {
			return nil, fmt.Errorf("failed to append key value metadata, delete-field-ids: %v", err)
		}

		if err = writer.AppendKeyValueMetadata("iceberg.schema", w.fileschemajson[fileTypeDelete]); err != nil {
			return nil, fmt.Errorf("failed to append iceberg schema json: %v", err)
		}
	} else if fileType == fileTypeData {
		if err = writer.AppendKeyValueMetadata("iceberg.schema", w.fileschemajson[fileTypeData]); err != nil {
			return nil, fmt.Errorf("failed to append iceberg schema json: %v", err)
		}
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
				FileType:     uploadData.FileType,
				PartitionKey: uploadData.PartitionKey,
			},
		},
	}

	// Send upload request with timeout
	uploadCtx, uploadCancel := context.WithTimeout(ctx, 3600*time.Second)
	defer uploadCancel()

	filePath, _, err := w.server.SendClientRequest(uploadCtx, &request)
	if err != nil {
		return fmt.Errorf("failed to upload %s file via Iceberg FileIO: %v", uploadData.FileType, err)
	}

	fileMeta := &proto.ArrowPayload_FileMetadata{
		FileType:        uploadData.FileType,
		FilePath:        filePath,
		RecordCount:     uploadData.RecordCount,
		PartitionValues: uploadData.PartitionValues,
	}

	w.createdFilePaths = append(w.createdFilePaths, fileMeta)

	return nil
}

func (w *ArrowWriter) fetchIcebergSchemas(ctx context.Context) error {
	request := &proto.ArrowPayload{
		Type: proto.ArrowPayload_JSONSCHEMA,
		Metadata: &proto.ArrowPayload_Metadata{
			DestTableName: w.stream.GetDestinationTable(),
			ThreadId:      w.server.ServerID(),
		},
	}

	schemaCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	_, schemaMap, err := w.server.SendClientRequest(schemaCtx, request)
	if err != nil {
		return fmt.Errorf("failed to fetch schema JSON from server: %w", err)
	}

	w.fileschemajson = schemaMap

	return nil
}
