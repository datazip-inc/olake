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

type ArrowWriter struct {
	fileschemajson   map[string]string
	schema           map[string]string
	arrowSchema      map[string]*arrow.Schema
	allocator        memory.Allocator
	stream           types.StreamInterface
	server           internal.ServerClient
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

type PartitionedRecords struct {
	Records         []types.RawRecord
	PartitionKey    string
	PartitionValues []string
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
		return nil, fmt.Errorf("failed to initialize fields: %s", err)
	}

	return writer, nil
}

func (w *ArrowWriter) createPartitionKey(record types.RawRecord, olakeTimestamp time.Time) (string, []string, error) {
	paths := make([]string, 0, len(w.partitionInfo))
	transformedValues := make([]string, 0, len(w.partitionInfo))

	for _, partition := range w.partitionInfo {
		field, transform := partition.Field, partition.Transform
		colType, ok := w.schema[field]
		if !ok {
			return "", nil, fmt.Errorf("partition field %q does not exist in schema", field)
		}

		// case: now() in partitionr regex
		fieldValue := utils.Ternary(field == constants.OlakeTimestamp, olakeTimestamp, record.Data[field])

		value, err := TransformValue(fieldValue, transform, colType)
		if err != nil {
			return "", nil, err
		}

		colPath := ConstructColPath(value, field, transform)
		paths = append(paths, colPath)
		transformedValues = append(transformedValues, value)
	}

	partitionKey := strings.Join(paths, "/")

	return partitionKey, transformedValues, nil
}

func (w *ArrowWriter) extract(records []types.RawRecord, olakeTimestamp time.Time) (map[string]*PartitionedRecords, map[string]*PartitionedRecords, error) {
	data := make(map[string]*PartitionedRecords)
	deletes := make(map[string]*PartitionedRecords)

	for _, rec := range records {
		pKey := ""
		var pValues []string
		if len(w.partitionInfo) != 0 {
			key, values, err := w.createPartitionKey(rec, olakeTimestamp)
			if err != nil {
				return nil, nil, err
			}
			pKey = key
			pValues = values
		}

		if rec.OperationType == "d" || rec.OperationType == "u" || rec.OperationType == "c" {
			del := types.RawRecord{OlakeID: rec.OlakeID}
			if deletes[pKey] == nil {
				deletes[pKey] = &PartitionedRecords{
					PartitionKey:    pKey,
					PartitionValues: pValues,
				}
			}
			deletes[pKey].Records = append(deletes[pKey].Records, del)
		}

		if data[pKey] == nil {
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

			writer, err := w.getOrCreateWriter(partitioned.PartitionKey, *record.Schema(), fileTypeDelete, partitioned.PartitionValues)
			if err != nil {
				record.Release()
				return fmt.Errorf("failed to get or create writer for %s: %s", fileTypeDelete, err)
			}
			if err := writer.currentWriter.WriteBuffered(record); err != nil {
				record.Release()
				return fmt.Errorf("failed to write delete record: %s", err)
			}
			writer.currentRowCount += record.NumRows()
			record.Release()

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

		writer, err := w.getOrCreateWriter(partitioned.PartitionKey, *record.Schema(), fileTypeData, partitioned.PartitionValues)
		if err != nil {
			record.Release()
			return fmt.Errorf("failed to get or create writer for data: %s", err)
		}
		if err := writer.currentWriter.WriteBuffered(record); err != nil {
			record.Release()
			return fmt.Errorf("failed to write data record: %s", err)
		}
		writer.currentRowCount += record.NumRows()
		record.Release()

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

		w.writers.Delete(fileType + ":" + partitionKey)
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
	var err error

	w.writers.Range(func(key, value interface{}) bool {
		mapKey := key.(string)
		writer, _ := value.(*RollingWriter)
		if closeErr := writer.currentWriter.Close(); closeErr != nil {
			err = fmt.Errorf("failed to close writer: %s", closeErr)
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
			err = fmt.Errorf("failed to upload parquet: %s", uploadErr)
			return false
		}

		return true
	})

	if err == nil {
		w.writers.Range(func(key, _ interface{}) bool {
			w.writers.Delete(key)
			return true
		})
	}

	return err
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

func (w *ArrowWriter) getOrCreateWriter(partitionKey string, schema arrow.Schema, fileType string, partitionValues []string) (*RollingWriter, error) {
	// differentiating data and delete file writers, "data:pk" : *writer, "delete:pk" : *writer
	key := fileType + ":" + partitionKey

	if existing, ok := w.writers.Load(key); ok {
		return existing.(*RollingWriter), nil
	}

	writer, err := w.createWriter(schema, fileType, partitionValues)
	if err != nil {
		return nil, fmt.Errorf("failed to create rolling writer: %s", err)
	}

	ww, ok := w.writers.LoadOrStore(key, writer)
	if ok {
		return ww.(*RollingWriter), nil
	}

	return writer, nil
}

func (w *ArrowWriter) createWriter(schema arrow.Schema, fileType string, partitionValues []string) (*RollingWriter, error) {
	baseProps := getDefaultWriterProps()
	baseProps = append(baseProps, parquet.WithAllocator(w.allocator))

	currentBuffer := &bytes.Buffer{}
	writerProps := parquet.NewWriterProperties(baseProps...)

	arrowProps := pqarrow.NewArrowWriterProperties(
		pqarrow.WithStoreSchema(),
		pqarrow.WithNoMapLogicalType(),
	)

	writer, err := pqarrow.NewFileWriter(&schema, currentBuffer, writerProps, arrowProps)
	if err != nil {
		return nil, fmt.Errorf("failed to create new file writer: %s", err)
	}

	if fileType == fileTypeDelete {
		if err = writer.AppendKeyValueMetadata("delete-type", "equality"); err != nil {
			return nil, fmt.Errorf("failed to append key value metadata, delete-type equality: %s", err)
		}

		// Extract field ID from _olake_id field metadata
		olakeIDField := schema.Field(0)
		fieldIDStr, _ := olakeIDField.Metadata.GetValue("PARQUET:field_id")
		if err = writer.AppendKeyValueMetadata("delete-field-ids", fieldIDStr); err != nil {
			return nil, fmt.Errorf("failed to append key value metadata, delete-field-ids: %s", err)
		}
	}

	if err = writer.AppendKeyValueMetadata("iceberg.schema", w.fileschemajson[fileType]); err != nil {
		return nil, fmt.Errorf("failed to append iceberg schema json: %s", err)
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
	fileMeta := &proto.ArrowPayload_FileMetadata{
		FileType:        uploadData.FileType,
		FilePath:        arrowResponse.GetResult(),
		RecordCount:     uploadData.RecordCount,
		PartitionValues: uploadData.PartitionValues,
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
