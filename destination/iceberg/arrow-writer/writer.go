package arrowwriter

import (
	"bytes"
	"context"
	"fmt"
	"strings"
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
	fileschemajson map[string]string
	schema         map[string]string
	arrowSchema    map[string]*arrow.Schema
	allocator      memory.Allocator
	stream         types.StreamInterface
	server         internal.ServerClient
	partitionInfo  []internal.PartitionInfo
	createdFiles   map[string]*PartitionFiles
	writers        map[string]*RollingWriter
	upsertMode     bool
}

type RollingWriter struct {
	filePath         string
	currentWriter    *pqarrow.FileWriter
	currentBuffer    *bytes.Buffer
	currentRowCount  int64
	partitionValues  []any
	olakeIDPositions map[string]int64 // latest olake_id -> position
}

type PartitionFiles struct {
	PartitionKey    string
	PartitionValues []any
	DataFiles       []*proto.ArrowPayload_FileMetadata
	EqDeleteFiles   []*proto.ArrowPayload_FileMetadata
	PosDeleteFiles  []*proto.ArrowPayload_FileMetadata
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

// tracks similar _olake_id positions
type PositionTracker struct {
	Positions []int64
}

func New(ctx context.Context, partitionInfo []internal.PartitionInfo, schema map[string]string, stream types.StreamInterface, server internal.ServerClient, upsertMode bool) (*ArrowWriter, error) {
	writer := &ArrowWriter{
		partitionInfo: partitionInfo,
		schema:        schema,
		stream:        stream,
		server:        server,
		arrowSchema:   make(map[string]*arrow.Schema),
		writers:       make(map[string]*RollingWriter),
		createdFiles:  make(map[string]*PartitionFiles),
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

type Deletes struct {
	Partitions      *PartitionedRecords
	PostitionOffset map[string]*PositionTracker
}

func (w *ArrowWriter) extract(records []types.RawRecord, olakeTimestamp time.Time) (map[string]*PartitionedRecords, map[string]*Deletes, error) {
	data := make(map[string]*PartitionedRecords)
	deletes := make(map[string]*Deletes)

	for idx, rec := range records {
		pKey := ""
		var pValues []any
		if len(w.partitionInfo) != 0 {
			key, err := w.getRecordPartitionKey(rec, olakeTimestamp)
			if err != nil {
				return nil, nil, err
			}
			pKey = key

			values, err := w.getRecordPartitionValues(rec, olakeTimestamp)
			if err != nil {
				return nil, nil, err
			}
			pValues = values
		}

		if rec.OperationType == "d" || rec.OperationType == "u" || rec.OperationType == "c" {
			if deletes[pKey] == nil {
				deletes[pKey] = &Deletes{
					Partitions: &PartitionedRecords{
						PartitionKey:    pKey,
						PartitionValues: pValues,
					},
					PostitionOffset: make(map[string]*PositionTracker),
				}
			}

			// for positional deletes
			if deletes[pKey].PostitionOffset[rec.OlakeID] == nil {
				deletes[pKey].PostitionOffset[rec.OlakeID] = &PositionTracker{}
			}
			deletes[pKey].PostitionOffset[rec.OlakeID].Positions = append(deletes[pKey].PostitionOffset[rec.OlakeID].Positions, int64(idx))
		}

		if data[pKey] == nil {
			data[pKey] = &PartitionedRecords{
				PartitionKey:    pKey,
				PartitionValues: pValues,
			}
		}
		data[pKey].Records = append(data[pKey].Records, rec)
	}

	// for equality delete files
	for _, del := range deletes {
		for olakeID := range del.PostitionOffset {
			del.Partitions.Records = append(del.Partitions.Records, types.RawRecord{OlakeID: olakeID})
		}
	}

	return data, deletes, nil
}

func (w *ArrowWriter) Write(ctx context.Context, records []types.RawRecord) error {
	olakeTimestamp := time.Now().UTC() // for olake timestamp, set current timestamp

	data, deletes, err := w.extract(records, olakeTimestamp)
	if err != nil {
		return fmt.Errorf("failed to partition data: %s", err)
	}

	for pKey, partitioned := range data {
		if len(partitioned.Records) == 0 {
			continue
		}

		record, err := createArrowRecord(partitioned.Records, w.allocator, w.arrowSchema[fileTypeData], w.stream.NormalizationEnabled(), olakeTimestamp)
		if err != nil {
			return fmt.Errorf("failed to create arrow record: %s", err)
		}

		writer, err := w.getOrCreateWriter(ctx, partitioned.PartitionKey, *record.Schema(), fileTypeData, partitioned.PartitionValues)
		if err != nil {
			return fmt.Errorf("failed to get or create writer for data: %s", err)
		}
		defer record.Release()

		// for writing positional deletes, we need starting row count to calculate position
		startRowCount := writer.currentRowCount

		if err := writer.currentWriter.WriteBuffered(record); err != nil {
			return fmt.Errorf("failed to write data record: %s", err)
		}
		writer.currentRowCount += record.NumRows()

		// create positional deletes for any duplicate _olake_ids
		if w.upsertMode && deletes[pKey] != nil {
			if err := w.writePositionalDeletes(ctx, pKey, deletes[pKey], writer, startRowCount, partitioned); err != nil {
				return err
			}
		}

		if err := w.checkAndFlush(ctx, writer, partitioned.PartitionKey, fileTypeData); err != nil {
			return err
		}
	}

	// Process equality delete files
	if w.upsertMode {
		for _, del := range deletes {
			if len(del.Partitions.Records) == 0 {
				continue
			}

			record, err := createDeleteArrowRecord(del.Partitions.Records, w.allocator, w.arrowSchema[fileTypeEqualityDelete])
			if err != nil {
				return fmt.Errorf("failed to create equality delete record: %s", err)
			}

			writer, err := w.getOrCreateWriter(ctx, del.Partitions.PartitionKey, *record.Schema(), fileTypeEqualityDelete, del.Partitions.PartitionValues)
			if err != nil {
				record.Release()
				return fmt.Errorf("failed to get or create writer for %s: %s", fileTypeEqualityDelete, err)
			}

			if err := writer.currentWriter.WriteBuffered(record); err != nil {
				record.Release()
				return fmt.Errorf("failed to write equality delete record: %s", err)
			}
			writer.currentRowCount += record.NumRows()
			record.Release()

			if err := w.checkAndFlush(ctx, writer, del.Partitions.PartitionKey, fileTypeEqualityDelete); err != nil {
				return err
			}
		}
	}

	return nil
}

// suppose _olake_id appears n times, we create positional delete files for n-1 oldest rows, the latest goes for equality delete files
func (w *ArrowWriter) writePositionalDeletes(ctx context.Context, pKey string, del *Deletes, writer *RollingWriter, startRowCount int64, partitioned *PartitionedRecords) error {
	var posDeletes []PositionalDeleteRecord

	for idx, rec := range partitioned.Records {
		position := startRowCount + int64(idx)
		if prevPos, exists := writer.olakeIDPositions[rec.OlakeID]; exists {
			// any previous occurrence becomes a positional delete, latest goes for equality delete
			posDeletes = append(posDeletes, PositionalDeleteRecord{
				FilePath: writer.filePath,
				Pos:      prevPos,
			})
		}
		// update to the latest occurrence
		writer.olakeIDPositions[rec.OlakeID] = position
	}

	if len(posDeletes) == 0 {
		return nil
	}

	posRecord := createPositionalDeleteArrowRecord(posDeletes, w.allocator, w.arrowSchema[fileTypePositionalDelete])
	posDelWriter, err := w.getOrCreateWriter(ctx, pKey, *posRecord.Schema(), fileTypePositionalDelete, del.Partitions.PartitionValues)
	if err != nil {
		return fmt.Errorf("failed to get or create positional delete writer: %s", err)
	}
	defer posRecord.Release()

	if err := posDelWriter.currentWriter.WriteBuffered(posRecord); err != nil {
		return fmt.Errorf("failed to write positional delete record: %s", err)
	}
	posDelWriter.currentRowCount += posRecord.NumRows()

	if err := w.checkAndFlush(ctx, posDelWriter, pKey, fileTypePositionalDelete); err != nil {
		return err
	}

	return nil
}

// A single positional delete entry
type PositionalDeleteRecord struct {
	FilePath string
	Pos      int64
}

func (w *ArrowWriter) checkAndFlush(ctx context.Context, writer *RollingWriter, partitionKey string, fileType string) error {
	// logic, sizeSoFar := actual File Size + current compressed data (RowGroupTotalBytesWritten())
	// we can find out current row group's compressed size even before completing the entire row group
	sizeSoFar := int64(writer.currentBuffer.Len()) + writer.currentWriter.RowGroupTotalBytesWritten()
	targetFileSize := utils.Ternary(fileType == fileTypeEqualityDelete || fileType == fileTypePositionalDelete, targetDeleteFileSize, targetDataFileSize).(int64)

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

		if err := w.uploadFile(ctx, uploadData, writer.filePath); err != nil {
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
	if err := w.completeWriters(ctx); err != nil {
		return fmt.Errorf("failed to close arrow writers: %s", err)
	}

	// Build ordered file metadata list: for each partition, order is:
	// 1. Equality delete files first
	// 2. Data files second
	// 3. Positional delete files last
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

	commitCtx, commitCancel := context.WithTimeout(ctx, 3600*time.Second)
	defer commitCancel()

	_, err := w.server.SendClientRequest(commitCtx, commitRequest)
	if err != nil {
		return fmt.Errorf("failed to commit arrow files: %s", err)
	}

	return nil
}

func (w *ArrowWriter) completeWriters(ctx context.Context) error {
	for pKey, writer := range w.writers {
		if writer.currentRowCount == 0 {
			continue 
		}

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

		if err := w.uploadFile(ctx, uploadData, writer.filePath); err != nil {
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
		deleteFieldIDs, err := parseFieldIDsFromIcebergSchema(w.fileschemajson[fileTypeEqualityDelete])
		if err != nil {
			return fmt.Errorf("failed to parse delete schema field IDs: %s", err)
		}

		olakeIDFieldID, ok := deleteFieldIDs[constants.OlakeID]
		if !ok {
			return fmt.Errorf("_olake_id field not found in delete schema")
		}

		// Equality delete schema: just the identifier field (_olake_id)
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

		// positional delete schema: file_path (string) and pos (long)
		// iceberg's reserved field IDs: file_path = MAX_VALUE-101, pos = MAX_VALUE-102
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
	}

	return nil
}

func (w *ArrowWriter) getOrCreateWriter(ctx context.Context, partitionKey string, schema arrow.Schema, fileType string, partitionValues []any) (*RollingWriter, error) {
	key := getPartitionKey(fileType, partitionKey)

	if writer, exists := w.writers[key]; exists {
		return writer, nil
	}

	// Allocate filepath from Java side before creating writer
	filePath, err := w.allocateFilePath(ctx, partitionKey)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate file path: %s", err)
	}

	writer, err := w.createWriter(schema, fileType, partitionValues, filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create rolling writer: %s", err)
	}

	w.writers[key] = writer
	return writer, nil
}

func (w *ArrowWriter) createWriter(schema arrow.Schema, fileType string, partitionValues []any, filePath string) (*RollingWriter, error) {
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

	if fileType == fileTypeEqualityDelete {
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

	schemaJSON := w.fileschemajson[fileType]

	if schemaJSON != "" {
		if err = writer.AppendKeyValueMetadata("iceberg.schema", schemaJSON); err != nil {
			return nil, fmt.Errorf("failed to append iceberg schema json: %s", err)
		}
	}

	return &RollingWriter{
		filePath:         filePath,
		currentWriter:    writer,
		currentBuffer:    currentBuffer,
		currentRowCount:  0,
		partitionValues:  partitionValues,
		olakeIDPositions: make(map[string]int64),
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

	reqCtx, cancel := context.WithTimeout(ctx, 3600*time.Second)
	defer cancel()

	resp, err := w.server.SendClientRequest(reqCtx, request)
	if err != nil {
		return "", fmt.Errorf("failed to allocate file path: %s", err)
	}

	arrowResp := resp.(*proto.ArrowIngestResponse)
	basePath := arrowResp.GetResult()

	if partitionKey != "" {
		lastSlashIdx := strings.LastIndex(basePath, "/")
		if lastSlashIdx > 0 {
			// basePath = "s3://bucket/namespace/table/data/filename.parquet"
			// result = "s3://bucket/namespace/table/data/partitionKey/filename.parquet"
			dirPath := basePath[:lastSlashIdx]
			fileName := basePath[lastSlashIdx+1:]
			return dirPath + "/" + partitionKey + "/" + fileName, nil
		}
	}

	return basePath, nil
}

func (w *ArrowWriter) uploadFile(ctx context.Context, uploadData *FileUploadData, filePath string) error {
	request := proto.ArrowPayload{
		Type: proto.ArrowPayload_UPLOAD_FILE,
		Metadata: &proto.ArrowPayload_Metadata{
			DestTableName: w.stream.GetDestinationTable(),
			ThreadId:      w.server.ServerID(),
			FileUpload: &proto.ArrowPayload_FileUploadRequest{
				FileData: uploadData.FileData,
				FilePath: filePath,
			},
		},
	}

	uploadCtx, uploadCancel := context.WithTimeout(ctx, 3600*time.Second)
	defer uploadCancel()

	_, err := w.server.SendClientRequest(uploadCtx, &request)
	if err != nil {
		return fmt.Errorf("failed to upload %s file via Iceberg FileIO: %s", uploadData.FileType, err)
	}

	protoPartitionValues, err := toProtoPartitionValues(uploadData.PartitionValues)
	if err != nil {
		return fmt.Errorf("failed to convert partition values: %s", err)
	}

	fileMeta := &proto.ArrowPayload_FileMetadata{
		FileType:        uploadData.FileType,
		FilePath:        filePath,
		RecordCount:     uploadData.RecordCount,
		PartitionValues: protoPartitionValues,
	}

	// Group files by partition key for proper ordering during commit
	if w.createdFiles[uploadData.PartitionKey] == nil {
		w.createdFiles[uploadData.PartitionKey] = &PartitionFiles{
			PartitionKey:    uploadData.PartitionKey,
			PartitionValues: uploadData.PartitionValues,
		}
	}

	pf := w.createdFiles[uploadData.PartitionKey]
	switch uploadData.FileType {
	case fileTypeData:
		pf.DataFiles = append(pf.DataFiles, fileMeta)
	case fileTypeEqualityDelete:
		pf.EqDeleteFiles = append(pf.EqDeleteFiles, fileMeta)
	case fileTypePositionalDelete:
		pf.PosDeleteFiles = append(pf.PosDeleteFiles, fileMeta)
	}

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
