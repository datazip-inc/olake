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
	writers        map[string]*RollingWriter
	createdFiles   map[string]*PartitionFiles
	upsertMode     bool
}

type Partition struct {
	Key    string // partition key
	Values []any  // individual partition values
}

type RollingWriter struct {
	filePath        string
	currentWriter   *parquetWriter
	currentBuffer   *bytes.Buffer
	currentRowCount int64
	partitionValues Partition
	olakeIDPosition map[string]int64 // latest olake_id -> position
}

type PartitionFiles struct {
	DataFiles      []*proto.ArrowPayload_FileMetadata
	EqDeleteFiles  []*proto.ArrowPayload_FileMetadata
	PosDeleteFiles []*proto.ArrowPayload_FileMetadata
}

// PartitionedRecords groups records by their partition.
type PartitionedRecords struct {
	Records   []types.RawRecord
	Partition Partition
}

type FileUploadData struct {
	FileType    string
	FileData    []byte
	Partition   Partition
	RecordCount int64
}

// DeleteTracker tracks delete records and duplicate positions for a partition.
type DeleteTracker struct {
	Partition     Partition
	Records       []types.RawRecord  // unique records by _olake_id for equality deletes
	DupePositions map[string][]int64 // _olake_id -> positions of duplicates
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
		writers:       make(map[string]*RollingWriter),
		createdFiles:  make(map[string]*PartitionFiles),
		upsertMode:    upsertMode,
	}

	if err := writer.initialize(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize: %s", err)
	}

	return writer, nil
}

// computes both partition key and typed values
func (w *ArrowWriter) getRecordPartition(record types.RawRecord, olakeTimestamp time.Time) (Partition, error) {
	if len(w.partitionInfo) == 0 {
		return Partition{}, nil
	}

	paths := make([]string, 0, len(w.partitionInfo))
	values := make([]any, 0, len(w.partitionInfo))

	for _, pInfo := range w.partitionInfo {
		colType, ok := w.schema[pInfo.Field]
		if !ok {
			return Partition{}, fmt.Errorf("partition field %q not in schema", pInfo.Field)
		}

		fieldValue := utils.Ternary(pInfo.Field == constants.OlakeTimestamp, olakeTimestamp, record.Data[pInfo.Field])
		if colType == "timestamptz" {
			if ts, err := typeutils.ReformatDate(fieldValue, true); err == nil {
				fieldValue = ts
			}
		}

		pathStr, typedVal, err := TransformValue(fieldValue, pInfo.Transform, colType)
		if err != nil {
			return Partition{}, err
		}

		paths = append(paths, ConstructColPath(pathStr, pInfo.Field, pInfo.Transform))
		values = append(values, typedVal)
	}

	return Partition{Key: strings.Join(paths, "/"), Values: values}, nil
}

// extract partitions records and tracks deletes for upsert mode.
func (w *ArrowWriter) extract(records []types.RawRecord, olakeTimestamp time.Time) (map[string]*PartitionedRecords, map[string]*DeleteTracker, error) {
	data := make(map[string]*PartitionedRecords)
	deletes := make(map[string]*DeleteTracker)

	for idx, rec := range records {
		partition, err := w.getRecordPartition(rec, olakeTimestamp)
		if err != nil {
			return nil, nil, err
		}
		pKey := partition.Key

		// Track deletes for upsert operations (d, u, c all need delete handling)
		if rec.OperationType == "d" || rec.OperationType == "u" || rec.OperationType == "c" {
			if deletes[pKey] == nil {
				deletes[pKey] = &DeleteTracker{
					Partition:     partition,
					DupePositions: make(map[string][]int64),
				}
			}

			dt := deletes[pKey]
			if dt.DupePositions[rec.OlakeID] == nil {
				// First occurrence - add to equality delete records
				dt.Records = append(dt.Records, types.RawRecord{OlakeID: rec.OlakeID})
			}
			dt.DupePositions[rec.OlakeID] = append(dt.DupePositions[rec.OlakeID], int64(idx))
		}

		if data[pKey] == nil {
			data[pKey] = &PartitionedRecords{Partition: partition}
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

	for pKey, partitioned := range data {
		if len(partitioned.Records) == 0 {
			continue
		}

		record, err := createArrowRecord(partitioned.Records, w.allocator, w.arrowSchema[fileTypeData], w.stream.NormalizationEnabled(), olakeTimestamp)
		if err != nil {
			return fmt.Errorf("failed to create arrow record: %s", err)
		}
		defer record.Release()

		rw, err := w.getOrCreateWriter(ctx, partitioned.Partition, *record.Schema(), fileTypeData)
		if err != nil {
			return fmt.Errorf("failed to get or create writer for data: %s", err)
		}

		startRowCount := rw.currentRowCount

		if err := rw.currentWriter.WriteBuffered(record); err != nil {
			return fmt.Errorf("failed to write data record: %s", err)
		}
		rw.currentRowCount += record.NumRows()

		// create positional deletes for duplicate _olake_ids
		if w.upsertMode && deletes[pKey] != nil {
			if err := w.writePositionalDeletes(ctx, deletes[pKey], rw, startRowCount, partitioned); err != nil {
				return err
			}
		}

		if err := w.checkAndFlush(ctx, rw, pKey, fileTypeData); err != nil {
			return err
		}
	}

	// Write equality delete files
	if w.upsertMode {
		for pKey, dt := range deletes {
			if len(dt.Records) == 0 {
				continue
			}

			record, err := createDeleteArrowRecord(dt.Records, w.allocator, w.arrowSchema[fileTypeEqualityDelete])
			if err != nil {
				return fmt.Errorf("failed to create equality delete record: %s", err)
			}

			rw, err := w.getOrCreateWriter(ctx, dt.Partition, *record.Schema(), fileTypeEqualityDelete)
			if err != nil {
				record.Release()
				return fmt.Errorf("failed to get or create writer for %s: %s", fileTypeEqualityDelete, err)
			}

			if err := rw.currentWriter.WriteBuffered(record); err != nil {
				record.Release()
				return fmt.Errorf("failed to write equality delete record: %s", err)
			}
			rw.currentRowCount += record.NumRows()
			record.Release()

			if err := w.checkAndFlush(ctx, rw, pKey, fileTypeEqualityDelete); err != nil {
				return err
			}
		}
	}

	return nil
}

// suppose _olake_id appears n times, we create positional delete files for n-1 oldest rows, the latest goes for equality delete files
func (w *ArrowWriter) writePositionalDeletes(ctx context.Context, dt *DeleteTracker, dataWriter *RollingWriter, startRowCount int64, partitioned *PartitionedRecords) error {
	var posDeletes []PositionalDelete

	for idx, rec := range partitioned.Records {
		position := startRowCount + int64(idx)
		if prevPos, exists := dataWriter.olakeIDPosition[rec.OlakeID]; exists {
			// any previous occurrence becomes a positional delete, latest goes for equality delete
			posDeletes = append(posDeletes, PositionalDelete{
				FilePath: dataWriter.filePath,
				Position: prevPos,
			})
		}
		dataWriter.olakeIDPosition[rec.OlakeID] = position
	}

	if len(posDeletes) == 0 {
		return nil
	}

	posRecord := createPositionalDeleteArrowRecord(posDeletes, w.allocator, w.arrowSchema[fileTypePositionalDelete])
	defer posRecord.Release()

	rw, err := w.getOrCreateWriter(ctx, dt.Partition, *posRecord.Schema(), fileTypePositionalDelete)
	if err != nil {
		return fmt.Errorf("failed to get or create positional delete writer: %s", err)
	}

	if err := rw.currentWriter.WriteBuffered(posRecord); err != nil {
		return fmt.Errorf("failed to write positional delete record: %s", err)
	}
	rw.currentRowCount += posRecord.NumRows()

	return w.checkAndFlush(ctx, rw, dt.Partition.Key, fileTypePositionalDelete)
}

// checkAndFlush checks if file size threshold is reached and flushes if needed.
func (w *ArrowWriter) checkAndFlush(ctx context.Context, rw *RollingWriter, partitionKey string, fileType string) error {
	// logic, sizeSoFar := actual File Size + current compressed data (RowGroupTotalBytesWritten())
	// we can find out current row group's compressed size even before completing the entire row group
	sizeSoFar := int64(rw.currentBuffer.Len()) + rw.currentWriter.RowGroupTotalBytesWritten()
	targetSize := targetDataFileSize
	if fileType == fileTypeEqualityDelete || fileType == fileTypePositionalDelete {
		targetSize = targetDeleteFileSize
	}

	if sizeSoFar < targetSize {
		return nil
	}

	if err := rw.currentWriter.Close(); err != nil {
		return fmt.Errorf("failed to close writer during flush: %s", err)
	}

	uploadData := &FileUploadData{
		FileType:    fileType,
		FileData:    rw.currentBuffer.Bytes(),
		Partition:   rw.partitionValues,
		RecordCount: rw.currentRowCount,
	}

	if err := w.uploadFile(ctx, uploadData, rw.filePath); err != nil {
		return fmt.Errorf("failed to upload parquet during flush: %s", err)
	}

	delete(w.writers, writerKey(fileType, partitionKey))
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

	commitCtx, cancel := context.WithTimeout(ctx, 3600*time.Second)
	defer cancel()

	if _, err := w.server.SendClientRequest(commitCtx, commitRequest); err != nil {
		return fmt.Errorf("failed to commit arrow files: %s", err)
	}

	return nil
}

func (w *ArrowWriter) completeWriters(ctx context.Context) error {
	for key, rw := range w.writers {
		if rw.currentRowCount == 0 {
			continue
		}

		if err := rw.currentWriter.Close(); err != nil {
			return fmt.Errorf("failed to close writer: %s", err)
		}

		fileType, _ := parseWriterKey(key)
		fileData := make([]byte, rw.currentBuffer.Len())
		copy(fileData, rw.currentBuffer.Bytes())

		uploadData := &FileUploadData{
			FileType:    fileType,
			FileData:    fileData,
			Partition:   rw.partitionValues,
			RecordCount: rw.currentRowCount,
		}

		if err := w.uploadFile(ctx, uploadData, rw.filePath); err != nil {
			return fmt.Errorf("failed to upload parquet: %s", err)
		}
	}

	w.writers = make(map[string]*RollingWriter)
	return nil
}

func (w *ArrowWriter) initialize(ctx context.Context) error {
	if err := w.fetchfileschemajson(ctx); err != nil {
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

func (w *ArrowWriter) getOrCreateWriter(ctx context.Context, partition Partition, schema arrow.Schema, fileType string) (*RollingWriter, error) {
	key := writerKey(fileType, partition.Key)

	if rw, exists := w.writers[key]; exists {
		return rw, nil
	}

	filePath, err := w.allocateFilePath(ctx, partition.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate file path: %s", err)
	}

	rw, err := w.newRollingWriter(schema, fileType, partition, filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create rolling writer: %s", err)
	}

	w.writers[key] = rw
	return rw, nil
}

func (w *ArrowWriter) newRollingWriter(arrowSchema arrow.Schema, fileType string, partition Partition, filePath string) (*RollingWriter, error) {
	buf := &bytes.Buffer{}

	kvMeta := make(metadata.KeyValueMetadata, 0)
	_ = kvMeta.Append("iceberg.schema", w.fileschemajson[fileType])

	if fileType == fileTypeEqualityDelete {
		_ = kvMeta.Append("delete-type", "equality")
		fieldIDStr, _ := arrowSchema.Field(0).Metadata.GetValue("PARQUET:field_id")
		_ = kvMeta.Append("delete-field-ids", fieldIDStr)
	}

	pqWriter, err := newParquetWriter(&arrowSchema, buf, getDefaultWriterProps(), kvMeta)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet writer: %s", err)
	}

	return &RollingWriter{
		filePath:        filePath,
		currentWriter:   pqWriter,
		currentBuffer:   buf,
		currentRowCount: 0,
		partitionValues: partition,
		olakeIDPosition: make(map[string]int64),
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

	basePath := resp.(*proto.ArrowIngestResponse).GetResult()

	if partitionKey == "" {
		return basePath, nil
	}

	// Insert partition key into path: dir/file.parquet → dir/partitionKey/file.parquet
	if idx := strings.LastIndex(basePath, "/"); idx > 0 {
		return basePath[:idx] + "/" + partitionKey + "/" + basePath[idx+1:], nil
	}

	return basePath, nil
}

func (w *ArrowWriter) uploadFile(ctx context.Context, uploadData *FileUploadData, filePath string) error {
	request := &proto.ArrowPayload{
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

	uploadCtx, cancel := context.WithTimeout(ctx, 3600*time.Second)
	defer cancel()

	if _, err := w.server.SendClientRequest(uploadCtx, request); err != nil {
		return fmt.Errorf("failed to upload %s file: %s", uploadData.FileType, err)
	}

	protoPartitionValues, err := toProtoPartitionValues(uploadData.Partition.Values)
	if err != nil {
		return fmt.Errorf("failed to convert partition values: %s", err)
	}

	fileMeta := &proto.ArrowPayload_FileMetadata{
		FileType:        uploadData.FileType,
		FilePath:        filePath,
		RecordCount:     uploadData.RecordCount,
		PartitionValues: protoPartitionValues,
	}

	pKey := uploadData.Partition.Key
	if w.createdFiles[pKey] == nil {
		w.createdFiles[pKey] = &PartitionFiles{}
	}

	pf := w.createdFiles[pKey]
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

func (w *ArrowWriter) fetchfileschemajson(ctx context.Context) error {
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

	w.fileschemajson = resp.(*proto.ArrowIngestResponse).GetIcebergSchemas()
	return nil
}

// writerKey creates a map key for writers
func writerKey(fileType, partitionKey string) string {
	return fileType + ":" + partitionKey
}

// parseWriterKey extracts fileType and partitionKey from a writer key.
func parseWriterKey(key string) (fileType, partitionKey string) {
	parts := strings.SplitN(key, ":", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return parts[0], ""
}
