package iceberg

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/datazip-inc/olake/constants"
	arrow_writer "github.com/datazip-inc/olake/destination/iceberg/arrow"
	"github.com/datazip-inc/olake/destination/iceberg/proto"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
)

type ArrowWriter struct {
	iceberg           *Iceberg
	icebergSchemaJSON string

	unpartitionedDataWriter   *arrow_writer.RollingWriter
	unpartitionedDeleteWriter *arrow_writer.RollingWriter
	partitionedWriter         *arrow_writer.Fanout

	fields   []arrow.Field
	fieldIds map[string]int // iceberg schema column ids
}

func (i *Iceberg) NewArrowWriter(ctx context.Context) (*ArrowWriter, error) {
	arrowWriter := &ArrowWriter{
		iceberg: i,
	}

	arrowWriter.intitializeFields(ctx)

	if len(i.partitionInfo) != 0 {
		arrowPartitions := make([]arrow_writer.PartitionInfo, len(i.partitionInfo))
		for idx, p := range i.partitionInfo {
			arrowPartitions[idx] = arrow_writer.PartitionInfo{
				Field:     p.Field,
				Transform: p.Transform,
			}
		}

		arrowWriter.partitionedWriter = arrow_writer.NewFanoutWriter(arrowPartitions, i.schema, arrowWriter.fieldIds, arrowWriter.icebergSchemaJSON, arrowWriter)
		arrowWriter.partitionedWriter.Normalization = i.stream.NormalizationEnabled()
	} else {
		arrowWriter.unpartitionedDataWriter = arrow_writer.NewRollingWriter("", "data", arrowWriter)
		arrowWriter.unpartitionedDataWriter.IcebergSchemaJSON = arrowWriter.icebergSchemaJSON
	}

	return arrowWriter, nil
}

func (aw *ArrowWriter) intitializeFields(ctx context.Context) error {
	aw.fieldIds = make(map[string]int)

	// TODO: batch this operation
	for fieldName := range aw.iceberg.schema {
		fieldId, err := aw.GetFieldId(ctx, fieldName)
		if err != nil {
			return fmt.Errorf("failed to get field ID for column %s: %w", fieldName, err)
		}

		aw.fieldIds[fieldName] = fieldId
	}

	if aw.iceberg.stream.NormalizationEnabled() {
		aw.fields = arrow_writer.CreateNormFields(aw.iceberg.schema, aw.fieldIds)
	} else {
		aw.fields = arrow_writer.CreateDeNormFields(aw.fieldIds)
	}

	aw.icebergSchemaJSON = arrow_writer.BuildIcebergSchemaJSON(aw.iceberg.schema, aw.fieldIds)

	return nil
}

func (aw *ArrowWriter) UploadFile(ctx context.Context, uploadData *arrow_writer.FileUploadData) error {
	if uploadData == nil {
		return nil
	}

	filePath, err := aw.uploadParquetFile(ctx, uploadData.FileData, uploadData.FileType, uploadData.PartitionKey, uploadData.Filename, uploadData.EqualityFieldId)
	if err != nil {
		return fmt.Errorf("failed to upload %s file %s: %w", uploadData.FileType, uploadData.Filename, err)
	}

	aw.iceberg.createdFilePaths = append(aw.iceberg.createdFilePaths, FileMetadata{
		FileType:    uploadData.FileType,
		FilePath:    filePath,
		RecordCount: uploadData.RecordCount,
	})

	return nil
}

func (aw *ArrowWriter) Write(ctx context.Context, records []types.RawRecord) error {
	if len(records) == 0 {
		return nil
	}

	// java-writer does not pass the OLakeTimeStamp for denormalized table
	now := time.Now().UTC()
	for i := range records {
		records[i].OlakeTimestamp = now
	}

	if len(aw.iceberg.partitionInfo) != 0 {
		err := aw.partitionedWriter.Write(ctx, records, aw.fields)
		if err != nil {
			return fmt.Errorf("failed to write partitioned data using fanout writer: %w", err)
		}

		return nil
	} else {
		deletes := arrow_writer.ExtractDeleteRecords(records)

		if len(deletes) > 0 {
			// for equality delete files, we need to store the iceberg schema "field-id" of OLakeID column explicitly
			// in the metadata of the delete file
			fieldId := aw.fieldIds[constants.OlakeID]

			if aw.unpartitionedDeleteWriter == nil {
				aw.unpartitionedDeleteWriter = arrow_writer.NewRollingWriter("", "delete", aw)
				aw.unpartitionedDeleteWriter.FieldId = fieldId
			}

			rec, err := arrow_writer.CreateDelArrowRec(deletes, fieldId)
			if err != nil {
				return fmt.Errorf("failed to create delete record: %w", err)
			}

			uploadData, err := aw.unpartitionedDeleteWriter.Write(ctx, rec)
			if err != nil {
				return fmt.Errorf("failed to write delete arrow record: %w", err)
			}

			if err := aw.UploadFile(ctx, uploadData); err != nil {
				return err
			}
		}

		rec, err := arrow_writer.CreateArrowRecord(records, aw.fields, aw.iceberg.stream.NormalizationEnabled())
		if err != nil {
			return fmt.Errorf("failed to create arrow record: %w", err)
		}

		uploadData, err := aw.unpartitionedDataWriter.Write(ctx, rec)
		if err != nil {
			return fmt.Errorf("failed to write arrow record to parquet: %w", err)
		}

		return aw.UploadFile(ctx, uploadData)
	}
}

func (aw *ArrowWriter) Close(ctx context.Context) error {
	var errs []error

	if aw.partitionedWriter != nil {
		if err := aw.partitionedWriter.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("partitioned writer close: %w", err))
		}
	}

	if aw.unpartitionedDataWriter != nil {
		uploadData, err := aw.unpartitionedDataWriter.Close()
		if err != nil {
			errs = append(errs, fmt.Errorf("data writer close: %w", err))
		} else if err := aw.UploadFile(ctx, uploadData); err != nil {
			errs = append(errs, fmt.Errorf("data file upload: %w", err))
		}
	}

	if aw.unpartitionedDeleteWriter != nil {
		uploadData, err := aw.unpartitionedDeleteWriter.Close()
		if err != nil {
			errs = append(errs, fmt.Errorf("delete writer close: %w", err))
		} else if err := aw.UploadFile(ctx, uploadData); err != nil {
			errs = append(errs, fmt.Errorf("delete file upload: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("error while closing arrow writer: %v", errors.Join(errs...))
	}

	return nil
}

func (aw *ArrowWriter) uploadParquetFile(ctx context.Context, fileData []byte, fileType, partitionKey, filename string, equalityFieldId int) (string, error) {
	request := proto.ArrowPayload{
		Type: proto.ArrowPayload_UPLOAD_FILE,
		Metadata: &proto.ArrowPayload_Metadata{
			DestTableName: aw.iceberg.stream.GetDestinationTable(),
			ThreadId:      aw.iceberg.server.serverID,
			FileUpload: &proto.ArrowPayload_FileUploadRequest{
				FileData:        fileData,
				FileType:        fileType,
				PartitionKey:    partitionKey,
				Filename:        filename,
				EqualityFieldId: int32(equalityFieldId),
			},
		},
	}

	response, err := aw.iceberg.server.sendArrowRequest(ctx, &request)
	if err != nil {
		return "", fmt.Errorf("failed to upload file '%s' via Iceberg FileIO: %w", filename, err)
	}

	logger.Infof("Successfully uploaded file to Iceberg storage: %s", response.GetResult())

	return response.GetResult(), nil
}

// GenerateFilename generates a unique filename for Iceberg Data and Delete Files
// Following the Iceberg standard: https://github.com/apache/iceberg/blob/059310ead702814e5d95116210b9af77b29f6b94/core/src/main/java/org/apache/iceberg/io/OutputFileFactory.java#L93-L103
// TODO: needs fixing
func (aw *ArrowWriter) GenerateFilename(ctx context.Context) (string, error) {
	request := proto.ArrowPayload{
		Type: proto.ArrowPayload_GENERATE_FILENAME,
		Metadata: &proto.ArrowPayload_Metadata{
			DestTableName: aw.iceberg.stream.GetDestinationTable(),
			ThreadId:      aw.iceberg.server.serverID,
		},
	}

	resp, err := aw.iceberg.server.sendArrowRequest(ctx, &request)
	if err != nil {
		return "", fmt.Errorf("failed to generate filename: %w", err)
	}

	filename := resp.GetFilename()
	if filename == "" {
		return "", fmt.Errorf("server returned empty filename")
	}

	return filename, nil
}

// This function returns the 'field-id' of a column from the iceberg table
// For delete files this is required while creating equality delete files as the `field-id` value of the column
// is stored in the metadata of the equality delete file,
// For data files, this is required while storing the iceberg schema json in the metadata of the parquet file
// Possible Improvement: hard code the olake fields to specific field ids while creating iceberg schema everytime and keep it constant throughout
func (aw *ArrowWriter) GetFieldId(ctx context.Context, fieldName string) (int, error) {
	if aw.iceberg.server == nil {
		return -1, fmt.Errorf("iceberg server not initialized")
	}

	request := proto.ArrowPayload{
		Type: proto.ArrowPayload_GET_FIELD_ID,
		Metadata: &proto.ArrowPayload_Metadata{
			DestTableName: aw.iceberg.stream.GetDestinationTable(),
			ThreadId:      aw.iceberg.server.serverID,
			FieldName:     &fieldName,
		},
	}

	response, err := aw.iceberg.server.sendArrowRequest(ctx, &request)
	if err != nil {
		return -1, fmt.Errorf("failed to get field ID for column '%s': %w", fieldName, err)
	}

	fieldId, err := strconv.Atoi(response.GetResult())
	if err != nil {
		return -1, fmt.Errorf("failed to parse field ID from response '%s': %w", response.GetResult(), err)
	}

	return fieldId, nil
}
