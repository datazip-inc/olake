package iceberg

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/datazip-inc/olake/constants"
	arrow_writer "github.com/datazip-inc/olake/destination/iceberg/arrow"
	"github.com/datazip-inc/olake/types"
)

type ArrowWriter struct {
	iceberg *Iceberg

	unpartitionedWriter *arrow_writer.RollingWriter
	partitionedWriter   *arrow_writer.Fanout
	// deleteFileWriter removed - now creating partition-specific delete writers on demand

	fields       []arrow.Field
	isNormalized bool
}

func (i *Iceberg) NewArrowWriter() (*ArrowWriter, error) {
	arrowWriter := &ArrowWriter{
		iceberg:      i,
		isNormalized: i.stream.NormalizationEnabled(),
	}

	if arrowWriter.isNormalized {
		arrowWriter.fields = arrow_writer.CreateNormFields(i.schema)
	} else {
		arrowWriter.fields = arrow_writer.CreateDeNormFields()
	}

	return arrowWriter, nil
}

// uploadAndTrack uploads a file to Iceberg and tracks it in createdFilePaths
func (aw *ArrowWriter) uploadAndTrack(ctx context.Context, uploadData *arrow_writer.FileUploadData) error {
	if uploadData == nil {
		return nil
	}

	storagePath, err := aw.iceberg.UploadParquetFile(ctx, uploadData.FileData, uploadData.FileType,
		uploadData.PartitionKey, uploadData.Filename, uploadData.EqualityFieldId)
	if err != nil {
		return fmt.Errorf("failed to upload %s file %s: %w", uploadData.FileType, uploadData.Filename, err)
	}

	if aw.iceberg.createdFilePaths == nil {
		aw.iceberg.createdFilePaths = make([][]string, 0)
	}
	aw.iceberg.createdFilePaths = append(aw.iceberg.createdFilePaths, []string{uploadData.FileType, storagePath})

	return nil
}

// uploadDataList uploads multiple files and tracks them
func (aw *ArrowWriter) uploadDataList(ctx context.Context, dataList []*arrow_writer.FileUploadData) error {
	for _, uploadData := range dataList {
		if err := aw.uploadAndTrack(ctx, uploadData); err != nil {
			return err
		}
	}
	return nil
}

func (aw *ArrowWriter) Write(ctx context.Context, records []types.RawRecord) error {
	if len(records) == 0 {
		return nil
	}

	now := time.Now().UTC()
	for i := range records {
		records[i].OlakeTimestamp = now
	}

	arrowFields := aw.fields

	if len(aw.iceberg.partitionInfo) != 0 {
		if aw.partitionedWriter == nil {
			// Convert iceberg.PartitionInfo to arrow.PartitionInfo
			arrowPartitions := make([]arrow_writer.PartitionInfo, len(aw.iceberg.partitionInfo))
			for i, p := range aw.iceberg.partitionInfo {
				arrowPartitions[i] = arrow_writer.PartitionInfo{
					Field:     p.Field,
					Transform: p.Transform,
				}
			}
			aw.partitionedWriter = arrow_writer.NewFanoutWriter(arrowPartitions, aw.iceberg.schema)
			aw.partitionedWriter.Normalization = aw.iceberg.stream.NormalizationEnabled()
			aw.partitionedWriter.FilenameGen = func() (string, error) {
				return aw.iceberg.GenerateFilename(ctx)
			}
			aw.partitionedWriter.FieldIdFunc = aw.iceberg.GetFieldId
		}

		uploadDataList, err := aw.partitionedWriter.Write(ctx, records, arrowFields)
		if err != nil {
			return fmt.Errorf("failed to write partitioned data using fanout writer: %v", err)
		}

		return aw.uploadDataList(ctx, uploadDataList)
	} else {
		if aw.unpartitionedWriter == nil {
			aw.unpartitionedWriter = arrow_writer.NewRollingWriter("", "data")
			aw.unpartitionedWriter.FilenameGen = func() (string, error) {
				return aw.iceberg.GenerateFilename(ctx)
			}
		}

		deletes := make([]types.RawRecord, 0)

		for _, rec := range records {
			if rec.OperationType == "d" || rec.OperationType == "u" || rec.OperationType == "c" {
				r := types.RawRecord{OlakeID: rec.OlakeID}
				deletes = append(deletes, r)
			}
		}

		if len(deletes) > 0 {
			fieldId, err := aw.iceberg.GetFieldId(ctx, constants.OlakeID)
			if err != nil {
				return fmt.Errorf("failed to get field ID for %s: %w", constants.OlakeID, err)
			}

			deleteWriter := arrow_writer.NewRollingWriter("", "delete")
			deleteWriter.FieldId = fieldId
			deleteWriter.FilenameGen = func() (string, error) {
				return aw.iceberg.GenerateFilename(ctx)
			}

			deleteRecordsOnly := arrow_writer.ExtractDeleteRecords(deletes)
			rec, err := arrow_writer.CreateDelArrRecord(deleteRecordsOnly, fieldId)
			if err != nil {
				return fmt.Errorf("failed to create delete record: %w", err)
			}
			defer rec.Release()

			uploadData, err := deleteWriter.Write(rec)
			if err != nil {
				return fmt.Errorf("failed to write delete arrow record: %w", err)
			}

			if err := aw.uploadAndTrack(ctx, uploadData); err != nil {
				return err
			}

			finalUpload, err := deleteWriter.Close()
			if err != nil {
				return fmt.Errorf("failed to close delete writer: %w", err)
			}

			if err := aw.uploadAndTrack(ctx, finalUpload); err != nil {
				return err
			}
		}

		rec, err := arrow_writer.CreateArrowRecordWithFields(records, arrowFields, aw.isNormalized)
		if err != nil {
			return fmt.Errorf("failed to create arrow record: %w", err)
		}

		defer rec.Release()

		uploadData, err := aw.unpartitionedWriter.Write(rec)
		if err != nil {
			return fmt.Errorf("failed to write arrow record to parquet: %w", err)
		}

		return aw.uploadAndTrack(ctx, uploadData)
	}
}

func (aw *ArrowWriter) Close() ([][]string, error) {
	ctx := context.Background()

	if aw.partitionedWriter != nil {
		uploadDataList, err := aw.partitionedWriter.Close()
		if err != nil {
			return nil, err
		}

		if err := aw.uploadDataList(ctx, uploadDataList); err != nil {
			return nil, err
		}
	}

	if aw.unpartitionedWriter != nil {
		uploadData, err := aw.unpartitionedWriter.Close()
		if err != nil {
			return nil, err
		}

		if err := aw.uploadAndTrack(ctx, uploadData); err != nil {
			return nil, err
		}
	}

	return aw.iceberg.createdFilePaths, nil
}
