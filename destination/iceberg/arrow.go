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

func (aw *ArrowWriter) uploadFile(ctx context.Context, uploadData *arrow_writer.FileUploadData) error {
	if uploadData == nil {
		return nil
	}

	storagePath, err := aw.iceberg.UploadParquetFile(ctx, uploadData.FileData, uploadData.FileType,
		uploadData.PartitionKey, uploadData.Filename, uploadData.EqualityFieldId)
	if err != nil {
		return fmt.Errorf("failed to upload %s file %s: %w", uploadData.FileType, uploadData.Filename, err)
	}

	if aw.iceberg.createdFilePaths == nil {
		aw.iceberg.createdFilePaths = make([]FileMetadata, 0)
	}
	aw.iceberg.createdFilePaths = append(aw.iceberg.createdFilePaths, FileMetadata{
		FileType:    uploadData.FileType,
		FilePath:    storagePath,
		RecordCount: uploadData.RecordCount,
	})

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
			aw.partitionedWriter.UploadFunc = aw.uploadFile
		}

		err := aw.partitionedWriter.Write(ctx, records, arrowFields)
		if err != nil {
			return fmt.Errorf("failed to write partitioned data using fanout writer: %v", err)
		}

		return nil
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

			if err := aw.uploadFile(ctx, uploadData); err != nil {
				return err
			}

			finalUpload, err := deleteWriter.Close()
			if err != nil {
				return fmt.Errorf("failed to close delete writer: %w", err)
			}

			if err := aw.uploadFile(ctx, finalUpload); err != nil {
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

		return aw.uploadFile(ctx, uploadData)
	}
}

func (aw *ArrowWriter) Close() error {
	ctx := context.Background()

	if aw.partitionedWriter != nil {
		if err := aw.partitionedWriter.Close(ctx); err != nil {
			return err
		}
	}

	if aw.unpartitionedWriter != nil {
		uploadData, err := aw.unpartitionedWriter.Close()
		if err != nil {
			return err
		}

		if err := aw.uploadFile(ctx, uploadData); err != nil {
			return err
		}
	}

	return nil
}
