package iceberg

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	arrow_writer "github.com/datazip-inc/olake/destination/iceberg/arrow"
	"github.com/datazip-inc/olake/types"
)

type ArrowWriter struct {
	iceberg *Iceberg

	unpartitionedWriter *arrow_writer.RollingWriter
	partitionedWriter   *arrow_writer.Fanout
	deleteFileWriter    *arrow_writer.RollingWriter

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

func (aw *ArrowWriter) ArrowWrites(ctx context.Context, records []types.RawRecord) error {
	if len(records) == 0 {
		return nil
	}

	arrowFields := aw.fields

	if len(aw.iceberg.partitionInfo) != 0 {
		if aw.partitionedWriter == nil {
			aw.partitionedWriter = arrow_writer.NewFanoutWriter(context.Background(), aw.iceberg.partitionInfo, aw.iceberg.schema)
			aw.partitionedWriter.Normalization = aw.iceberg.stream.NormalizationEnabled()
		}

		uploadDataList, err := aw.partitionedWriter.Write(ctx, records, arrowFields)
		if err != nil {
			return fmt.Errorf("failed to write partitioned data using fanout writer: %v", err)
		}

		for _, uploadData := range uploadDataList {
			if uploadData != nil {
				storagePath, err := aw.iceberg.UploadParquetFile(ctx, uploadData.FileData, uploadData.FileType,
					uploadData.PartitionKey, uploadData.Filename, uploadData.EqualityFieldId)
				if err != nil {
					return fmt.Errorf("failed to upload file %s via Iceberg FileIO: %w", uploadData.Filename, err)
				}
				if aw.iceberg.createdFilePaths == nil {
					aw.iceberg.createdFilePaths = make([][]string, 0)
				}
				aw.iceberg.createdFilePaths = append(aw.iceberg.createdFilePaths, []string{uploadData.FileType, storagePath})
			}
		}
	} else {
		if aw.unpartitionedWriter == nil {
			aw.unpartitionedWriter = arrow_writer.NewRollingWriter(context.Background(), "", "data")
		}

		deletes := make([]types.RawRecord, 0)

		for _, rec := range records {
			if rec.OperationType == "d" {
				r := types.RawRecord{OlakeID: rec.OlakeID}
				deletes = append(deletes, r)
			}
		}

		if len(deletes) > 0 {
			fieldId, err := aw.iceberg.GetFieldId(ctx, "_olake_id")
			if err != nil {
				return fmt.Errorf("failed to get field ID for _olake_id: %w", err)
			}

			if aw.deleteFileWriter == nil {
				aw.deleteFileWriter = arrow_writer.NewRollingWriter(context.Background(), "", "delete")
				aw.deleteFileWriter.FieldId = fieldId
			}

			rec, err := arrow_writer.CreateDeleteFiles(deletes, fieldId)
			if err != nil {
				return fmt.Errorf("failed to create delete record: %w", err)
			}

			defer rec.Release()

			uploadData, err := aw.deleteFileWriter.Write(rec)
			if err != nil {
				return fmt.Errorf("failed to write arrow record to parquet: %w", err)
			}

			if uploadData != nil {
				storagePath, err := aw.iceberg.UploadParquetFile(ctx, uploadData.FileData, uploadData.FileType,
					uploadData.PartitionKey, uploadData.Filename, uploadData.EqualityFieldId)
				if err != nil {
					return fmt.Errorf("failed to upload delete file via Iceberg FileIO: %w", err)
				}
				if aw.iceberg.createdFilePaths == nil {
					aw.iceberg.createdFilePaths = make([][]string, 0)
				}

				aw.iceberg.createdFilePaths = append(aw.iceberg.createdFilePaths, []string{uploadData.FileType, storagePath})
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

		if uploadData != nil {
			storagePath, err := aw.iceberg.UploadParquetFile(ctx, uploadData.FileData, uploadData.FileType,
				uploadData.PartitionKey, uploadData.Filename, uploadData.EqualityFieldId)
			if err != nil {
				return fmt.Errorf("failed to upload data file via Iceberg FileIO: %w", err)
			}
			if aw.iceberg.createdFilePaths == nil {
				aw.iceberg.createdFilePaths = make([][]string, 0)
			}
			aw.iceberg.createdFilePaths = append(aw.iceberg.createdFilePaths, []string{uploadData.FileType, storagePath})
		}
	}

	return nil
}

func (aw *ArrowWriter) Close() ([][]string, error) {
	ctx := context.Background()
	outputFilePaths := make([][]string, 0)

	if aw.deleteFileWriter != nil {
		uploadData, err := aw.deleteFileWriter.Close()
		if err != nil {
			return outputFilePaths, err
		}
		if uploadData != nil {
			storagePath, err := aw.iceberg.UploadParquetFile(ctx, uploadData.FileData, uploadData.FileType,
				uploadData.PartitionKey, uploadData.Filename, uploadData.EqualityFieldId)
			if err != nil {
				return outputFilePaths, fmt.Errorf("failed to upload delete file on close: %w", err)
			}

			outputFilePaths = append(outputFilePaths, []string{uploadData.FileType, storagePath})
		}
	}

	if aw.partitionedWriter != nil {
		uploadDataList, err := aw.partitionedWriter.Close()
		if err != nil {
			return outputFilePaths, err
		}

		for _, uploadData := range uploadDataList {
			if uploadData != nil {
				storagePath, err := aw.iceberg.UploadParquetFile(ctx, uploadData.FileData, uploadData.FileType,
					uploadData.PartitionKey, uploadData.Filename, uploadData.EqualityFieldId)
				if err != nil {
					return outputFilePaths, fmt.Errorf("failed to upload file on close: %w", err)
				}
				outputFilePaths = append(outputFilePaths, []string{uploadData.FileType, storagePath})
			}
		}
	}

	if aw.unpartitionedWriter != nil {
		uploadData, err := aw.unpartitionedWriter.Close()
		if err != nil {
			return outputFilePaths, err
		}

		if uploadData != nil {
			storagePath, err := aw.iceberg.UploadParquetFile(ctx, uploadData.FileData, uploadData.FileType,
				uploadData.PartitionKey, uploadData.Filename, uploadData.EqualityFieldId)
			if err != nil {
				return outputFilePaths, fmt.Errorf("failed to upload data file on close: %w", err)
			}
			outputFilePaths = append(outputFilePaths, []string{uploadData.FileType, storagePath})
		}
	}

	return outputFilePaths, nil
}
