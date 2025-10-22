package iceberg

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	arrow_writer "github.com/datazip-inc/olake/destination/iceberg/arrow"
	"github.com/datazip-inc/olake/types"
)

type ArrowWriter struct {
	iceberg             *Iceberg
	unpartitionedWriter *arrow_writer.RollingDataWriter
	partitionedWriter   *arrow_writer.Fanout
	deleteFileWriter    *arrow_writer.RollingDeleteWriter

	s3Client   *s3.Client
	bucketName string
	prefix     string

	cachedArrowFields []arrow.Field
	isNormalized      bool
}

func (i *Iceberg) NewArrowWriter(bucketName, prefix, accessKey, secretKey, region string) (*ArrowWriter, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")), config.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	arrowWriter := &ArrowWriter{
		iceberg:      i,
		s3Client:     s3.NewFromConfig(cfg),
		bucketName:   bucketName,
		prefix:       prefix,
		isNormalized: i.stream.NormalizationEnabled(),
	}

	if arrowWriter.isNormalized {
		arrowWriter.cachedArrowFields = arrow_writer.CreateNormFields(i.schema)
	} else {
		arrowWriter.cachedArrowFields = arrow_writer.CreateDeNormFields()
	}

	return arrowWriter, nil
}

func (aw *ArrowWriter) ArrowWrites(ctx context.Context, records []types.RawRecord) error {
	if len(records) == 0 {
		return nil
	}

	arrowFields := aw.cachedArrowFields

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
		data := make([]types.RawRecord, 0)
		deletes := make([]types.RawRecord, 0)

		for _, rec := range records {
			if rec.OperationType == "d" {
				r := types.RawRecord{OlakeID: rec.OlakeID}
				deletes = append(deletes, r)
			}

			// i think data == records, need to check once
			if aw.unpartitionedWriter == nil {
				aw.unpartitionedWriter = arrow_writer.NewRollingDataWriter(context.Background(), "")
			}

			data = append(data, rec)
		}

		if len(deletes) > 0 {
			fieldId, err := aw.iceberg.GetFieldId(ctx, "_olake_id")
			if err != nil {
				return fmt.Errorf("failed to get field ID for _olake_id: %w", err)
			}

			fmt.Println("☀️ _olake_id field ID:", fieldId)

			if aw.deleteFileWriter == nil {
				aw.deleteFileWriter = arrow_writer.NewRollingDeleteWriter(context.Background(), "", fieldId)
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
				fmt.Println("✅ adding delete file path")
				aw.iceberg.createdFilePaths = append(aw.iceberg.createdFilePaths, []string{uploadData.FileType, storagePath})
			}
		}

		if len(data) > 0 {
			rec, err := arrow_writer.CreateArrowRecordWithFields(data, arrowFields, aw.isNormalized)
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
	}

	return nil
}

func (i *Iceberg) parseS3Path() (bucketName, prefix string, err error) {
	s3Path := i.config.IcebergS3Path
	if s3Path == "" {
		return "", "", fmt.Errorf("iceberg_s3_path is not configured")
	}

	if !strings.HasPrefix(s3Path, "s3://") {
		return "", "", fmt.Errorf("invalid S3 path format, must start with s3://")
	}

	path := strings.TrimPrefix(s3Path, "s3://")
	parts := strings.SplitN(path, "/", 2)

	bucketName = parts[0]
	if len(parts) > 1 {
		prefix = parts[1]
	}

	return bucketName, prefix, nil
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
		// Upload each file via Iceberg FileIO
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
