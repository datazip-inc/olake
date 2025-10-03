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

type ArrowWriter2 struct {
	iceberg             *Iceberg
	unpartitionedWriter *arrow_writer.RollingDataWriter
	partitionedWriter   *arrow_writer.Fanout

	s3Client   *s3.Client
	bucketName string
	prefix     string

	// Cache arrow fields to avoid recreation on every write
	cachedArrowFields []arrow.Field
	isNormalized      bool
}

func (i *Iceberg) NewArrowWriter2(bucketName, prefix, accessKey, secretKey, region string) (*ArrowWriter2, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")), config.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	arrowWriter := &ArrowWriter2{
		iceberg:      i,
		s3Client:     s3.NewFromConfig(cfg),
		bucketName:   bucketName,
		prefix:       prefix,
		isNormalized: i.stream.NormalizationEnabled(),
	}

	// Pre-compute and cache arrow fields to avoid recreation on every write
	if arrowWriter.isNormalized {
		arrowWriter.cachedArrowFields = arrow_writer.CreateNormFields(i.schema)
	} else {
		arrowWriter.cachedArrowFields = arrow_writer.CreateDeNormFields()
	}

	return arrowWriter, nil
}

func (aw *ArrowWriter2) ArrowWrites(ctx context.Context, records []types.RawRecord) error {
	if len(records) == 0 {
		return nil
	}

	// Use cached arrow fields instead of recreating them
	arrowFields := aw.cachedArrowFields

	if len(aw.iceberg.partitionInfo) != 0 {
		// Initialize partitioned writer once
		if aw.partitionedWriter == nil {
			// Use background context to avoid cancellation issues during Close()
			aw.partitionedWriter = arrow_writer.NewFanoutWriter(context.Background(), aw.iceberg.partitionInfo, aw.iceberg.schema)
			aw.partitionedWriter.S3Config = arrow_writer.NewS3Config(aw.s3Client, aw.bucketName, aw.prefix)
			aw.partitionedWriter.Normalization = aw.iceberg.stream.NormalizationEnabled()
		}

		filePaths, err := aw.partitionedWriter.Write(ctx, records, arrowFields)
		if err != nil {
			return fmt.Errorf("failed to write partitioned data using fanout writer: %v", err)
		}
		if len(filePaths) != 0 {
			if aw.iceberg.createdFilePaths == nil {
				aw.iceberg.createdFilePaths = make([]string, 0, len(filePaths))
			}
			aw.iceberg.createdFilePaths = append(aw.iceberg.createdFilePaths, filePaths...)
		}
	} else {
		if aw.unpartitionedWriter == nil {
			aw.unpartitionedWriter = arrow_writer.NewRollingDataWriter(context.Background(), "")
			aw.unpartitionedWriter.S3Config = arrow_writer.NewS3Config(aw.s3Client, aw.bucketName, aw.prefix)
		}

		rec, err := arrow_writer.CreateArrowRecordWithFields(records, arrowFields, aw.isNormalized)
		if err != nil {
			return fmt.Errorf("failed to create arrow record: %w", err)
		}
		defer rec.Release()

		filePath, err := aw.unpartitionedWriter.Write(rec)
		if err != nil {
			return fmt.Errorf("failed to write arrow record to parquet: %w", err)
		}
		if filePath != "" {
			if aw.iceberg.createdFilePaths == nil {
				aw.iceberg.createdFilePaths = make([]string, 0)
			}
			aw.iceberg.createdFilePaths = append(aw.iceberg.createdFilePaths, filePath)
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

// Close flushes any remaining data and closes all writers
func (aw *ArrowWriter2) Close() ([]string, error) {
	outputFilePaths := make([]string, 0)
	if aw.partitionedWriter != nil {
		filePaths, err := aw.partitionedWriter.Close()
		if err != nil {
			return outputFilePaths, err
		}
		outputFilePaths = append(outputFilePaths, filePaths...)
	}

	if aw.unpartitionedWriter != nil {
		filePath, err := aw.unpartitionedWriter.Close()
		if err != nil {
			return outputFilePaths, err
		}
		if filePath != "" {
			outputFilePaths = append(outputFilePaths, filePath)
		}
	}

	return outputFilePaths, nil
}
