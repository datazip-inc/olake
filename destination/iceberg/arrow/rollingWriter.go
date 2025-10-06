package arrow

import (
	"bytes"
	"context"
	"fmt"
	"regexp"
	"runtime"
	"strings"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/google/uuid"
)

var totalDataFiles atomic.Int64

const (
	targetFileSize  = int64(27 * 1024*1024 - 1 * 1024 * 1024)
	streamChunkSize = int64(8 * 1024 * 1024)
)

func GenerateDataFileName() string {
	// It mimics the behavior in the Java API:
	// https://github.com/apache/iceberg/blob/a582968975dd30ff4917fbbe999f1be903efac02/core/src/main/java/org/apache/iceberg/io/OutputFileFactory.java#L92-L101
	return fmt.Sprintf("00000-%d-%s.parquet", totalDataFiles.Load(), uuid.New())
}

type RollingDataWriter struct {
	partitionKey string
	ctx          context.Context

	currentWriter         *pqarrow.FileWriter
	currentBuffer         *bytes.Buffer
	currentFile           string
	currentSize           int64
	currentRowCount       int64
	currentCompressedSize int64

	S3Config *S3Config
}

func NewRollingDataWriter(ctx context.Context, partitionKey string) *RollingDataWriter {
	writer := &RollingDataWriter{
		partitionKey: partitionKey,
		ctx:          ctx,
	}

	return writer
}

func (r *RollingDataWriter) Write(record arrow.Record) (string, error) {
	if r.currentWriter == nil {
		r.currentFile = GenerateDataFileName()
		r.currentBuffer = &bytes.Buffer{}

		allocator := memory.NewGoAllocator()

		writerProps := parquet.NewWriterProperties(
			parquet.WithCompression(compress.Codecs.Zstd),
			parquet.WithCompressionLevel(1),
			parquet.WithDataPageSize(1*1024*1024),
			parquet.WithDictionaryPageSizeLimit(2*1024*1024),
			parquet.WithDictionaryDefault(false),
			parquet.WithMaxRowGroupLength(streamChunkSize),
			parquet.WithBatchSize(4096),
			parquet.WithStats(true),
			parquet.WithAllocator(allocator),
		)

		arrowProps := pqarrow.NewArrowWriterProperties(
			pqarrow.WithStoreSchema(),
			pqarrow.WithNoMapLogicalType(),
		)

		writer, err := pqarrow.NewFileWriter(record.Schema(), r.currentBuffer, writerProps, arrowProps)
		if err != nil {
			record.Release()

			return "", err
		}

		r.currentWriter = writer
		r.currentRowCount = 0
		r.currentCompressedSize = 0
		logger.Infof("Starting file: %s", r.currentFile)
	}

	if err := r.currentWriter.WriteBuffered(record); err != nil {
		return "", fmt.Errorf("failed to write buffered record: %w", err)
	}

	record.Release()
	r.currentRowCount += record.NumRows()

	sizeSoFar := int64(0)
	if r.currentBuffer != nil {
		sizeSoFar += int64(r.currentBuffer.Len())
	}
	if r.currentWriter != nil {
		sizeSoFar += r.currentWriter.RowGroupTotalCompressedBytes()
	}
	r.currentCompressedSize = sizeSoFar

	if r.currentCompressedSize >= targetFileSize {
		filePath, err := r.flush()
		if err != nil {
			return "", err
		} else if filePath != "" {
			return filePath, nil
		}
	}

	return "", nil
}

func (r *RollingDataWriter) flush() (string, error) {
	if r.currentWriter == nil {
		return "", nil
	}

	if err := r.currentWriter.Close(); err != nil {
		return "", err
	}

	totalDataFiles.Add(1)

	s3Key := r.currentFile
	if r.S3Config.Prefix != "" {
		if r.partitionKey != "" {
			s3Key = fmt.Sprintf("%s/%s/%s", r.S3Config.Prefix, r.partitionKey, r.currentFile)
		} else {
			s3Key = fmt.Sprintf("%s/%s", r.S3Config.Prefix, r.currentFile)
		}
	} else if r.partitionKey != "" {
		s3Key = fmt.Sprintf("%s/%s", r.partitionKey, r.currentFile)
	}

	_, err := r.S3Config.S3Client.PutObject(r.ctx, &s3.PutObjectInput{
		Bucket: aws.String(r.S3Config.BucketName),
		Key:    aws.String(s3Key),
		Body:   bytes.NewReader(r.currentBuffer.Bytes()),
	})
	if err != nil {
		return "", fmt.Errorf("failed to upload to S3: %w", err)
	}

	fileSize := int64(r.currentBuffer.Len())
	s3Path := fmt.Sprintf("s3://%s/%s", r.S3Config.BucketName, s3Key)
	logger.Infof("Successfully uploaded file: %s (%.2f MB, %d records)", s3Path, float64(fileSize)/1024/1024, r.currentRowCount)

	if r.currentBuffer != nil {
		r.currentBuffer.Reset()
		r.currentBuffer = nil
	}

	r.currentWriter = nil
	r.currentRowCount = 0
	r.currentFile = ""
	r.currentSize = 0
	r.currentCompressedSize = 0
	runtime.GC()

	return s3Path, nil
}

func (r *RollingDataWriter) Close() (string, error) {
	if r.currentWriter != nil {
		return r.flush()
	}
	return "", nil
}

func constructColPath(tVal, field, transform string) string {
	if transform == "identity" {
		return fmt.Sprintf("%s=%s", field, tVal)
	}

	re := regexp.MustCompile(`^([a-zA-Z]+)(\[\d+\])?$`)
	matches := re.FindStringSubmatch(transform)
	if len(matches) == 0 {
		return fmt.Sprintf("%s_%s=%s", field, transform, tVal)
	}

	base := strings.ToLower(matches[1])

	switch base {
	case "bucket":
		return fmt.Sprintf("%s_bucket=%s", field, tVal)
	case "truncate":
		return fmt.Sprintf("%s_trunc=%s", field, tVal)
	default:
		return fmt.Sprintf("%s_%s=%s", field, base, tVal)
	}
}
