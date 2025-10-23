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
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/google/uuid"
)

var totalDataFiles atomic.Int64

const (
	targetFileSize  = int64(27*1024*1024 - 1*1024*1024)
	streamChunkSize = int64(8 * 1024 * 1024)
)

type FileUploadData struct {
	FileType        string
	FileData        []byte
	PartitionKey    string
	Filename        string
	EqualityFieldId int
}

func GenerateDataFileName() string {
	// It mimics the behavior in the Java API:
	// https://github.com/apache/iceberg/blob/a582968975dd30ff4917fbbe999f1be903efac02/core/src/main/java/org/apache/iceberg/io/OutputFileFactory.java#L92-L101
	return fmt.Sprintf("00000-%d-%s.parquet", totalDataFiles.Load(), uuid.New())
}

type RollingWriter struct {
	partitionKey string
	ctx          context.Context

	FieldId  int
	fileType string

	currentWriter         *pqarrow.FileWriter
	currentBuffer         *bytes.Buffer
	currentFile           string
	currentSize           int64
	currentRowCount       int64
	currentCompressedSize int64
}

func NewRollingWriter(ctx context.Context, partitionKey string, fileType string) *RollingWriter {
	writer := &RollingWriter{
		partitionKey: partitionKey,
		ctx:          ctx,
		fileType:     fileType,
	}

	return writer
}

func (r *RollingWriter) flush() (*FileUploadData, error) {
	if r.currentWriter == nil {
		return nil, nil
	}

	if err := r.currentWriter.Close(); err != nil {
		return nil, err
	}

	// TODO: create a java api to get the total count of files written
	totalDataFiles.Add(1)

	fileData := make([]byte, r.currentBuffer.Len())
	copy(fileData, r.currentBuffer.Bytes())

	fileSize := int64(len(fileData))
	logger.Infof("Prepared file for upload: %s (%.2f MB, %d records, partition: %s)",
		r.currentFile, float64(fileSize)/1024/1024, r.currentRowCount, r.partitionKey)

	uploadData := &FileUploadData{
		FileType:        r.fileType,
		FileData:        fileData,
		PartitionKey:    r.partitionKey,
		Filename:        r.currentFile,
		EqualityFieldId: r.FieldId,
	}

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

	return uploadData, nil
}

func (r *RollingWriter) Close() (*FileUploadData, error) {
	if r.currentWriter != nil {
		uploadData, err := r.flush()
		if err != nil {
			return nil, err
		}
		return uploadData, nil
	}
	return nil, nil
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

func (r *RollingWriter) Write(record arrow.Record) (*FileUploadData, error) {
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

			return nil, err
		}

		if r.fileType == "delete" {
			icebergSchemaJSON := fmt.Sprintf(`{"type":"struct","schema-id":0,"fields":[{"id":%d,"name":"_olake_id","required":true,"type":"string"}]}`, r.FieldId)

			writer.AppendKeyValueMetadata("delete-type", "equality")
			writer.AppendKeyValueMetadata("delete-field-ids", fmt.Sprintf("%d", r.FieldId))
			writer.AppendKeyValueMetadata("iceberg.schema", icebergSchemaJSON)
		}

		r.currentWriter = writer
		r.currentRowCount = 0
		r.currentCompressedSize = 0
		logger.Infof("Starting delete file: %s with field ID %d", r.currentFile, r.FieldId)
	}

	if err := r.currentWriter.WriteBuffered(record); err != nil {
		return nil, fmt.Errorf("failed to write buffered delete record: %w", err)
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
		uploadData, err := r.flush()
		if err != nil {
			return nil, err
		}
		return uploadData, nil
	}

	return nil, nil
}
