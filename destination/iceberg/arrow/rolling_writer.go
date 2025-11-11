package arrow

import (
	"bytes"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/datazip-inc/olake/utils/logger"
)

const (
	dataFileTargetSize = int64(350*1024*1024 - 1*1024*1024)
	// the delete file target size is as per Apache Iceberg
	// https://github.com/apache/iceberg/blob/68e555b94f4706a2af41dcb561c84007230c0bc1/core/src/main/java/org/apache/iceberg/TableProperties.java#L323
	deleteFileTargetSize = int64(64*1024*1024 - 1*1024*1024)
	streamChunkSize      = int64(8 * 1024 * 1024)
)

type FilenameGenerator func() (string, error)

type FileUploadData struct {
	FileType        string
	FileData        []byte
	PartitionKey    string
	Filename        string
	EqualityFieldId int
	RecordCount     int64
}

type RollingWriter struct {
	partitionKey string

	FieldId     int
	fileType    string
	FilenameGen FilenameGenerator

	currentWriter         *pqarrow.FileWriter
	currentBuffer         *bytes.Buffer
	currentFile           string
	currentRowCount       int64
	currentCompressedSize int64
}

func NewRollingWriter(partitionKey string, fileType string) *RollingWriter {
	return &RollingWriter{
		partitionKey: partitionKey,
		fileType:     fileType,
	}
}

func (r *RollingWriter) flush() (*FileUploadData, error) {
	if r.currentWriter == nil {
		return nil, nil
	}

	if err := r.currentWriter.Close(); err != nil {
		return nil, err
	}

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
		RecordCount:     r.currentRowCount,
	}

	if r.currentBuffer != nil {
		r.currentBuffer.Reset()
		r.currentBuffer = nil
	}

	r.currentWriter = nil
	r.currentRowCount = 0
	r.currentFile = ""
	r.currentCompressedSize = 0

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

func (r *RollingWriter) Write(record arrow.Record) (*FileUploadData, error) {
	if r.currentWriter == nil {
		filename, err := r.FilenameGen()
		if err != nil {
			record.Release()
			return nil, fmt.Errorf("failed to generate filename: %w", err)
		}
		r.currentFile = filename
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
		if r.fileType == "delete" {
			logger.Infof("Starting delete file: %s with field ID %d", r.currentFile, r.FieldId)
		} else {
			logger.Infof("Starting data file: %s (partition: %s)", r.currentFile, r.partitionKey)
		}
	}

	if err := r.currentWriter.WriteBuffered(record); err != nil {
		return nil, fmt.Errorf("failed to write buffered record: %w", err)
	}

	r.currentRowCount += record.NumRows()
	record.Release()

	// logic : all previously flushed row groups (in buffer) + current in-progress row group (buffered in memory)
	sizeSoFar := int64(r.currentBuffer.Len()) + r.currentWriter.RowGroupTotalBytesWritten()

	r.currentCompressedSize = sizeSoFar

	var targetFileSize int64
	if r.fileType == "data" {
		targetFileSize = dataFileTargetSize
	} else {
		targetFileSize = deleteFileTargetSize
	}

	if r.currentCompressedSize >= targetFileSize {
		uploadData, err := r.flush()
		if err != nil {
			return nil, err
		}
		return uploadData, nil
	}

	return nil, nil
}
