package arrow

import (
	"bytes"
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils/logger"
)

var (
	// Apache Iceberg sets its default compression to 'zstd'
	// https://github.com/apache/iceberg/blob/2899b5a75106c698ea8e59fe0b93c4857acaadee/core/src/main/java/org/apache/iceberg/TableProperties.java#L147
	DefaultCompression = parquet.WithCompression(compress.Codecs.Zstd)

	// Apache Iceberg default compression level is "null", and thus doesn't set any compression level config (https://github.com/apache/iceberg/blob/2899b5a75106c698ea8e59fe0b93c4857acaadee/core/src/main/java/org/apache/iceberg/TableProperties.java#L152)
	// thus, falls back to the underlying Parquet-Java library's default which is compression level 3 (https://github.com/apache/parquet-java/blob/dfc025e17e21a326addaf0e43c493e085cbac8f4/parquet-hadoop/src/main/java/org/apache/parquet/hadoop/codec/ZstandardCodec.java#L52)
	DefaultCompressionLevel = parquet.WithCompressionLevel(3)

	// Apache Iceberg: https://github.com/apache/iceberg/blob/2899b5a75106c698ea8e59fe0b93c4857acaadee/core/src/main/java/org/apache/iceberg/TableProperties.java#L130-L133
	DefaultDataPageSize = parquet.WithDataPageSize(1 * 1024 * 1024)

	// Apache Iceberg: https://github.com/apache/iceberg/blob/2899b5a75106c698ea8e59fe0b93c4857acaadee/core/src/main/java/org/apache/iceberg/TableProperties.java#L139-L142
	DefaultDictionaryPageSizeLimit = parquet.WithDictionaryPageSizeLimit(2 * 1024 * 1024)

	// Apache Iceberg: https://github.com/apache/iceberg/blob/2899b5a75106c698ea8e59fe0b93c4857acaadee/parquet/src/main/java/org/apache/iceberg/parquet/Parquet.java#L612
	DefaultDictionaryEncoding = parquet.WithDictionaryDefault(true)

	// Apache Iceberg page row count limit: https://github.com/apache/iceberg/blob/68e555b94f4706a2af41dcb561c84007230c0bc1/core/src/main/java/org/apache/iceberg/TableProperties.java#L137
	DefaultBatchSize = parquet.WithBatchSize(int64(20000))

	// Apache Iceberg relies on Parquet-Java: https://github.com/apache/parquet-java/blob/dfc025e17e21a326addaf0e43c493e085cbac8f4/parquet-column/src/main/java/org/apache/parquet/column/ParquetProperties.java#L67
	DefaultStatsEnabled = parquet.WithStats(true)

	// Apache Iceberg: https://github.com/apache/iceberg/blob/2899b5a75106c698ea8e59fe0b93c4857acaadee/parquet/src/main/java/org/apache/iceberg/parquet/Parquet.java#L171
	DefaultParquetVersion = parquet.WithVersion(parquet.V1_0)

	// iceberg writes root name as "table" in parquet's meta
	DefaultRootName = parquet.WithRootName("table")
)

const (
	dataFileTargetSize = int64(350 * 1024 * 1024)

	// the delete file target size is as per Apache Iceberg
	// https://github.com/apache/iceberg/blob/68e555b94f4706a2af41dcb561c84007230c0bc1/core/src/main/java/org/apache/iceberg/TableProperties.java#L323
	deleteFileTargetSize = int64(64 * 1024 * 1024)

	// Row group size threshold matching Iceberg's default
	// https://github.com/apache/iceberg/blob/68e555b94f4706a2af41dcb561c84007230c0bc1/core/src/main/java/org/apache/iceberg/TableProperties.java#L128
	rowGroupSizeThreshold = int64(128 * 1024 * 1024)
)

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

	FieldId           int
	SchemaId          int
	fileType          string
	ops               IcebergOperations
	IcebergSchemaJSON string

	currentWriter           *pqarrow.FileWriter
	currentBuffer           *bytes.Buffer
	currentFile             string
	currentRowCount         int64
	currentCompressedSize   int64
	currentRowGroupRowCount int64
}

func NewRollingWriter(partitionKey string, fileType string, ops IcebergOperations) *RollingWriter {
	return &RollingWriter{
		partitionKey: partitionKey,
		fileType:     fileType,
		ops:          ops,
	}
}

func (r *RollingWriter) flush() (*FileUploadData, error) {
	if err := r.currentWriter.Close(); err != nil {
		return nil, err
	}

	fileData := make([]byte, r.currentBuffer.Len())
	copy(fileData, r.currentBuffer.Bytes())

	uploadData := &FileUploadData{
		FileType:        r.fileType,
		FileData:        fileData,
		PartitionKey:    r.partitionKey,
		Filename:        r.currentFile,
		EqualityFieldId: r.FieldId,
		RecordCount:     r.currentRowCount,
	}

	r.currentBuffer = nil
	r.currentWriter = nil
	r.currentRowCount = 0
	r.currentFile = ""
	r.currentCompressedSize = 0
	r.currentRowGroupRowCount = 0

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

func (r *RollingWriter) createWriter(ctx context.Context, record arrow.Record) error {
	filename, err := r.ops.GenerateFilename(ctx)
	if err != nil {
		return fmt.Errorf("failed to generate filename: %w", err)
	}

	r.currentFile = filename
	r.currentBuffer = &bytes.Buffer{}

	allocator := memory.NewGoAllocator()

	baseProps := []parquet.WriterProperty{
		DefaultCompression,
		DefaultCompressionLevel,
		DefaultDataPageSize,
		DefaultDictionaryPageSizeLimit,
		DefaultDictionaryEncoding,
		DefaultBatchSize,
		DefaultStatsEnabled,
		DefaultParquetVersion,
		DefaultRootName,
		parquet.WithAllocator(allocator),
	}

	writerProps := parquet.NewWriterProperties(baseProps...)

	arrowProps := pqarrow.NewArrowWriterProperties(
		pqarrow.WithStoreSchema(),
		pqarrow.WithNoMapLogicalType(),
	)

	writer, err := pqarrow.NewFileWriter(record.Schema(), r.currentBuffer, writerProps, arrowProps)
	if err != nil {
		record.Release()

		return err
	}

	if r.fileType == "delete" {
		icebergSchemaJSON := fmt.Sprintf(`{"type":"struct","schema-id":%d,"fields":[{"id":%d,"name":"%s","required":true,"type":"string"}]}`, r.SchemaId, r.FieldId, constants.OlakeID)

		writer.AppendKeyValueMetadata("delete-type", "equality")
		writer.AppendKeyValueMetadata("delete-field-ids", fmt.Sprintf("%d", r.FieldId))
		writer.AppendKeyValueMetadata("iceberg.schema", icebergSchemaJSON)
	} else if r.fileType == "data" && r.IcebergSchemaJSON != "" {
		writer.AppendKeyValueMetadata("iceberg.schema", r.IcebergSchemaJSON)
	}

	r.currentWriter = writer
	r.currentRowCount = 0
	r.currentCompressedSize = 0
	r.currentRowGroupRowCount = 0

	writer.NewBufferedRowGroup()

	if r.fileType == "delete" {
		logger.Infof("Starting delete file: %s with field ID %d", r.currentFile, r.FieldId)
	} else {
		logger.Infof("Starting data file: %s (partition: %s)", r.currentFile, r.partitionKey)
	}

	return nil
}

func (r *RollingWriter) Write(ctx context.Context, record arrow.Record) (*FileUploadData, error) {
	if r.currentWriter == nil {
		r.createWriter(ctx, record)
	}

	if err := r.currentWriter.WriteBuffered(record); err != nil {
		return nil, fmt.Errorf("failed to write buffered record: %w", err)
	}

	r.currentRowCount += record.NumRows()
	r.currentRowGroupRowCount += record.NumRows()
	record.Release()

	rowGroupSize := r.currentWriter.RowGroupTotalBytesWritten()
	if rowGroupSize >= rowGroupSizeThreshold {
		r.currentWriter.NewBufferedRowGroup()
		r.currentRowGroupRowCount = 0
	}

	// Check total file size: all flushed row groups in buffer + current in-progress row group in memory
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
