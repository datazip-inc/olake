package iceberg

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/google/uuid"
)

var totalDataFiles atomic.Int64

const parquetSizeThresholdBytes = int64(27*1024*1024 - 1*1024*1024)
const streamChunkSize = int64(8 * 1024 * 1024)

type ArrowWriter struct {
	s3Client   *s3.Client
	bucketName string
	prefix     string

	currentWriter         *pqarrow.FileWriter
	currentBuffer         *bytes.Buffer
	currentRowCount       int64
	currentFile           string
	currentCompressedSize int64
}

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func GenerateDataFileName() string {
	// It mimics the behavior in the Java API:
	// https://github.com/apache/iceberg/blob/a582968975dd30ff4917fbbe999f1be903efac02/core/src/main/java/org/apache/iceberg/io/OutputFileFactory.java#L92-L101
	return fmt.Sprintf("00000-%d-%s.parquet", totalDataFiles.Load(), uuid.New())
}

func NewArrowWriter(bucketName, prefix, accessKey, secretKey, region string) (*ArrowWriter, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")), config.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	arrowWriter := &ArrowWriter{
		s3Client:   s3.NewFromConfig(cfg),
		bucketName: bucketName,
		prefix:     prefix,
	}

	return arrowWriter, nil
}

func (i *Iceberg) ArrowWrites(ctx context.Context, records []types.RawRecord) error {
	arrowRec, err := i.createArrowRecord(records)
	if err != nil {
		return fmt.Errorf("failed to create arrow record: %w", err)
	}
	defer arrowRec.Release()

	filePath, err := i.arrowToParquet(arrowRec)
	if err != nil {
		return fmt.Errorf("failed to write arrow record to parquet: %w", err)
	}

	if filePath != "" {
		if i.createdFilePaths == nil {
			i.createdFilePaths = make([]string, 0)
		}
		i.createdFilePaths = append(i.createdFilePaths, filePath)
	}

	return nil
}

func (i *Iceberg) createNormFields() []arrow.Field {
	fieldNames := make([]string, 0, len(i.schema)+1)
	for fieldName := range i.schema {
		fieldNames = append(fieldNames, fieldName)
	}

	sort.Strings(fieldNames)

	fields := make([]arrow.Field, 0, len(fieldNames))
	for _, fieldName := range fieldNames {
		var arrowType arrow.DataType

		icebergType := i.schema[fieldName]
		switch icebergType {
		case "boolean":
			arrowType = arrow.FixedWidthTypes.Boolean
		case "int":
			arrowType = arrow.PrimitiveTypes.Int32
		case "long":
			arrowType = arrow.PrimitiveTypes.Int64
		case "float":
			arrowType = arrow.PrimitiveTypes.Float32
		case "double":
			arrowType = arrow.PrimitiveTypes.Float64
		case "timestamptz":
			arrowType = arrow.FixedWidthTypes.Timestamp_us
		default:
			arrowType = arrow.BinaryTypes.String
		}

		fields = append(fields, arrow.Field{
			Name:     fieldName,
			Type:     arrowType,
			Nullable: true,
		})
	}

	return fields
}

func (i *Iceberg) createDeNormFields() []arrow.Field {
	fields := make([]arrow.Field, 0, 5)

	fields = append(fields, arrow.Field{Name: "_olake_id", Type: arrow.BinaryTypes.String, Nullable: false})
	fields = append(fields, arrow.Field{Name: "_olake_timestamp", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: false})
	fields = append(fields, arrow.Field{Name: "_op_type", Type: arrow.BinaryTypes.String, Nullable: false})
	fields = append(fields, arrow.Field{Name: "_cdc_timestamp", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true})
	fields = append(fields, arrow.Field{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true})

	return fields
}

func (i *Iceberg) createArrowRecord(records []types.RawRecord) (arrow.Record, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("no records provided")
	}

	allocator := memory.NewGoAllocator()
	var fields []arrow.Field

	if i.stream.NormalizationEnabled() {
		fields = i.createNormFields()
	} else {
		fields = i.createDeNormFields()
	}

	arrowSchema := arrow.NewSchema(fields, nil)
	recordBuilder := array.NewRecordBuilder(allocator, arrowSchema)
	defer recordBuilder.Release()

	for _, record := range records {
		for idx, field := range arrowSchema.Fields() {
			var val any
			switch field.Name {
			case "_olake_id":
				val = record.OlakeID
			case "_olake_timestamp":
				val = record.OlakeTimestamp
			case "_op_type":
				val = record.CdcTimestamp
			case "_cdc_timestamp":
				val = record.CdcTimestamp
			default:
				if i.stream.NormalizationEnabled() {
					val = record.Data[field.Name]
				} else {
					val = record.Data
				}
			}
			
			if val == nil {
				recordBuilder.Field(idx).AppendNull()
			} else {
				if err := i.appendValueToBuilder(recordBuilder.Field(idx), val, field.Type); err != nil {
					logger.Warnf("failed to append value for field %s: %v", field.Name, err)
					recordBuilder.Field(idx).AppendNull()
				}
			}
		}
	}

	arrowRecord := recordBuilder.NewRecord()

	return arrowRecord, nil
}

func (i *Iceberg) appendValueToBuilder(builder array.Builder, val interface{}, fieldType arrow.DataType) error {
	switch builder := builder.(type) {
	case *array.BooleanBuilder:
		if boolVal, err := typeutils.ReformatBool(val); err == nil {
			builder.Append(boolVal)
		} else {
			return err
		}
	case *array.Int32Builder:
		if intVal, err := typeutils.ReformatInt32(val); err == nil {
			builder.Append(intVal)
		} else {
			return err
		}
	case *array.Int64Builder:
		if longVal, err := typeutils.ReformatInt64(val); err == nil {
			builder.Append(longVal)
		} else {
			return err
		}
	case *array.Float32Builder:
		if floatVal, err := typeutils.ReformatFloat32(val); err == nil {
			builder.Append(floatVal)
		} else {
			return err
		}
	case *array.Float64Builder:
		if doubleVal, err := typeutils.ReformatFloat64(val); err == nil {
			builder.Append(doubleVal)
		} else {
			return err
		}
	case *array.TimestampBuilder:
		if timeVal, err := typeutils.ReformatDate(val); err == nil {
			ts := arrow.Timestamp(timeVal.UnixMicro())
			builder.Append(ts)
		} else {
			return err
		}
	case *array.StringBuilder:
		builder.Append(fmt.Sprintf("%v", val))
	default:
		return fmt.Errorf("unsupported builder type: %T", builder)
	}
	return nil
}

func (i *Iceberg) arrowToParquet(rec arrow.Record) (string, error) {
	if i.batchedWriter == nil {
		bucketName, prefix, err := i.parseS3Path()
		if err != nil {
			return "", fmt.Errorf("failed to parse S3 path: %w", err)
		}

		writer, err := NewArrowWriter(bucketName, prefix, i.config.AccessKey, i.config.SecretKey, i.config.Region)
		if err != nil {
			return "", err
		}

		i.batchedWriter = writer
	}

	filePath, err := i.batchedWriter.(*ArrowWriter).write(rec)
	if err != nil {
		return "", fmt.Errorf("failed to write record: %w", err)
	}
	return filePath, nil
}

func (aw *ArrowWriter) Close() error {
	if aw.currentWriter != nil {
		if _, err := aw.flush(); err != nil {
			return fmt.Errorf("failed to finalize pending batch: %w", err)
		}
	}
	runtime.GC()

	return nil
}

func (aw *ArrowWriter) closeAndGetFinalPath() (string, error) {
	var finalPath string
	if aw.currentWriter != nil {
		path, err := aw.flush()
		if err != nil {
			return "", fmt.Errorf("failed to finalize pending batch: %w", err)
		}
		finalPath = path
	}

	runtime.GC()

	return finalPath, nil
}

func (aw *ArrowWriter) write(rec arrow.Record) (string, error) {
	if aw.currentWriter == nil {
		fileName := fmt.Sprintf("olake_%s", time.Now().Format("2006-01-02_15-04-05"))
		aw.currentFile = fileName
		allocator := memory.NewGoAllocator()
		aw.currentBuffer = &bytes.Buffer{}

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

		writer, err := pqarrow.NewFileWriter(rec.Schema(), aw.currentBuffer, writerProps, arrowProps)
		if err != nil {
			return "", fmt.Errorf("failed to create file writer: %w", err)
		}

		aw.currentWriter = writer
		aw.currentRowCount = 0
		aw.currentCompressedSize = 0
		logger.Infof("Starting file: %s", fileName)
	}

	if err := aw.currentWriter.WriteBuffered(rec); err != nil {
		return "", fmt.Errorf("failed to write buffered record: %w", err)
	}

	rec.Release()
	aw.currentRowCount += rec.NumRows()

	sizeSoFar := int64(0)
	if aw.currentBuffer != nil {
		sizeSoFar += int64(aw.currentBuffer.Len())
	}
	if aw.currentWriter != nil {
		sizeSoFar += aw.currentWriter.RowGroupTotalCompressedBytes()
	}
	aw.currentCompressedSize = sizeSoFar
	if aw.currentCompressedSize >= parquetSizeThresholdBytes {
		return aw.flush()
	}

	return "", nil
}

func (aw *ArrowWriter) flush() (string, error) {
	if err := aw.currentWriter.Close(); err != nil {
		return "", fmt.Errorf("failed to close writer: %w", err)
	}

	key := GenerateDataFileName()
	totalDataFiles.Add(1)

	_, err := aw.s3Client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(aw.bucketName),
		Key:    aws.String(key),
		Body:   bytes.NewReader(aw.currentBuffer.Bytes()),
	})

	if err != nil {
		return "", fmt.Errorf("failed to upload to S3: %w", err)
	}

	fileSize := int64(aw.currentBuffer.Len())

	s3Path := fmt.Sprintf("s3://%s/%s", aw.bucketName, key)
	logger.Infof("Successfully uploaded file: %s (%.2f MB, %d records)", s3Path, float64(fileSize)/1024/1024, aw.currentRowCount)

	if aw.currentBuffer != nil {
		aw.currentBuffer.Reset()
		aw.currentBuffer = nil
	}

	aw.currentWriter = nil
	aw.currentRowCount = 0
	aw.currentFile = ""
	aw.currentCompressedSize = 0
	runtime.GC()

	return s3Path, nil
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
