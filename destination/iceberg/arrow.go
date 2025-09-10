package iceberg

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
)

// Pending:
// - cpu level optimizations
// - normalization : true, false
// - where the arrow enable option will be?

func (i *Iceberg) ArrowWrites(ctx context.Context, record []types.RawRecord) error {
	logger.Infof(">>>>>>>>> Apache Arrow for faster writes")
	arrowRecs, err := i.createArrowRecordBatch(record)
	if err != nil {
		return err
	}
	filePath, err := i.WriteArrowRecordsToParquet(ctx, arrowRecs, "zstd")
	if err != nil {
		return err
	}

	// Store the created parquet file path
	if i.createdFilePaths == nil {
		i.createdFilePaths = make([]string, 0)
	}
	i.createdFilePaths = append(i.createdFilePaths, filePath)

	return nil
}

func (i *Iceberg) createArrowRecordBatch(records []types.RawRecord) (arrow.Record, error) {
	allocator := memory.NewGoAllocator()

	fieldNames := make([]string, 0, len(i.schema))
	for fieldName := range i.schema {
		fieldNames = append(fieldNames, fieldName)
	}
	// if i.stream.NormalizationEnabled() {
	// 	for fieldName, _ := range records[0].Data {
	// 		fieldNames = append(fieldNames, fieldName)
	// 	}
	// }
	sort.Strings(fieldNames)

	fields := make([]arrow.Field, 0, len(fieldNames))
	for _, fieldName := range fieldNames {
		icebergType := i.schema[fieldName]
		var arrowType arrow.DataType

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

	arrowSchema := arrow.NewSchema(fields, nil)
	fmt.Println(arrowSchema.String())
	recordBuilder := array.NewRecordBuilder(allocator, arrowSchema)
	defer recordBuilder.Release()

	if i.stream.NormalizationEnabled() {
		for _, record := range records {
			for i, field := range arrowSchema.Fields() {
				fmt.Println("field name:", field.Name)
				for k, v := range record.Data {
					fmt.Println(k, v)
				}
				val, exists := record.Data[field.Name]
				if !exists || val == nil {
					recordBuilder.Field(i).AppendNull()
					continue
				}
				fmt.Println("Value: ", val)
				switch builder := recordBuilder.Field(i).(type) {
				case *array.BooleanBuilder:
					if boolVal, err := typeutils.ReformatBool(val); err == nil {
						builder.Append(boolVal)
					} else {
						builder.AppendNull()
					}
				case *array.Int32Builder:
					if intVal, err := typeutils.ReformatInt32(val); err == nil {
						builder.Append(intVal)
					} else {
						builder.AppendNull()
					}
				case *array.Int64Builder:
					if longVal, err := typeutils.ReformatInt64(val); err == nil {
						builder.Append(longVal)
					} else {
						builder.AppendNull()
					}
				case *array.Float32Builder:
					if floatVal, err := typeutils.ReformatFloat32(val); err == nil {
						builder.Append(floatVal)
					} else {
						builder.AppendNull()
					}
				case *array.Float64Builder:
					if doubleVal, err := typeutils.ReformatFloat64(val); err == nil {
						builder.Append(doubleVal)
					} else {
						builder.AppendNull()
					}
				case *array.TimestampBuilder:
					if timeVal, err := typeutils.ReformatDate(val); err == nil {
						ts := arrow.Timestamp(timeVal.UnixMicro())
						builder.Append(ts)
					} else {
						builder.AppendNull()
					}
				case *array.StringBuilder:
					builder.Append(fmt.Sprintf("%v", val))
				default:
					if sb, ok := recordBuilder.Field(i).(*array.StringBuilder); ok {
						sb.Append(fmt.Sprintf("%v", val))
					} else {
						recordBuilder.Field(i).AppendNull()
					}
				}
			}
		}
	} else {
		// for _, record := records {

		// }
	}

	arrowRecord := recordBuilder.NewRecord()
	fmt.Println(arrowRecord.Schema())

	return arrowRecord, nil
}

func (i *Iceberg) createS3Client() (*s3.S3, error) {
	s3Config := aws.Config{
		Region: aws.String(i.config.Region),
	}

	if i.config.S3Endpoint != "" {
		s3Config.Endpoint = aws.String(i.config.S3Endpoint)
		s3Config.S3ForcePathStyle = aws.Bool(i.config.S3PathStyle)
		if !i.config.S3UseSSL {
			s3Config.DisableSSL = aws.Bool(true)
		}
	}

	if i.config.AccessKey != "" && i.config.SecretKey != "" {
		creds := credentials.NewStaticCredentials(i.config.AccessKey, i.config.SecretKey, i.config.SessionToken)
		s3Config.Credentials = creds
	} else if i.config.ProfileName != "" {
		creds := credentials.NewSharedCredentials("", i.config.ProfileName)
		s3Config.Credentials = creds
	}

	sess, err := session.NewSession(&s3Config)
	if err != nil {
		return nil, fmt.Errorf("failed to create AWS session: %w", err)
	}

	return s3.New(sess), nil
}

func (i *Iceberg) WriteArrowRecordsToParquet(ctx context.Context, record arrow.Record, compression string) (string, error) {
	var writerProps *parquet.WriterProperties
	switch strings.ToLower(compression) {
	case "uncompressed":
		writerProps = parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Uncompressed))
	case "snappy":
		writerProps = parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))
	case "gzip":
		writerProps = parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Gzip))
	case "zstd":
		writerProps = parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Zstd))
	default:
		writerProps = parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Zstd))
	}

	arrowWriterProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())
	fileName := utils.TimestampedFileName(constants.ParquetFileExt)

	filePath, err := i.writeBatchToS3(ctx, record, fileName, writerProps, arrowWriterProps)
	if err != nil {
		return "", err
	}
	return filePath, nil
}

func (i *Iceberg) writeBatchToS3(ctx context.Context, record arrow.Record, fileName string, writerProps *parquet.WriterProperties, arrowWriterProps pqarrow.ArrowWriterProperties) (string, error) {
	s3Path := strings.TrimPrefix(i.config.IcebergS3Path, "s3://")
	pathParts := strings.SplitN(s3Path, "/", 2)
	if len(pathParts) < 1 {
		return "", fmt.Errorf("invalid S3 path format: %s", i.config.IcebergS3Path)
	}

	bucket := pathParts[0]
	var prefix string
	if len(pathParts) > 1 {
		prefix = pathParts[1]
	}

	if i.stream != nil {
		streamPath := filepath.Join(i.stream.GetDestinationDatabase(&i.config.IcebergDatabase), i.stream.GetDestinationTable())
		if prefix != "" {
			prefix = filepath.Join(prefix, streamPath)
		} else {
			prefix = streamPath
		}
	}

	s3Key := filepath.Join(prefix, fileName)
	s3Client, err := i.createS3Client()
	if err != nil {
		return "", fmt.Errorf("failed to create S3 client: %w", err)
	}

	var buffer bytes.Buffer
	if err := i.writeArrowToParquet(&buffer, record, writerProps, arrowWriterProps); err != nil {
		return "", fmt.Errorf("failed to write Arrow records to parquet buffer: %w", err)
	}

	_, err = s3Client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(s3Key),
		Body:   bytes.NewReader(buffer.Bytes()),
	})
	if err != nil {
		return "", fmt.Errorf("failed to upload parquet file to S3: %w", err)
	}

	fullS3Path := fmt.Sprintf("s3://%s/%s", bucket, s3Key)
	logger.Infof("Thread[%s]: Successfully wrote Arrow record directly to S3 parquet file: %s", i.options.ThreadID, fullS3Path)
	return fullS3Path, nil
}

func (i *Iceberg) writeArrowToParquet(writer io.Writer, record arrow.Record, writerProps *parquet.WriterProperties, arrowWriterProps pqarrow.ArrowWriterProperties) error {
	pqWriter, err := pqarrow.NewFileWriter(record.Schema(), writer, writerProps, arrowWriterProps)
	if err != nil {
		return fmt.Errorf("error creating pqarrow file writer: %w", err)
	}
	defer pqWriter.Close()

	if err := pqWriter.Write(record); err != nil {
		return fmt.Errorf("failed to write Arrow record: %w", err)
	}

	return nil
}

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}
