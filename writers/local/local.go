// local_writer.go
package local

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/typeutils"
	"github.com/datazip-inc/olake/utils"
	"github.com/fraugster/parquet-go/parquet"

	goparquet "github.com/fraugster/parquet-go"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/source"
)

// Local destination writes Parquet files to a local path and optionally uploads them to S3.
type Local struct {
	options             *protocol.Options
	fileName            string
	destinationFilePath string
	closed              bool
	config              *Config
	file                source.ParquetFile
	writer              *goparquet.FileWriter
	stream              protocol.Stream
	records             atomic.Int64
	pqSchemaMutex       sync.Mutex // To prevent concurrent map access from fraugster library
	s3Client            *s3.S3
	s3KeyPath           string
}

// GetConfigRef returns the config reference for the local writer.
func (l *Local) GetConfigRef() protocol.Config {
	l.config = &Config{}
	return l.config
}

// Spec returns a new Config instance.
func (l *Local) Spec() any {
	return Config{}
}

// Setup configures the local writer, including paths, file names, and optional S3 setup.
func (l *Local) Setup(stream protocol.Stream, options *protocol.Options) error {
	l.options = options
	l.fileName = utils.TimestampedFileName(constants.ParquetFileExt)
	l.destinationFilePath = filepath.Join(l.config.Path, stream.Namespace(), stream.Name(), l.fileName)

	// Create directories
	if err := os.MkdirAll(filepath.Dir(l.destinationFilePath), os.ModePerm); err != nil {
		return fmt.Errorf("failed to create directories: %w", err)
	}

	// Initialize local Parquet writer
	pqFile, err := local.NewLocalFileWriter(l.destinationFilePath)
	if err != nil {
		return fmt.Errorf("failed to create local file writer: %w", err)
	}
	writer := goparquet.NewFileWriter(pqFile, goparquet.WithSchemaDefinition(stream.Schema().ToParquet()),
		goparquet.WithMaxRowGroupSize(100),
		goparquet.WithMaxPageSize(10),
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
	)

	l.file = pqFile
	l.writer = writer
	l.stream = stream

	// Setup S3 client if S3 configuration is provided
	if l.config.Bucket != "" {
		s3Config := aws.Config{
			Region: aws.String(l.config.Region),
		}
		if l.config.AccessKey != "" && l.config.SecretKey != "" {
			s3Config.Credentials = credentials.NewStaticCredentials(l.config.AccessKey, l.config.SecretKey, "")
		}
		sess, err := session.NewSession(&s3Config)
		if err != nil {
			return fmt.Errorf("failed to create AWS session: %w", err)
		}
		l.s3Client = s3.New(sess)

		basePath := filepath.Join(stream.Namespace(), stream.Name())
		if l.config.Prefix != "" {
			basePath = filepath.Join(l.config.Prefix, basePath)
		}
		l.s3KeyPath = filepath.Join(basePath, l.fileName)
	}

	return nil
}

// Write writes a record to the Parquet file.
func (l *Local) Write(ctx context.Context, record types.Record) error {
	// Lock for thread safety and write the record
	l.pqSchemaMutex.Lock()
	defer l.pqSchemaMutex.Unlock()

	if err := l.writer.AddData(record); err != nil {
		return fmt.Errorf("parquet write error: %w", err)
	}

	l.records.Add(1)
	return nil
}

// ReInitiationOnTypeChange always returns true to reinitialize on type change.
func (l *Local) ReInitiationOnTypeChange() bool {
	return true
}

// ReInitiationOnNewColumns always returns true to reinitialize on new columns.
func (l *Local) ReInitiationOnNewColumns() bool {
	return true
}

// Check validates local paths and S3 credentials if applicable.
func (l *Local) Check() error {
	// Validate the local path
	if err := os.MkdirAll(l.config.Path, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create path: %w", err)
	}

	// Test directory writability
	tempFile, err := os.CreateTemp(l.config.Path, "temporary-*.txt")
	if err != nil {
		return fmt.Errorf("directory is not writable: %w", err)
	}
	tempFile.Close()
	os.Remove(tempFile.Name())

	// Validate S3 credentials if S3 is configured
	if l.config.Bucket != "" && l.s3Client != nil {
		if _, err := l.s3Client.ListBuckets(&s3.ListBucketsInput{}); err != nil {
			return fmt.Errorf("failed to validate S3 credentials: %w", err)
		}
	}

	return nil
}
func (l *Local) Close() error {
	if l.closed {
		return nil
	}
	l.closed = true

	// Close the writer and file
	if err := utils.ErrExecSequential(
		utils.ErrExecFormat("failed to close writer: %s", func() error { return l.writer.Close() }),
		utils.ErrExecFormat("failed to close file: %s", l.file.Close),
	); err != nil {
		return fmt.Errorf("failed to close local writer: %w", err)
	}

	// Upload to S3 if configured and records exist
	if l.s3Client != nil && l.records.Load() > 0 {
		file, err := os.Open(l.destinationFilePath)
		if err != nil {
			return fmt.Errorf("failed to open local file for S3 upload: %w", err)
		}
		defer file.Close()

		if _, err = l.s3Client.PutObject(&s3.PutObjectInput{
			Bucket: aws.String(l.config.Bucket),
			Key:    aws.String(l.s3KeyPath),
			Body:   file,
		}); err != nil {
			return fmt.Errorf("failed to upload file to S3: %w", err)
		}

		logger.Infof("Successfully uploaded file to S3: s3://%s/%s", l.config.Bucket, l.s3KeyPath)
	}

	return nil
}

// EvolveSchema updates the schema based on changes.
func (l *Local) EvolveSchema(mutation map[string]*types.Property) error {
	l.pqSchemaMutex.Lock()
	defer l.pqSchemaMutex.Unlock()

	l.writer.SetSchemaDefinition(l.stream.Schema().ToParquet())
	return nil
}

// Type returns the type of the writer.
func (l *Local) Type() string {
	return string(types.Local)
}

// Flattener returns a flattening function for records.
func (l *Local) Flattener() protocol.FlattenFunction {
	flattener := typeutils.NewFlattener()
	return flattener.Flatten
}

func init() {
	protocol.RegisteredWriters[types.Local] = func() protocol.Writer {
		return new(Local)
	}
}
