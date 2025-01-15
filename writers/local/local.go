package parquet

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

// ParquetWriter destination writes Parquet files to a local path and optionally uploads them to S3.
type ParquetWriter struct {
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

// GetConfigRef returns the config reference for the parquet writer.
func (p *ParquetWriter) GetConfigRef() protocol.Config {
	p.config = &Config{}
	return p.config
}

// Spec returns a new Config instance.
func (p *ParquetWriter) Spec() any {
	return Config{}
}

// Setup configures the parquet writer, including local paths, file names, and optional S3 setup.
func (p *ParquetWriter) Setup(stream protocol.Stream, options *protocol.Options) error {
	p.options = options
	p.fileName = utils.TimestampedFileName(constants.ParquetFileExt)
	p.destinationFilePath = filepath.Join(p.config.Path, stream.Namespace(), stream.Name(), p.fileName)

	// Create directories
	if err := os.MkdirAll(filepath.Dir(p.destinationFilePath), os.ModePerm); err != nil {
		return fmt.Errorf("failed to create directories: %s", err)
	}

	// Initialize local Parquet writer
	pqFile, err := local.NewLocalFileWriter(p.destinationFilePath)
	if err != nil {
		return fmt.Errorf("failed to create parquet file writer: %s", err)
	}
	writer := goparquet.NewFileWriter(pqFile, goparquet.WithSchemaDefinition(stream.Schema().ToParquet()),
		goparquet.WithMaxRowGroupSize(100),
		goparquet.WithMaxPageSize(10),
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
	)

	p.file = pqFile
	p.writer = writer
	p.stream = stream

	// Setup S3 client if S3 configuration is provided
	if p.config.Bucket != "" {
		s3Config := aws.Config{
			Region: aws.String(p.config.Region),
		}
		if p.config.AccessKey != "" && p.config.SecretKey != "" {
			s3Config.Credentials = credentials.NewStaticCredentials(p.config.AccessKey, p.config.SecretKey, "")
		}
		sess, err := session.NewSession(&s3Config)
		if err != nil {
			return fmt.Errorf("failed to create AWS session: %s", err)
		}
		p.s3Client = s3.New(sess)

		basePath := filepath.Join(stream.Namespace(), stream.Name())
		if p.config.Prefix != "" {
			basePath = filepath.Join(p.config.Prefix, basePath)
		}
		p.s3KeyPath = filepath.Join(basePath, p.fileName)
	}

	return nil
}

// Write writes a record to the Parquet file.
func (p *ParquetWriter) Write(_ context.Context, record types.Record) error {
	// Lock for thread safety and write the record
	p.pqSchemaMutex.Lock()
	defer p.pqSchemaMutex.Unlock()

	if err := p.writer.AddData(record); err != nil {
		return fmt.Errorf("parquet write error: %s", err)
	}

	p.records.Add(1)
	return nil
}

// ReInitiationOnTypeChange always returns true to reinitialize on type change.
func (p *ParquetWriter) ReInitiationOnTypeChange() bool {
	return true
}

// ReInitiationOnNewColumns always returns true to reinitialize on new columns.
func (p *ParquetWriter) ReInitiationOnNewColumns() bool {
	return true
}

// Check validates local paths and S3 credentials if applicable.
func (p *ParquetWriter) Check() error {
	// Validate the local path
	if err := os.MkdirAll(p.config.Path, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create path: %s", err)
	}

	// Test directory writability
	tempFile, err := os.CreateTemp(p.config.Path, "temporary-*.txt")
	if err != nil {
		return fmt.Errorf("directory is not writable: %s", err)
	}
	tempFile.Close()
	os.Remove(tempFile.Name())

	// Validate S3 credentials if S3 is configured
	if p.s3Client != nil {
		if _, err := p.s3Client.ListBuckets(&s3.ListBucketsInput{}); err != nil {
			return fmt.Errorf("failed to validate S3 credentials: %s", err)
		}
	}

	return nil
}

func (p *ParquetWriter) Close() error {
	if p.closed {
		return nil
	}
	removeLocalFile := func() {
		err := os.Remove(p.destinationFilePath)
		if err != nil {
			logger.Warnf("failed to delete file[%s] with records %d", p.destinationFilePath, p.records.Load())
			return
		}
		logger.Debugf("Deleted file[%s] with records %d.", p.destinationFilePath, p.records.Load())
	}

	// Defer file deletion if no records were written
	defer func() {
		p.closed = true
		if p.records.Load() == 0 {
			removeLocalFile()
		}
	}()

	// Close the writer and file
	if err := utils.ErrExecSequential(
		utils.ErrExecFormat("failed to close writer: %s", func() error { return p.writer.Close() }),
		utils.ErrExecFormat("failed to close file: %s", p.file.Close),
	); err != nil {
		return fmt.Errorf("failed to close parquet writer: %s", err)
	}

	// Log if records were written
	if p.records.Load() > 0 {
		logger.Infof("Finished writing file [%s] with %d records", p.destinationFilePath, p.records.Load())
	}

	// Upload to S3 if configured and records exist
	if p.s3Client != nil && p.records.Load() > 0 {
		file, err := os.Open(p.destinationFilePath)
		if err != nil {
			return fmt.Errorf("failed to open local file for S3 upload: %s", err)
		}
		defer file.Close()

		_, err = p.s3Client.PutObject(&s3.PutObjectInput{
			Bucket: aws.String(p.config.Bucket),
			Key:    aws.String(p.s3KeyPath),
			Body:   file,
		})
		if err != nil {
			return fmt.Errorf("failed to upload file to S3: %s", err)
		}
		// remove file from local once written to s3
		removeLocalFile()
		logger.Infof("Successfully uploaded file to S3: s3://%s/%s", p.config.Bucket, p.s3KeyPath)
	}
	return nil
}

// EvolveSchema updates the schema based on changes.
func (p *ParquetWriter) EvolveSchema(_ map[string]*types.Property) error {
	p.pqSchemaMutex.Lock()
	defer p.pqSchemaMutex.Unlock()

	// Attempt to set the schema definition
	if err := p.writer.SetSchemaDefinition(p.stream.Schema().ToParquet()); err != nil {
		return fmt.Errorf("failed to set schema definition: %s", err)
	}
	return nil
}

// Type returns the type of the writer.
func (p *ParquetWriter) Type() string {
	return string(types.Parquet)
}

// Flattener returns a flattening function for records.
func (p *ParquetWriter) Flattener() protocol.FlattenFunction {
	flattener := typeutils.NewFlattener()
	return flattener.Flatten
}

func init() {
	protocol.RegisteredWriters[types.Parquet] = func() protocol.Writer {
		return new(ParquetWriter)
	}
}
