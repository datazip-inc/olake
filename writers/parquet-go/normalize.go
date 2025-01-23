// TODO: olake should use single parquet writer (parquet-go)
package normalize

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
	"github.com/datazip-inc/olake/utils"
	wp "github.com/datazip-inc/olake/writers/parquet"
	"github.com/parquet-go/parquet-go"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/source"
)

// Parquet destination writes Parquet files to a local path and optionally uploads them to S3.
type Parquet struct {
	options             *protocol.Options
	fileName            string
	destinationFilePath string
	closed              bool
	config              *wp.Config
	file                source.ParquetFile
	writer              *parquet.GenericWriter[types.RawRecord]
	stream              protocol.Stream
	records             atomic.Int64
	pqSchemaMutex       sync.Mutex // To prevent concurrent map access from fraugster library
	s3Client            *s3.S3
	s3KeyPath           string
}

// GetConfigRef returns the config reference for the parquet writer.
func (p *Parquet) GetConfigRef() protocol.Config {
	p.config = &wp.Config{}
	return p.config
}

// Spec returns a new Config instance.
func (p *Parquet) Spec() any {
	return wp.Config{}
}

// setup s3 client if credentials provided
func (p *Parquet) initS3Writer() error {
	if p.config.Bucket == "" || p.config.Region == "" {
		return nil
	}

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

	return nil
}

// Setup configures the parquet writer, including local paths, file names, and optional S3 setup.
func (p *Parquet) Setup(stream protocol.Stream, options *protocol.Options) error {
	p.options = options
	p.fileName = utils.TimestampedFileName(constants.ParquetFileExt)

	// for s3 p.config.path may not be provided
	if p.config.Path == "" {
		p.config.Path = os.TempDir()
	}

	p.destinationFilePath = filepath.Join(p.config.Path, stream.Namespace(), stream.Name(), p.fileName)
	if err := os.MkdirAll(filepath.Dir(p.destinationFilePath), os.ModePerm); err != nil {
		return fmt.Errorf("failed to create directories: %s", err)
	}

	// Initialize local Parquet writer

	pqFile, err := local.NewLocalFileWriter(p.destinationFilePath)
	if err != nil {
		return fmt.Errorf("failed to create parquet file writer: %s", err)
	}
	writer := parquet.NewGenericWriter[types.RawRecord](pqFile)
	p.file = pqFile
	p.writer = writer
	p.stream = stream

	err = p.initS3Writer()
	if err != nil {
		return err
	}
	// Setup S3 client if S3 configuration is provided
	if p.s3Client != nil {
		basePath := filepath.Join(stream.Namespace(), stream.Name())
		if p.config.Prefix != "" {
			basePath = filepath.Join(p.config.Prefix, basePath)
		}
		p.s3KeyPath = filepath.Join(basePath, p.fileName)
	}

	return nil
}

// Write writes a record to the Parquet file.
func (p *Parquet) Write(_ context.Context, record types.RawRecord) error {
	if _, err := p.writer.Write([]types.RawRecord{record}); err != nil {
		return fmt.Errorf("parquet write error: %s", err)
	}
	p.records.Add(1)
	return nil
}

// ReInitiationOnTypeChange always returns true to reinitialize on type change.
func (p *Parquet) ReInitiationOnTypeChange() bool {
	return false
}

// ReInitiationOnNewColumns always returns true to reinitialize on new columns.
func (p *Parquet) ReInitiationOnNewColumns() bool {
	return false
}

// Check validates local paths and S3 credentials if applicable.
func (p *Parquet) Check() error {
	// check for s3 writer configuration
	err := p.initS3Writer()
	if err != nil {
		return err
	}
	// test for s3 permissions
	if p.s3Client != nil {
		testKey := fmt.Sprintf("olake_writer_test/%s", utils.TimestampedFileName(".txt"))
		// Try to upload a small test file
		_, err = p.s3Client.PutObject(&s3.PutObjectInput{
			Bucket: aws.String(p.config.Bucket),
			Key:    aws.String(testKey),
			Body:   strings.NewReader("S3 write test"),
		})
		if err != nil {
			return fmt.Errorf("failed to write test file to S3: %s", err)
		}
		p.config.Path = os.TempDir()
		logger.Info("s3 writer configuration found")
	} else if p.config.Path != "" {
		logger.Info("local writer configuration found, writing at location[%s]", p.config.Path)
	} else {
		return fmt.Errorf("invalid configuration found")
	}

	// Create the directory if it doesn't exist
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
	return nil
}

func (p *Parquet) Close() error {
	if p.closed {
		return nil
	}
	recordCount := p.records.Load()
	removeLocalFile := func(reason string) {
		err := os.Remove(p.destinationFilePath)
		if err != nil {
			logger.Warnf("Failed to delete file [%s] with %d records (%s): %s", p.destinationFilePath, recordCount, reason, err)
			return
		}
		logger.Debugf("Deleted file [%s] with %d records (%s).", p.destinationFilePath, recordCount, reason)
	}

	// Defer closing and possible file removal if no records were written
	defer func() {
		p.closed = true
		if recordCount == 0 {
			removeLocalFile("no records written")
		}
	}()

	// Close the writer and file
	if err := utils.ErrExecSequential(
		utils.ErrExecFormat("failed to close writer: %s", func() error { return p.writer.Close() }),
		utils.ErrExecFormat("failed to close file: %s", p.file.Close),
	); err != nil {
		return fmt.Errorf("failed to close parquet writer after adding %d records: %s", recordCount, err)
	}

	// Log if records were written
	if recordCount > 0 {
		logger.Infof("Finished writing file [%s] with %d records.", p.destinationFilePath, recordCount)
	}

	// Upload to S3 if configured and records exist
	if p.s3Client != nil && recordCount > 0 {
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
			return fmt.Errorf("failed to upload file to S3 (bucket: %s, path: %s): %s", p.config.Bucket, p.s3KeyPath, err)
		}

		// remove local file after upload
		removeLocalFile("uploaded to S3")
		logger.Infof("Successfully uploaded file to S3: s3://%s/%s", p.config.Bucket, p.s3KeyPath)
	}

	return nil
}

// EvolveSchema updates the schema based on changes.
func (p *Parquet) EvolveSchema(_ map[string]*types.Property) error {
	p.pqSchemaMutex.Lock()
	defer p.pqSchemaMutex.Unlock()

	// Attempt to set the schema definition
	// if err := p.writer.SetSchemaDefinition(p.stream.Schema().ToParquet()); err != nil {
	// 	return fmt.Errorf("failed to set schema definition: %s", err)
	// }
	return nil
}

// Type returns the type of the writer.
func (p *Parquet) Type() string {
	return string(types.Parquet)
}

// Flattener returns a flattening function for records.
func (p *Parquet) Flattener() protocol.FlattenFunction {
	return func(record types.Record) (types.Record, error) {
		return nil, nil
	}
}

func init() {
	protocol.RegisteredWriters[types.RawParquet] = func() protocol.Writer {
		return new(Parquet)
	}
}
