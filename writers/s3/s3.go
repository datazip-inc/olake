package s3

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/typeutils"
	"github.com/datazip-inc/olake/utils"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/piyushsingariya/relec/memory"

	"github.com/aws/aws-sdk-go/aws/credentials"
	goparquet "github.com/fraugster/parquet-go"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/source"
)

// S3 destination writes Parquet files to S3
type S3 struct {
	options             *protocol.Options
	fileName            string
	destinationFilePath string
	tempFilePath        string
	closed              bool
	config              *Config
	file                source.ParquetFile
	writer              *goparquet.FileWriter
	stream              protocol.Stream
	records             atomic.Int64
	pqSchemaMutex       sync.Mutex
	s3Client            *s3.S3
}

func (s *S3) GetConfigRef() protocol.Config {
	s.config = &Config{}
	return s.config
}

func (s *S3) Spec() any {
	return Config{}
}

func (s *S3) Setup(stream protocol.Stream, options *protocol.Options) error {
	s.options = options
	s.fileName = utils.TimestampedFileName(constants.ParquetFileExt)
	s.destinationFilePath = filepath.Join(stream.Namespace(), stream.Name(), s.fileName)
	s.tempFilePath = filepath.Join(os.TempDir(), s.fileName)

	// Create AWS session with optional credentials
	config := aws.Config{
		Region: aws.String(s.config.Region),
	}

	if s.config.AccessKey != "" && s.config.SecretKey != "" {
		config.Credentials = credentials.NewStaticCredentials(s.config.AccessKey, s.config.SecretKey, "")
	}

	sess, err := session.NewSession(&config)
	if err != nil {
		return fmt.Errorf("failed to create AWS session: %s", err)
	}
	s.s3Client = s3.New(sess)

	// Create temp directory for local file
	err = os.MkdirAll(filepath.Dir(s.tempFilePath), os.ModePerm)
	if err != nil {
		return err
	}

	pqFile, err := local.NewLocalFileWriter(s.tempFilePath)
	if err != nil {
		return err
	}

	writer := goparquet.NewFileWriter(pqFile, goparquet.WithSchemaDefinition(stream.Schema().ToParquet()),
		goparquet.WithMaxRowGroupSize(100),
		goparquet.WithMaxPageSize(10),
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
	)

	s.file = pqFile
	s.writer = writer
	s.stream = stream
	return nil
}

func (s *S3) Check() error {
	// Create AWS session with optional credentials
	config := aws.Config{
		Region: aws.String(s.config.Region),
	}

	if s.config.AccessKey != "" && s.config.SecretKey != "" {
		config.Credentials = credentials.NewStaticCredentials(s.config.AccessKey, s.config.SecretKey, "")
	}

	sess, err := session.NewSession(&config)
	if err != nil {
		return fmt.Errorf("failed to create AWS session: %s", err)
	}
	s3Client := s3.New(sess)

	// Create a test key with a random name
	testKey := fmt.Sprintf("olake_test/%s", utils.TimestampedFileName(".txt"))

	// Try to upload a small test file
	_, err = s3Client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(testKey),
		Body:   strings.NewReader("S3 write test"),
	})
	if err != nil {
		return fmt.Errorf("failed to write test file to S3: %s", err)
	}

	logger.Infof("Successfully wrote test file to s3://%s/%s", s.config.Bucket, testKey)

	// Try to delete the test file to verify delete permissions
	_, err = s3Client.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(testKey),
	})
	if err != nil {
		return fmt.Errorf("failed to delete test file from S3: %s", err)
	}

	logger.Infof("Successfully deleted test file from s3://%s/%s", s.config.Bucket, testKey)

	// Wait for the delete to complete
	err = s3Client.WaitUntilObjectNotExists(&s3.HeadObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(testKey),
	})
	if err != nil {
		return fmt.Errorf("failed to confirm test file deletion: %s", err)
	}

	return nil
}

func (s *S3) Write(ctx context.Context, channel <-chan types.Record) error {
iteration:
	for !s.closed {
		select {
		case <-ctx.Done():
			break iteration
		default:
			record, ok := <-channel
			if !ok {
				break iteration
			}

			var err error
			memory.LockWithTrigger(ctx, func() {
				err = s.writer.FlushRowGroupWithContext(ctx)
			})
			if err != nil {
				return err
			}

			s.pqSchemaMutex.Lock()
			if err := s.writer.AddData(record); err != nil {
				s.pqSchemaMutex.Unlock()
				return fmt.Errorf("parquet write error: %s", err)
			}
			s.pqSchemaMutex.Unlock()

			s.records.Add(1)
		}
	}

	return nil
}

func (s *S3) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true

	defer func() {
		// Clean up temp file
		os.Remove(s.tempFilePath)

		if s.records.Load() == 0 {
			logger.Debugf("No records written, skipping S3 upload")
			return
		}
	}()

	err := utils.ErrExecSequential(
		utils.ErrExecFormat("failed to close writer: %s", func() error { return s.writer.Close() }),
		utils.ErrExecFormat("failed to close file: %s", s.file.Close),
	)
	if err != nil && s.records.Load() > 0 {
		return fmt.Errorf("failed to stop writer after adding %d records: %s", s.records.Load(), err)
	}
	// Upload file to S3
	if s.records.Load() > 0 {
		file, err := os.Open(s.tempFilePath)
		if err != nil {
			return fmt.Errorf("failed to open temp file for upload: %s", err)
		}
		defer file.Close()

		_, err = s.s3Client.PutObject(&s3.PutObjectInput{
			Bucket: aws.String(s.config.Bucket),
			Key:    aws.String(s.destinationFilePath),
			Body:   file,
		})
		if err != nil {
			return fmt.Errorf("failed to upload to S3: %s", err)
		}

		logger.Infof("Successfully uploaded file to s3://%s/%s with %d records",
			s.config.Bucket, s.destinationFilePath, s.records.Load())
	}
	logger.Infof("Deleting temp file %s", s.tempFilePath)

	return nil
}

func (s *S3) ReInitiationOnTypeChange() bool {
	return false
}

func (s *S3) ReInitiationOnNewColumns() bool {
	return false
}

func (s *S3) EvolveSchema(mutation map[string]*types.Property) error {
	s.pqSchemaMutex.Lock()
	defer s.pqSchemaMutex.Unlock()

	s.writer.SetSchemaDefinition(s.stream.Schema().ToParquet())
	return nil
}

func (s *S3) Type() string {
	return string(types.S3)
}

func (s *S3) Flattener() protocol.FlattenFunction {
	flattener := typeutils.NewFlattener()
	return flattener.Flatten
}

func init() {
	protocol.RegisteredWriters[types.S3] = func() protocol.Writer {
		return new(S3)
	}
}
