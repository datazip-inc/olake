package parquet

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

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
	pqgo "github.com/parquet-go/parquet-go"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/source"
)

type FileMetadata struct {
	fileName    string
	recordCount int
	writer      any
	parquetFile source.ParquetFile
	currentSize int64
}

type Parquet struct {
	options          *protocol.Options
	config           *Config
	stream           protocol.Stream
	basePath         string
	partitionedFiles map[string][]FileMetadata
	s3Client         *s3.S3
}

func (p *Parquet) GetConfigRef() protocol.Config {
	p.config = &Config{}
	return p.config
}
func (p *Parquet) Spec() any {
	return Config{}
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

func (p *Parquet) createNewPartitionFile(basePath string) error {
	// Get target and minimum file sizes
	targetSize := int64(256 * 1024 * 1024) // Default: 256MB
	if p.config.FileSizeMB > 0 {
		targetSize = int64(p.config.FileSizeMB) * 1024 * 1024
	}
	minSize := int64(128 * 1024 * 1024) // Default: 128MB
	if p.config.MinFileSizeMB > 0 {
		minSize = int64(p.config.MinFileSizeMB) * 1024 * 1024
	}
	// construct directory path
	directoryPath := filepath.Join(p.config.Path, basePath)

	if err := os.MkdirAll(directoryPath, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create directories[%s]: %s", directoryPath, err)
	}

	fileName := utils.TimestampedFileName(constants.ParquetFileExt)
	filePath := filepath.Join(directoryPath, fileName)

	pqFile, err := local.NewLocalFileWriter(filePath)
	if err != nil {
		return fmt.Errorf("failed to create parquet file writer: %s", err)
	}

	writer := func() any {
		if p.stream.NormalizationEnabled() {
			return pqgo.NewGenericWriter[any](pqFile, p.stream.Schema().ToParquet(), pqgo.Compression(&pqgo.Snappy))
		}
		return pqgo.NewGenericWriter[types.RawRecord](pqFile, pqgo.Compression(&pqgo.Snappy))
	}()

	p.partitionedFiles[basePath] = append(p.partitionedFiles[basePath], FileMetadata{
		fileName:    fileName,
		recordCount: 0,
		writer:      writer,
		parquetFile: pqFile,
		currentSize: 0, // Start with zero size
	})

	return nil
}

// Setup configures the parquet writer, including local paths, file names, and optional S3 setup.
func (p *Parquet) Setup(stream protocol.Stream, options *protocol.Options) error {
	p.options = options
	p.stream = stream
	p.basePath = utils.StreamPath(stream)
	p.partitionedFiles = make(map[string][]FileMetadata)

	if err := p.initS3Writer(); err != nil {
		return fmt.Errorf("failed to setup S3 writer: %s", err)
	}

	return nil
}

// Write writes a record to the Parquet file.
func (p *Parquet) Write(_ context.Context, record types.RawRecord) error {
	// Get partition path
	path := p.getPartitionedFilePath(record.Data, time.Now())
	
	// Get or create partition file
	files := p.partitionedFiles[path]
	if len(files) == 0 {
		if err := p.createNewPartitionFile(path); err != nil {
			return err
		}
		files = p.partitionedFiles[path]
	}
	
	// Get current file metadata
	fileMeta := files[len(files)-1]
	
	// Check if we need to create a new file based on size or row count
	if fileMeta.currentSize >= p.config.FileSizeMB*1024*1024 || fileMeta.recordCount >= p.config.MaxRows {
		// Create new file if current file is too large or has too many rows
		if err := p.createNewPartitionFile(path); err != nil {
			return err
		}
		files = p.partitionedFiles[path]
		fileMeta = files[len(files)-1]
	}
	
	// Write record
	writer := fileMeta.writer
	if err := writer.Write(record); err != nil {
		return fmt.Errorf("failed to write record: %s", err)
	}
	
	// Update file metadata
	fileMeta.recordCount++
	fileMeta.currentSize += p.estimateRecordSize(record)
	files[len(files)-1] = fileMeta

	return nil
}

// Check validates local paths and S3 credentials if applicable.
func (p *Parquet) Check() error {
	// Check if local path exists
	if p.config.Path == "" {
		return fmt.Errorf("local path is required")
	}

	if err := os.MkdirAll(p.config.Path, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create local path: %s", err)
	}

	// Check S3 credentials if provided
	if p.s3Client != nil {
		_, err := p.s3Client.ListBuckets(&s3.ListBucketsInput{})
		if err != nil {
			return fmt.Errorf("failed to validate S3 credentials: %s", err)
		}
	}

	return nil
}

// Close closes all parquet files and uploads them to S3 if configured.
func (p *Parquet) Close() error {
	// Close all files
	for _, files := range p.partitionedFiles {
		for _, fileMeta := range files {
			// Close writer
			if err := fileMeta.writer.(*pqgo.GenericWriter).Close(); err != nil {
				return fmt.Errorf("failed to close writer: %s", err)
			}
			
			// Close parquet file
			if err := fileMeta.parquetFile.Close(); err != nil {
				return fmt.Errorf("failed to close parquet file: %s", err)
			}
		}
	}
	
	// Cleanup empty files
	for basePath, parquetFiles := range p.partitionedFiles {
		err := utils.Concurrent(context.TODO(), parquetFiles, len(parquetFiles), func(_ context.Context, fileMetadata FileMetadata, _ int) error {
			// construct full file path
			filePath := filepath.Join(p.config.Path, basePath, fileMetadata.fileName)

			// Remove empty files
			if fileMetadata.recordCount == 0 {
				removeLocalFile(filePath, "no records written", fileMetadata.recordCount)
				return nil
			}

			// Remove files with no data written
			if fileMetadata.currentSize == 0 {
				removeLocalFile(filePath, "no data written", fileMetadata.recordCount)
				return nil
			}

			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to cleanup files: %s", err)
		}
	}
	
	// Upload to S3 if configured
	if p.s3Client != nil {
		for path, files := range p.partitionedFiles {
			for _, fileMeta := range files {
				// Skip small files if they're below minimum size
				if fileMeta.currentSize < p.config.MinFileSizeMB*1024*1024 {
					continue
				}
				
				localPath := filepath.Join(p.config.Path, path, fileMeta.fileName)
				remotePath := filepath.Join(p.config.Prefix, path, fileMeta.fileName)
				
				// Upload to S3
				_, err := p.s3Client.PutObject(&s3.PutObjectInput{
					Bucket: aws.String(p.config.Bucket),
					Key:    aws.String(remotePath),
					Body:   aws.ReadSeekCloser(fileMeta.parquetFile),
				})
				if err != nil {
					return fmt.Errorf("failed to upload to S3: %s", err)
				}
			}
		}
	}
	
	return nil
}
	}
	return nil
}

// EvolveSchema updates the schema based on changes. Need to pass olakeTimestamp to get the correct partition path based on record ingestion time.
func (p *Parquet) EvolveSchema(change, typeChange bool, _ map[string]*types.Property, data types.Record, olakeTimestamp time.Time) error {
	if change || typeChange {
		// create new file and append at end
		partitionedPath := p.getPartitionedFilePath(data, olakeTimestamp)
		err := p.createNewPartitionFile(partitionedPath)
		if err != nil {
			return err
		}
	}

	return nil
}

// Type returns the type of the writer.
func (p *Parquet) Type() string {
	return string(types.Parquet)
}

// Flattener returns a flattening function for records.
func (p *Parquet) Flattener() protocol.FlattenFunction {
	flattener := typeutils.NewFlattener()
	return flattener.Flatten
}

func (p *Parquet) getPartitionedFilePath(values map[string]any, olakeTimestamp time.Time) string {
	pattern := p.stream.Self().StreamMetadata.PartitionRegex
	if pattern == "" {
		return p.basePath
	}
	// path pattern example /{col_name, 'fallback', granularity}/random_string/{col_name, fallback, granularity}
	patternRegex := regexp.MustCompile(`\{([^}]+)\}`)

	// Replace placeholders
	result := patternRegex.ReplaceAllStringFunc(pattern, func(match string) string {
		trimmed := strings.Trim(match, "{}")
		regexVarBlock := strings.Split(trimmed, ",")

		colName := strings.TrimSpace(strings.Trim(regexVarBlock[0], `'`))
		defaultValue := strings.TrimSpace(strings.Trim(regexVarBlock[1], `'`))
		granularity := strings.TrimSpace(strings.Trim(regexVarBlock[2], `'`))

		if defaultValue == "" {
			defaultValue = fmt.Sprintf("default_%s", colName)
		}

		granularityFunction := func(value any) string {
			if granularity != "" {
				timestampInterface, err := typeutils.ReformatValue(types.Timestamp, value)
				if err == nil {
					timestamp, converted := timestampInterface.(time.Time)
					if converted {
						switch granularity {
						case "HH":
							value = fmt.Sprintf("%02d", timestamp.UTC().Hour())
						case "DD":
							value = fmt.Sprintf("%02d", timestamp.UTC().Day())
						case "WW":
							_, week := timestamp.UTC().ISOWeek()
							value = fmt.Sprintf("%02d", week)
						case "MM":
							value = fmt.Sprintf("%02d", int(timestamp.UTC().Month()))
						case "YYYY":
							value = timestamp.UTC().Year()
						}
					}
				} else {
					logger.Debugf("Failed to convert value to timestamp: %s", err)
				}
			}
			return fmt.Sprintf("%v", value)
		}
		if colName == "now()" {
			return granularityFunction(olakeTimestamp)
		}
		value, exists := values[colName]
		if exists {
			return granularityFunction(value)
		}
		return defaultValue
	})

	return filepath.Join(p.basePath, strings.TrimSuffix(result, "/"))
}

func init() {
	protocol.RegisteredWriters[types.Parquet] = func() protocol.Writer {
		return new(Parquet)
	}
}
