package parquet

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
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
	"github.com/fraugster/parquet-go/parquet"
	pqgo "github.com/parquet-go/parquet-go"

	goparquet "github.com/fraugster/parquet-go"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/source"
)

type FileMetadata struct {
	fileName    string
	recordCount int
	writer      any
	parquetFile source.ParquetFile
}

// Parquet destination writes Parquet files to a local path and optionally uploads them to S3.
type Parquet struct {
	options          *protocol.Options
	config           *Config
	stream           protocol.Stream
	basePath         string                    // construct with streamNamespace/streamName
	partitionedFiles map[string][]FileMetadata // mapping of basePath/{regex} -> pqFiles
	pqSchemaMutex    sync.Mutex                // To prevent concurrent map access from fraugster library
	s3Client         *s3.S3
}

// GetConfigRef returns the config reference for the parquet writer.
func (p *Parquet) GetConfigRef() protocol.Config {
	p.config = &Config{}
	return p.config
}

// Spec returns a new Config instance.
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
	// construct directory path
	directoryPath := filepath.Join(p.config.Path, basePath)

	if err := os.MkdirAll(directoryPath, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create directories[%s]: %w", directoryPath, err)
	}

	fileName := utils.TimestampedFileName(constants.ParquetFileExt)
	filePath := filepath.Join(directoryPath, fileName)

	pqFile, err := local.NewLocalFileWriter(filePath)
	if err != nil {
		return fmt.Errorf("failed to create parquet file writer: %w", err)
	}

	writer := func() any {
		if p.config.Normalization {
			return goparquet.NewFileWriter(pqFile,
				goparquet.WithSchemaDefinition(p.stream.Schema().ToParquet()),
				goparquet.WithMaxRowGroupSize(100),
				goparquet.WithMaxPageSize(10),
				goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
			)
		}
		return pqgo.NewGenericWriter[types.RawRecord](pqFile, pqgo.Compression(&pqgo.Snappy))
	}()

	p.partitionedFiles[basePath] = append(p.partitionedFiles[basePath], FileMetadata{
		fileName:    fileName,
		parquetFile: pqFile,
		writer:      writer,
	})

	return nil
}

// Setup configures the parquet writer, including local paths, file names, and optional S3 setup.
func (p *Parquet) Setup(stream protocol.Stream, options *protocol.Options) error {
	p.options = options
	p.stream = stream
	p.partitionedFiles = make(map[string][]FileMetadata)

	// for s3 p.config.path may not be provided
	if p.config.Path == "" {
		p.config.Path = os.TempDir()
	}

	p.basePath = filepath.Join(p.stream.Namespace(), p.stream.Name())
	err := p.createNewPartitionFile(p.basePath)
	if err != nil {
		return fmt.Errorf("failed to create new partition file: %s", err)
	}

	err = p.initS3Writer()
	if err != nil {
		return err
	}
	return nil
}

// Write writes a record to the Parquet file.
func (p *Parquet) Write(_ context.Context, record types.RawRecord) error {
	partitionedPath := p.getPartitionedFilePath(record.Data)

	partitionFolder, exists := p.partitionedFiles[partitionedPath]
	if !exists {
		err := p.createNewPartitionFile(partitionedPath)
		if err != nil {
			return fmt.Errorf("failed to create parititon file: %s", err)
		}
		partitionFolder = p.partitionedFiles[partitionedPath]
	}

	if len(partitionFolder) == 0 {
		return fmt.Errorf("failed to get partitioned files")
	}

	// get last written file
	fileMetadata := &partitionFolder[len(partitionFolder)-1]

	if p.config.Normalization {
		// TODO: Need to check if we can remove locking to fasten sync (Good First Issue)
		p.pqSchemaMutex.Lock()
		defer p.pqSchemaMutex.Unlock()
		if err := fileMetadata.writer.(*goparquet.FileWriter).AddData(record.Data); err != nil {
			return fmt.Errorf("parquet write error: %s", err)
		}
	} else {
		// locking not required as schema is fixed for base writer
		if _, err := fileMetadata.writer.(*pqgo.GenericWriter[types.RawRecord]).Write([]types.RawRecord{record}); err != nil {
			return fmt.Errorf("parquet write error: %s", err)
		}
	}
	fileMetadata.recordCount++
	return nil
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
		logger.Infof("local writer configuration found, writing at location[%s]", p.config.Path)
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
	removeLocalFile := func(filePath, reason string, recordCount int) {
		err := os.Remove(filePath)
		if err != nil {
			logger.Warnf("Failed to delete file [%s] with %d records (%s): %s", filePath, recordCount, reason, err)
			return
		}
		logger.Debugf("Deleted file [%s] with %d records (%s).", filePath, recordCount, reason)
	}

	for basePath, parquetFiles := range p.partitionedFiles {
		ctx := context.Background()
		err := utils.Concurrent(ctx, parquetFiles, len(parquetFiles), func(_ context.Context, fileMetadata FileMetadata, _ int) error {
			// construct full file path
			filePath := filepath.Join(p.config.Path, basePath, fileMetadata.fileName)

			// Remove empty files
			if fileMetadata.recordCount == 0 {
				removeLocalFile(filePath, "no records written", fileMetadata.recordCount)
				return nil
			}

			// Close writers
			if p.config.Normalization {
				if err := fileMetadata.writer.(*goparquet.FileWriter).Close(); err != nil {
					return fmt.Errorf("failed to close normalized writer: %s", err)
				}
			} else {
				if err := fileMetadata.writer.(*pqgo.GenericWriter[types.RawRecord]).Close(); err != nil {
					return fmt.Errorf("failed to close base writer: %s", err)
				}
			}

			// Close file
			if err := fileMetadata.parquetFile.Close(); err != nil {
				return fmt.Errorf("failed to close file: %s", err)
			}

			logger.Infof("Finished writing file [%s] with %d records.", filePath, fileMetadata.recordCount)

			if p.s3Client != nil {
				// Open file for S3 upload
				file, err := os.Open(filePath)
				if err != nil {
					return fmt.Errorf("failed to open local file for S3 upload: %s", err)
				}
				defer file.Close()

				// Construct S3 key path
				if p.config.Prefix != "" {
					basePath = filepath.Join(p.config.Prefix, basePath)
				}
				s3KeyPath := filepath.Join(basePath, fileMetadata.fileName)

				// Upload to S3
				_, err = p.s3Client.PutObject(&s3.PutObjectInput{
					Bucket: aws.String(p.config.Bucket),
					Key:    aws.String(s3KeyPath),
					Body:   file,
				})
				if err != nil {
					return fmt.Errorf("failed to upload file to S3 (bucket: %s, path: %s): %s", p.config.Bucket, s3KeyPath, err)
				}

				// Remove local file after successful upload
				removeLocalFile(filePath, "uploaded to S3", fileMetadata.recordCount)
				logger.Infof("Successfully uploaded file to S3: s3://%s/%s", p.config.Bucket, s3KeyPath)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to close writers and files:%s", err)
		}
	}
	return nil
}

// EvolveSchema updates the schema based on changes.
func (p *Parquet) EvolveSchema(change, typeChange bool, _ map[string]*types.Property, data types.Record) error {
	if change || typeChange {
		// create new file and append at end
		partitionedPath := p.getPartitionedFilePath(data)
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

func (p *Parquet) Normalization() bool {
	return p.config.Normalization
}

func (p *Parquet) getPartitionedFilePath(values map[string]any) string {
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
							value = timestamp.UTC().Hour()
						case "DD":
							value = timestamp.UTC().Day()
						case "WW":
							_, value = timestamp.UTC().ISOWeek()
						case "MM":
							value = int(timestamp.UTC().Month())
						case "YY":
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
			return granularityFunction(time.Now().UTC())
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
