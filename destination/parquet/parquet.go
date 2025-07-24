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
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
	pqgo "github.com/parquet-go/parquet-go"
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
	options          *destination.Options
	config           *Config
	stream           types.StreamInterface
	basePath         string                    // construct with streamNamespace/streamName
	partitionedFiles map[string][]FileMetadata // mapping of basePath/{regex} -> pqFiles
	s3Client         *s3.S3
}

// GetConfigRef returns the config reference for the parquet writer.
func (p *Parquet) GetConfigRef() destination.Config {
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
	if p.config.S3Endpoint != "" {
		s3Config.Endpoint = aws.String(p.config.S3Endpoint)
		// Force path-style URLs (e.g., http://minio:9000/bucket/key) to support MinIO and avoid bucket-based DNS resolution
		s3Config.S3ForcePathStyle = aws.Bool(true)
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
		parquetFile: pqFile,
		writer:      writer,
	})

	return nil
}

// Setup configures the parquet writer, including local paths, file names, and optional S3 setup.
func (p *Parquet) Setup(stream types.StreamInterface, options *destination.Options) error {
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
	partitionedPath := p.getPartitionedFilePath(record.Data, record.OlakeTimestamp)

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
	var err error
	if p.stream.NormalizationEnabled() {
		record.Data[constants.OlakeID] = record.OlakeID
		record.Data[constants.OlakeTimestamp] = record.OlakeTimestamp
		record.Data[constants.OpType] = record.OperationType
		record.Data[constants.CdcTimestamp] = record.CdcTimestamp
		_, err = fileMetadata.writer.(*pqgo.GenericWriter[any]).Write([]any{record.Data})
	} else {
		_, err = fileMetadata.writer.(*pqgo.GenericWriter[types.RawRecord]).Write([]types.RawRecord{record})
	}
	if err != nil {
		return fmt.Errorf("failed to write in parquet file: %s", err)
	}
	fileMetadata.recordCount++
	return nil
}

// Check validates local paths and S3 credentials if applicable.
func (p *Parquet) Check(_ context.Context) error {
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
		// trim '/' from prefix path
		p.config.Prefix = strings.Trim(p.config.Prefix, "/")
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

func (p *Parquet) Close(ctx context.Context) error {
	removeLocalFile := func(filePath, reason string, recordCount int) {
		err := os.Remove(filePath)
		if err != nil {
			logger.Warnf("Failed to delete file [%s] with %d records (%s): %s", filePath, recordCount, reason, err)
			return
		}
		logger.Debugf("Deleted file [%s] with %d records (%s).", filePath, recordCount, reason)
	}

	for basePath, parquetFiles := range p.partitionedFiles {
		err := utils.Concurrent(ctx, parquetFiles, len(parquetFiles), func(_ context.Context, fileMetadata FileMetadata, _ int) error {
			// construct full file path
			filePath := filepath.Join(p.config.Path, basePath, fileMetadata.fileName)

			// Remove empty files
			if fileMetadata.recordCount == 0 {
				removeLocalFile(filePath, "no records written", fileMetadata.recordCount)
				return nil
			}

			// Close writers
			var err error
			if p.stream.NormalizationEnabled() {
				err = fileMetadata.writer.(*pqgo.GenericWriter[any]).Close()
			} else {
				err = fileMetadata.writer.(*pqgo.GenericWriter[types.RawRecord]).Close()
			}
			if err != nil {
				return fmt.Errorf("failed to close writer: %s", err)
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
				s3KeyPath := basePath
				if p.config.Prefix != "" {
					s3KeyPath = filepath.Join(p.config.Prefix, s3KeyPath)
				}
				s3KeyPath = filepath.Join(s3KeyPath, fileMetadata.fileName)

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
			return fmt.Errorf("failed to close writers and files: %s", err)
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
func (p *Parquet) Flattener() destination.FlattenFunction {
	flattener := typeutils.NewFlattener()
	return flattener.Flatten
}

func (p *Parquet) getPartitionedFilePath(values map[string]any, olakeTimestamp time.Time) string {
	pattern := p.stream.Self().StreamMetadata.PartitionRegex
	if pattern == "" {
		return p.basePath
	}
	// path pattern example /{col_name, 'fallback', granularity}/random_string/{col_name, fallback, granularity}
	patternRegex := regexp.MustCompile(constants.PartitionRegexParquet)

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

func (p *Parquet) DropStreams(ctx context.Context, selectedStreams []string) error {
	if len(selectedStreams) == 0 {
		logger.Info("No streams selected for clearing, skipping clear operation")
		return nil
	}

	logger.Infof("Clearing destination for %d selected streams: %v", len(selectedStreams), selectedStreams)

	if p.s3Client == nil {
		if err := p.clearLocalFiles(selectedStreams); err != nil {
			return fmt.Errorf("failed to clear local files: %s", err)
		}
	} else {
		if err := p.clearS3Files(ctx, selectedStreams); err != nil {
			return fmt.Errorf("failed to clear S3 files: %s", err)
		}
	}

	logger.Info("Successfully cleared destination for selected streams")
	return nil
}

func (p *Parquet) clearLocalFiles(selectedStreams []string) error {
	for _, streamID := range selectedStreams {
		parts := strings.SplitN(streamID, ".", 2)
		if len(parts) != 2 {
			logger.Warnf("Invalid stream ID format: %s, skipping", streamID)
			continue
		}

		namespace, streamName := parts[0], parts[1]
		streamPath := filepath.Join(p.config.Path, namespace, streamName)

		logger.Infof("Clearing local path: %s", streamPath)

		if _, err := os.Stat(streamPath); os.IsNotExist(err) {
			logger.Debugf("Local path does not exist, skipping: %s", streamPath)
			continue
		}

		if err := os.RemoveAll(streamPath); err != nil {
			return fmt.Errorf("failed to remove local path %s: %s", streamPath, err)
		}

		logger.Debugf("Successfully cleared local path: %s", streamPath)
	}

	return nil
}

func (p *Parquet) clearS3Files(ctx context.Context, selectedStreams []string) error {
	deleteS3PrefixStandard := func(filtPath string) error {
		iter := s3manager.NewDeleteListIterator(p.s3Client, &s3.ListObjectsInput{
			Bucket: aws.String(p.config.Bucket),
			Prefix: aws.String(filtPath),
		})

		if err := s3manager.NewBatchDeleteWithClient(p.s3Client).Delete(ctx, iter); err != nil {
			return fmt.Errorf("batch delete failed for filtPath %s: %s", filtPath, err)
		}
		return nil
	}

	for _, streamID := range selectedStreams {
		parts := strings.SplitN(streamID, ".", 2)
		if len(parts) != 2 {
			logger.Warnf("Invalid stream ID format: %s, skipping", streamID)
			continue
		}

		namespace, streamName := parts[0], parts[1]
		s3TablePath := filepath.Join(p.config.Prefix, namespace, streamName, "/")
		logger.Debugf("Clearing S3 prefix: s3://%s/%s", p.config.Bucket, s3TablePath)
		if err := deleteS3PrefixStandard(s3TablePath); err != nil {
			return fmt.Errorf("failed to clear S3 prefix %s: %s", s3TablePath, err)
		}

		logger.Debugf("Successfully cleared S3 prefix: s3://%s/%s", p.config.Bucket, s3TablePath)
	}
	return nil
}

func init() {
	destination.RegisteredWriters[types.Parquet] = func() destination.Writer {
		return new(Parquet)
	}
}
