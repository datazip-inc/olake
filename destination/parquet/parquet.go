package parquet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
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
	fileName string
	writer   any
	file     source.ParquetFile
}

// Parquet is the destination-level manager. It validates config and S3 access
// (Check), clears streams (DropTables), and spawns per-stream writer threads.
type Parquet struct {
	config     *Config
	s3Client   *s3.S3
	s3Uploader *s3manager.Uploader
}

// Writer writes Parquet files for a single stream thread to a local path
// and optionally uploads them to S3.
type Thread struct {
	config           *Config
	s3Client         *s3.S3
	s3Uploader       *s3manager.Uploader
	options          *destination.Options
	stream           types.StreamInterface
	basePath         string                     // construct with streamNamespace/streamName
	partitionedFiles map[string][]*FileMetadata // mapping of basePath/{regex} -> pqFiles
	schema           typeutils.Fields
}

// close removes global resources
func (p *Parquet) Close(_ context.Context) error {
	return nil
}

// Check validates local paths and S3 credentials if applicable.
func (p *Parquet) Check(_ context.Context) error {
	uniqueSuffix := fmt.Sprintf("%d", time.Now().UnixNano())
	threadID := fmt.Sprintf("test_parquet_destination_%s", uniqueSuffix)

	// test for s3 permissions
	if p.s3Client != nil {
		testKey := filepath.Join(p.config.Prefix, "olake_writer_test", utils.TimestampedFileName(".txt"))
		// Try to upload a small test file
		_, err := p.s3Client.PutObject(&s3.PutObjectInput{
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
		logger.Infof("Thread[%s]: s3 writer configuration found", threadID)
	} else if p.config.Path != "" {
		logger.Infof("Thread[%s]: local writer configuration found, writing at location[%s]", threadID, p.config.Path)
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

func (p *Parquet) clearS3Files(ctx context.Context, paths []string) error {
	deleteS3PrefixIndividually := func(filtPath string) error {
		var pageErr error
		listErr := utils.RetryWithSkip(ctx, 3, time.Minute, isRateLimitError, func(_ context.Context) error {
			pageErr = nil
			return p.s3Client.ListObjectsPagesWithContext(ctx, &s3.ListObjectsInput{
				Bucket: aws.String(p.config.Bucket),
				Prefix: aws.String(filtPath),
			}, func(page *s3.ListObjectsOutput, _ bool) bool {
				pageKeys := make([]string, 0, len(page.Contents))
				for _, obj := range page.Contents {
					pageKeys = append(pageKeys, *obj.Key)
				}
				if len(pageKeys) == 0 {
					return true
				}

				logger.Debugf("individual delete: found %d objects under %s, deleting", len(pageKeys), filtPath)

				// GCP allows 5000 mutations per second per bucket
				concurrency := min(runtime.GOMAXPROCS(0)*4, len(pageKeys))
				if pageErr = utils.Concurrent(ctx, pageKeys, concurrency, func(_ context.Context, key string, _ int) error {
					return utils.RetryWithSkip(ctx, 3, time.Minute, isRateLimitError, func(_ context.Context) error {
						_, err := p.s3Client.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
							Bucket: aws.String(p.config.Bucket),
							Key:    aws.String(key),
						})
						if err != nil {
							return err
						}
						return nil
					})
				}); pageErr != nil {
					return false
				}
				return true
			})
		})
		if listErr != nil {
			return fmt.Errorf("failed to list objects for prefix %s: %v", filtPath, listErr)
		}
		return pageErr
	}

	deleteS3PrefixStandard := func(filtPath string) error {
		err := utils.RetryWithSkip(ctx, 3, time.Minute, isRateLimitError, func(_ context.Context) error {
			iter := s3manager.NewDeleteListIterator(p.s3Client, &s3.ListObjectsInput{
				Bucket: aws.String(p.config.Bucket),
				Prefix: aws.String(filtPath),
			})
			return s3manager.NewBatchDeleteWithClient(p.s3Client).Delete(ctx, iter)
		})

		if err != nil {
			logger.Warnf("batch delete failed for filtPath %s, falling back to individual deletes: %v", filtPath, err)
			if fallbackErr := deleteS3PrefixIndividually(filtPath); fallbackErr != nil {
				return fmt.Errorf("batch delete failed: %v, fallback individual delete also failed: %s", err, fallbackErr)
			}
		}
		return nil
	}

	for _, streamID := range paths {
		parts := strings.SplitN(streamID, ".", 2)
		if len(parts) != 2 {
			logger.Warnf("invalid stream ID format: %s, skipping", streamID)
			continue
		}
		prefix, namespace, tableName := strings.TrimLeft(p.config.Prefix, "/"), parts[0], parts[1]
		s3TablePath := filepath.Join(prefix, namespace, tableName, "/")

		logger.Debugf("clearing S3 prefix: s3://%s/%s", p.config.Bucket, s3TablePath)

		var err error
		if strings.Contains(p.config.S3Endpoint, "googleapis.com") {
			err = deleteS3PrefixIndividually(s3TablePath)
		} else {
			err = deleteS3PrefixStandard(s3TablePath)
		}

		if err != nil {
			return fmt.Errorf("failed to clear S3 prefix %s: %s", s3TablePath, err)
		}

		logger.Debugf("successfully cleared S3 prefix: s3://%s/%s", p.config.Bucket, s3TablePath)
	}
	return nil
}

// Setup configures the parquet writer, including local paths, file names, and optional S3 setup.
func (p *Parquet) NewWriterThread(_ context.Context, stream types.StreamInterface, schema any, options *destination.Options) (destination.Writer, any, *types.MetadataState, error) {
	threadSchema := make(typeutils.Fields)

	if stream.NormalizationEnabled() {
		if schema != nil {
			fields, ok := schema.(typeutils.Fields)
			if !ok {
				return nil, nil, nil, fmt.Errorf("failed to typecast schema[%T] into typeutils.Fields", schema)
			}
			threadSchema = fields.Clone()
		} else {
			fields := make(typeutils.Fields)
			fields.FromSchema(stream.Schema(), stream.ResolveColumnName)
			threadSchema = fields.Clone() // update schema
		}
	}

	return &Thread{
		config:           p.config,
		s3Client:         p.s3Client,
		s3Uploader:       p.s3Uploader,
		options:          options,
		stream:           stream,
		basePath:         filepath.Join(stream.GetDestinationDatabase(nil), stream.GetDestinationTable()),
		schema:           threadSchema,
		partitionedFiles: make(map[string][]*FileMetadata),
	}, schema, nil, nil
}

func (t *Thread) createNewPartitionFile(basePath string) error {
	// construct directory path
	directoryPath := filepath.Join(t.config.Path, basePath)

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
		if t.stream.NormalizationEnabled() {
			return pqgo.NewGenericWriter[any](pqFile, t.schema.ToTypeSchema().ToParquet(false, t.stream), pqgo.Compression(&pqgo.Snappy))
		}
		return pqgo.NewGenericWriter[any](pqFile, t.stream.Schema().ToParquet(true, t.stream), pqgo.Compression(&pqgo.Snappy))
	}()

	t.partitionedFiles[basePath] = append(t.partitionedFiles[basePath], &FileMetadata{
		fileName: fileName,
		file:     pqFile,
		writer:   writer,
	})

	logger.Infof("Thread[%s]: created new partition file[%s]", t.options.ThreadID, filePath)
	return nil
}

// Write writes a record to the Parquet file.
func (t *Thread) Write(_ context.Context, records []types.RawRecord) error {
	// TODO: use batch writing feature of pq writer
	for _, record := range records {
		// Normalise "i" -? "c": Parquet has no equality-delete concept; downstream
		// consumers must see a consistent "c" for all CDC inserts.
		// OlakeColumns covers the non-normalized path; Data covers the normalized path
		// where FlattenAndCleanData has already merged OlakeColumns into Data.
		if opType, ok := record.OlakeColumns[constants.OpType].(string); ok && opType == "i" {
			record.OlakeColumns[constants.OpType] = "c"
			if _, exists := record.Data[constants.OpType]; exists {
				record.Data[constants.OpType] = "c"
			}
		}
		partitionedPath := t.getPartitionedFilePath(record.Data, record.OlakeColumns[constants.OlakeTimestamp].(time.Time))
		partitionFiles, exists := t.partitionedFiles[partitionedPath]
		if !exists {
			err := t.createNewPartitionFile(partitionedPath)
			if err != nil {
				return fmt.Errorf("failed to create partition file: %s", err)
			}
			partitionFiles = t.partitionedFiles[partitionedPath]
		}

		if len(partitionFiles) == 0 {
			return fmt.Errorf("failed to create partition file for path[%s]", partitionedPath)
		}

		partitionFile := partitionFiles[len(partitionFiles)-1]

		var err error
		if t.stream.NormalizationEnabled() {
			_, err = partitionFile.writer.(*pqgo.GenericWriter[any]).Write([]any{record.Data})
		} else {
			dataBytes, merr := json.Marshal(record.Data)
			if merr != nil {
				return fmt.Errorf("failed to marshal data: %s", err)
			}
			recordsMap := map[string]any{constants.StringifiedData: string(dataBytes)}
			maps.Copy(recordsMap, record.OlakeColumns)

			_, err = partitionFile.writer.(*pqgo.GenericWriter[any]).Write([]any{recordsMap})
		}
		if err != nil {
			return fmt.Errorf("failed to write in parquet file: %s", err)
		}
	}

	return nil
}

func (p *Parquet) clearLocalFiles(paths []string) error {
	for _, streamID := range paths {
		parts := strings.SplitN(streamID, ".", 2)
		if len(parts) != 2 {
			logger.Warnf("invalid stream ID format: %s, skipping", streamID)
			continue
		}
		namespace, tableName := parts[0], parts[1]
		streamPath := filepath.Join(p.config.Path, namespace, tableName)

		logger.Infof("clearing local path: %s", streamPath)

		if _, err := os.Stat(streamPath); os.IsNotExist(err) {
			logger.Debugf("local path does not exist, skipping: %s", streamPath)
			continue
		}

		if err := os.RemoveAll(streamPath); err != nil {
			return fmt.Errorf("failed to remove local path %s: %s", streamPath, err)
		}
	}

	return nil
}

func (t *Thread) closePqFiles(ctx context.Context, _ any, closeOnError bool) error {
	removeLocalFile := func(filePath, reason string) {
		err := os.Remove(filePath)
		if err != nil {
			logger.Warnf("Thread[%s]: Failed to delete file [%s], reason (%s): %s", t.options.ThreadID, filePath, reason, err)
			return
		}
		logger.Debugf("Thread[%s]: Deleted file [%s], reason (%s).", t.options.ThreadID, filePath, reason)
	}

	// Struct to hold file upload info
	type uploadInfo struct {
		filePath  string
		s3KeyPath string
	}

	var filesToUpload []uploadInfo

	for basePath, parquetFiles := range t.partitionedFiles {
		for _, parquetFile := range parquetFiles {
			// construct full file path
			filePath := filepath.Join(t.config.Path, basePath, parquetFile.fileName)

			// Close writers
			err := parquetFile.writer.(*pqgo.GenericWriter[any]).Close()
			if err != nil {
				return fmt.Errorf("failed to close writer: %s", err)
			}

			// Close file
			if err := parquetFile.file.Close(); err != nil {
				return fmt.Errorf("failed to close file: %s", err)
			}

			logger.Infof("Thread[%s]: Finished writing file [%s].", t.options.ThreadID, filePath)

			// close after closing writers
			if closeOnError {
				removeLocalFile(filePath, "closing parquet files due to retry attempt")
				continue
			}

			if t.s3Client != nil {
				// Construct S3 key path
				s3KeyPath := basePath
				if t.config.Prefix != "" {
					s3KeyPath = filepath.Join(t.config.Prefix, s3KeyPath)
				}
				s3KeyPath = filepath.Join(s3KeyPath, parquetFile.fileName)

				filesToUpload = append(filesToUpload, uploadInfo{
					filePath:  filePath,
					s3KeyPath: s3KeyPath,
				})
			}
		}
	}

	if len(filesToUpload) > 0 && t.s3Client != nil {
		concurrency := min(runtime.GOMAXPROCS(0)*2, len(filesToUpload))

		err := utils.Concurrent(ctx, filesToUpload, concurrency, func(_ context.Context, info uploadInfo, _ int) error {
			// Open file for S3 upload
			file, err := os.Open(info.filePath)
			if err != nil {
				return fmt.Errorf("failed to open file %s: %s", info.filePath, err)
			}
			defer file.Close()

			// Upload to S3 using multipart upload (automatically handles files > 5GB)
			_, err = t.s3Uploader.Upload(&s3manager.UploadInput{
				Bucket: aws.String(t.config.Bucket),
				Key:    aws.String(info.s3KeyPath),
				Body:   file,
			})
			if err != nil {
				return fmt.Errorf("failed to put object into s3 (%s): %s", info.s3KeyPath, err)
			}

			// Remove local file after successful upload
			removeLocalFile(info.filePath, "uploaded to S3")
			logger.Infof("Thread[%s]: successfully uploaded file to S3: s3://%s/%s", t.options.ThreadID, t.config.Bucket, info.s3KeyPath)
			return nil
		})
		if err != nil {
			return err
		}
	}

	// make map empty
	t.partitionedFiles = make(map[string][]*FileMetadata)
	return nil
}

func (p *Parquet) DropTables(ctx context.Context, selectedStreams []types.StreamInterface) error {
	if len(selectedStreams) == 0 {
		logger.Infof("no streams selected for clearing, skipping clear operation")
		return nil
	}

	paths := make([]string, 0, len(selectedStreams))
	for _, stream := range selectedStreams {
		paths = append(paths, stream.GetDestinationDatabase(nil)+"."+stream.GetDestinationTable())
	}

	if p.s3Client == nil {
		if err := p.clearLocalFiles(paths); err != nil {
			return fmt.Errorf("failed to clear local files: %s", err)
		}
	} else {
		if err := p.clearS3Files(ctx, paths); err != nil {
			return fmt.Errorf("failed to clear S3 files: %s", err)
		}
	}
	return nil
}

func (t *Thread) CommitAndCleanup(ctx context.Context, finalMetadataState any) error {
	// TODO: implement 2pc in parquet writer (difficulty: hard)
	return t.closePqFiles(ctx, finalMetadataState, ctx.Err() != nil)
}

// validate schema change & evolution and removes null records
func (t *Thread) FlattenAndCleanData(ctx context.Context, records []types.RawRecord) (bool, []types.RawRecord, any, error) {
	if !t.stream.NormalizationEnabled() {
		return false, records, nil, nil
	}

	if len(records) == 0 {
		return false, records, t.schema, nil
	}

	diffFound := atomic.Bool{} // to process records concurrently and detect schema difference

	// One flattener per batch: the internal cache amortizes resolve calls
	// across all records so each column name is resolved only once.
	batchFlattener := typeutils.NewFlattener(t.stream.ResolveColumnName)
	err := utils.Concurrent(ctx, records, runtime.GOMAXPROCS(0)*16, func(_ context.Context, record types.RawRecord, idx int) error {
		// Add common fields
		maps.Copy(records[idx].Data, record.OlakeColumns)
		flattenedRecord, err := batchFlattener.Flatten(record.Data)
		if err != nil {
			return fmt.Errorf("failed to flatten record at index %d, pq writer: %s", idx, err)
		}

		// Store flattened result back to the record
		records[idx].Data = flattenedRecord

		if !diffFound.Load() {
			for columnName, columnValue := range flattenedRecord {
				detectedType := typeutils.TypeFromValue(columnValue)
				if _, columnExist := t.schema[columnName]; !columnExist {
					diffFound.Store(true)
					break
				}

				persistedTypes := t.schema[columnName].Types()
				if _, exist := utils.ArrayContains(persistedTypes, func(elem types.DataType) bool {
					return elem == detectedType
				}); !exist {
					diffFound.Store(true)
					break
				}
			}
		}
		return nil
	})
	if err != nil {
		return false, nil, nil, fmt.Errorf("failed to process records: %s", err)
	}

	schemaChange := false // note: diff schema already detected so we can avoid this in future

	if diffFound.Load() {
		for _, record := range records {
			// Process the changes and upgrade new schema
			change, typeChange, _ := t.schema.Process(record.Data)
			schemaChange = change || typeChange || schemaChange
		}
	}

	if err := utils.Concurrent(ctx, records, runtime.GOMAXPROCS(0)*16, func(_ context.Context, record types.RawRecord, _ int) error {
		return typeutils.ReformatRecord(t.schema, record.Data)
	}); err != nil {
		return false, nil, nil, fmt.Errorf("failed to reformat records: %s", err)
	}
	if t.options.ApplyFilter {
		filter, isLegacy, filterErr := t.stream.GetFilter()
		if filterErr != nil {
			return false, nil, nil, fmt.Errorf("failed to parse stream filter: %s", filterErr)
		}
		records, err = typeutils.FilterRecords(ctx, records, filter, isLegacy, t.schema, t.stream.ResolveColumnName)
		if err != nil {
			return false, nil, nil, fmt.Errorf("failed to filter records: %s", err)
		}
	}
	return schemaChange, records, t.schema, nil
}

// EvolveSchema updates the schema based on changes. Need to pass olakeTimestamp to get the correct partition path based on record ingestion time.
func (t *Thread) EvolveSchema(_ context.Context, _, _ any) (any, error) {
	if !t.stream.NormalizationEnabled() {
		return false, nil
	}

	logger.Infof("Thread[%s]: schema evolution detected", t.options.ThreadID)

	// create new partition files for all paths as prev are of no use
	for path := range t.partitionedFiles {
		err := t.createNewPartitionFile(path)
		if err != nil {
			return nil, fmt.Errorf("failed to create new partition file: %s", err)
		}
	}

	// TODO: can we implement something https://github.com/parquet-go/parquet-go?tab=readme-ov-file#evolving-parquet-schemas-parquetconvert
	// close prev files as change detected (new files will be created with new schema)
	return t.schema.Clone(), nil
}

// Type returns the type of the writer.
func (p *Parquet) Type() string {
	return string(types.Parquet)
}

func (t *Thread) getPartitionedFilePath(values map[string]any, olakeTimestamp time.Time) string {
	pattern := t.stream.Self().StreamMetadata.PartitionRegex
	if pattern == "" {
		return t.basePath
	}
	// path pattern example /{col_name, 'fallback', granularity}/random_string/{col_name, fallback, granularity}
	patternRegex := regexp.MustCompile(constants.PartitionRegexParquet)

	// Replace placeholders
	result := patternRegex.ReplaceAllStringFunc(pattern, func(match string) string {
		trimmed := strings.Trim(match, "{}")
		regexVarBlock := strings.Split(trimmed, ",")

		if len(regexVarBlock) < 3 {
			return ""
		}

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
					logger.Debugf("Thread[%s]: failed to convert value to timestamp: %s", t.options.ThreadID, err)
				}
			}
			return fmt.Sprintf("%v", value)
		}
		if colName == "now()" {
			return granularityFunction(olakeTimestamp)
		}
		// Resolve the regex column name using the stream's naming strategy so that the
		// key matches record.Data keys after FlattenAndCleanData (resolved when
		// normalization=true, raw source name when normalization=false).
		// Try the resolved key first; fall back to the raw source name so that
		// normalization=false + use_source_column_names=false still works.
		resolvedColName := t.stream.ResolveColumnName(colName)
		value, exists := values[resolvedColName]
		if !exists {
			value, exists = values[colName]
		}
		if exists && value != nil {
			return granularityFunction(value)
		}
		return defaultValue
	})

	if result == "" {
		// use default for invalid partitions
		return t.basePath
	}
	return filepath.Join(t.basePath, strings.TrimSuffix(result, "/"))
}

// isRateLimitError checks if the error is a rate-limit/throttle response from S3 or GCP.
// AWS S3 returns HTTP 503 for throttling (SlowDown / ServiceUnavailable).
// GCP Cloud Storage returns HTTP 429 (Too Many Requests).
//
// For batch delete operations, errors are wrapped in s3manager.BatchError which does NOT
// implement awserr.RequestFailure directly. The actual RequestFailure is nested inside
// BatchError.Errors[].OrigErr, so we must inspect those inner errors as well.
func isRateLimitError(err error) bool {
	isThrottled := func(target error) bool {
		var rf awserr.RequestFailure
		return errors.As(target, &rf) && (rf.StatusCode() == 429 || rf.StatusCode() == 503)
	}
	if isThrottled(err) {
		return true
	}
	// AWS SDK v1 batch errors don't implement Unwrap(), so we peel one layer manually.
	var batchErr awserr.Error
	if errors.As(err, &batchErr) {
		return isThrottled(batchErr.OrigErr())
	}
	return false
}

// build s3 client and uploader
func newS3Writer(config *Config) (*s3.S3, *s3manager.Uploader, error) {
	if config.Bucket == "" || config.Region == "" {
		return nil, nil, nil
	}

	s3Config := aws.Config{
		Region: aws.String(config.Region),
	}
	if config.S3Endpoint != "" {
		s3Config.Endpoint = aws.String(config.S3Endpoint)
		// Force path-style URLs (e.g., http://minio:9000/bucket/key) to support MinIO and avoid bucket-based DNS resolution
		s3Config.S3ForcePathStyle = aws.Bool(true)
	}
	if config.AccessKey != "" && config.SecretKey != "" {
		s3Config.Credentials = credentials.NewStaticCredentials(config.AccessKey, config.SecretKey, "")
	}
	sess, err := session.NewSession(&s3Config)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create AWS session: %s", err)
	}
	// Initialize uploader for multipart uploads (handles files > 5GB automatically)
	return s3.New(sess), s3manager.NewUploader(sess), nil
}

func init() {
	destination.RegisteredWriters[types.Parquet] = func(config any) (destination.Destination, error) {
		parquetConfig := &Config{}
		err := utils.Unmarshal(config, parquetConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal parquet config: %s", err)
		}

		if err := parquetConfig.Validate(); err != nil {
			return nil, fmt.Errorf("failed to validate parquet config: %s", err)
		}

		s3Client, s3Uploader, err := newS3Writer(parquetConfig)
		if err != nil {
			return nil, err
		}

		return &Parquet{
			config:     parquetConfig,
			s3Client:   s3Client,
			s3Uploader: s3Uploader,
		}, nil
	}
}
