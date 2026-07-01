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
	writer   *pqgo.GenericWriter[any]
	file     source.ParquetFile // local file the parquet is streamed to; nil once finalized
	path     string             // full local path; used for size checks and S3 upload
}

// Parquet destination writes Parquet files to a local path and optionally uploads them to S3.
type Parquet struct {
	options          *destination.Options
	config           *Config
	stream           types.StreamInterface
	basePath         string                     // construct with streamNamespace/streamName
	partitionedFiles map[string][]*FileMetadata // mapping of basePath/{regex} -> open (un-finalized) pqFiles
	s3Client         *s3.S3
	s3Uploader       *s3manager.Uploader
	schema           typeutils.Fields

	maxFileBytes         int64 // roll a partition into a new file once its on-disk size reaches this
	checkIntervalForRoll int   // number of []RawRecord written between on-disk size checks within a batch
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
	// Initialize uploader for multipart uploads (handles files > 5GB automatically)
	p.s3Uploader = s3manager.NewUploader(sess)

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

	writer := func() *pqgo.GenericWriter[any] {
		if p.stream.NormalizationEnabled() {
			return pqgo.NewGenericWriter[any](pqFile, p.schema.ToTypeSchema().ToParquet(false, p.stream), pqgo.Compression(&pqgo.Snappy))
		}
		return pqgo.NewGenericWriter[any](pqFile, p.stream.Schema().ToParquet(true, p.stream), pqgo.Compression(&pqgo.Snappy))
	}()

	p.partitionedFiles[basePath] = append(p.partitionedFiles[basePath], &FileMetadata{
		fileName: fileName,
		file:     pqFile,
		writer:   writer,
		path:     filePath,
	})

	logger.Infof("Thread[%s]: created new partition file[%s]", p.options.ThreadID, filePath)
	return nil
}

// getOrCreatePartitionFile returns the partition's active (open) file, creating a fresh one when
// the partition has no file yet or its most recent file was just sealed by a roll (file == nil).
// Sealed files stay in the partition slice and are uploaded only in Close.
func (p *Parquet) getOrCreatePartitionFile(basePath string) (*FileMetadata, error) {
	files := p.partitionedFiles[basePath]
	if len(files) == 0 || files[len(files)-1].file == nil {
		if err := p.createNewPartitionFile(basePath); err != nil {
			return nil, fmt.Errorf("failed to create partition file: %s", err)
		}
		files = p.partitionedFiles[basePath]
	}
	return files[len(files)-1], nil
}

// Setup configures the parquet writer, including local paths, file names, and optional S3 setup.
func (p *Parquet) Setup(_ context.Context, stream types.StreamInterface, schema any, options *destination.Options) (any, *types.MetadataState, error) {
	p.options = options
	p.stream = stream
	p.partitionedFiles = make(map[string][]*FileMetadata)
	p.basePath = filepath.Join(p.stream.GetDestinationDatabase(nil), p.stream.GetDestinationTable())
	p.schema = make(typeutils.Fields)

	if p.maxFileBytes <= 0 {
		maxFileSizeMB := float64(defaultMaxFileSizeMB)
		if p.config.MaxFileSizeMB > 0 {
			maxFileSizeMB = p.config.MaxFileSizeMB
		}
		p.maxFileBytes = int64(maxFileSizeMB * 1024 * 1024)
	}
	if p.checkIntervalForRoll <= 0 {
		p.checkIntervalForRoll = defaultRollCheckInterval
	}

	// for s3 p.config.path may not be provided
	if p.config.Path == "" {
		p.config.Path = os.TempDir()
	}

	err := p.initS3Writer()
	if err != nil {
		return nil, nil, err
	}

	if !p.stream.NormalizationEnabled() {
		return p.schema, nil, nil
	}

	if schema != nil {
		fields, ok := schema.(typeutils.Fields)
		if !ok {
			return nil, nil, fmt.Errorf("failed to typecast schema[%T] into typeutils.Fields", schema)
		}
		p.schema = fields.Clone()
		return fields, nil, nil
	}

	fields := make(typeutils.Fields)
	fields.FromSchema(stream.Schema(), stream.ResolveColumnName)
	p.schema = fields.Clone() // update schema
	return fields, nil, nil
}

// Write writes a record to the Parquet file.
func (p *Parquet) Write(_ context.Context, records []types.RawRecord) error {
	// TODO: use batch writing feature of pq writer
	for i, record := range records {
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
		partitionedPath := p.getPartitionedFilePath(record.Data, record.OlakeColumns[constants.OlakeTimestamp].(time.Time))
		partitionFile, err := p.getOrCreatePartitionFile(partitionedPath)
		if err != nil {
			return err
		}

		if p.stream.NormalizationEnabled() {
			_, err = partitionFile.writer.Write([]any{record.Data})
		} else {
			dataBytes, merr := json.Marshal(record.Data)
			if merr != nil {
				return fmt.Errorf("failed to marshal data: %s", merr)
			}
			recordsMap := map[string]any{constants.StringifiedData: string(dataBytes)}
			maps.Copy(recordsMap, record.OlakeColumns)

			_, err = partitionFile.writer.Write([]any{recordsMap})
		}
		if err != nil {
			return fmt.Errorf("failed to write in parquet file: %s", err)
		}

		if p.checkForRoll(i, len(records)) {
			if err := p.rollPartitionFile(partitionFile); err != nil {
				return fmt.Errorf("failed to roll partition file: %s", err)
			}
		}
	}

	return nil
}

// roll gives true when we need to check for rolling based on the current index of record
func (p *Parquet) checkForRoll(index, total int) bool {
	interval := p.checkIntervalForRoll
	if interval == 0 {
		return false
	}

	n := index + 1
	return (n%interval == 0) || n == total
}

// rollPartitionFile flushes the partition's active writer so its buffered rows hit disk, then—
// if the on-disk file has reached maxFileBytes—seals it (writing the footer) and leaves it in
// the partition. The next Write opens a fresh file (it sees the sealed file has file == nil).
// Flushing on every check also caps the writer's in-memory row-group buffer, keeping memory
// bounded as the file grows.
//
// Sealed files are intentionally NOT uploaded here — every file is uploaded in Close, after the
// whole partition has rolled successfully, so a mid-sync failure never leaves partial objects in
// S3 (files exist only on local disk until then).
func (p *Parquet) rollPartitionFile(pf *FileMetadata) error {
	// Flush buffered rows to disk so the size check reflects the real bytes written so far.
	if err := pf.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush parquet writer[%s]: %s", pf.path, err)
	}

	info, err := os.Stat(pf.path)
	if err != nil {
		return fmt.Errorf("failed to stat parquet file[%s]: %s", pf.path, err)
	}
	if info.Size() < p.maxFileBytes {
		return nil
	}

	// Threshold reached: write the footer to seal the file. It stays in partitionedFiles (with
	// file == nil marking it finalized) to be uploaded in Close.
	if err := pf.writer.Close(); err != nil {
		return fmt.Errorf("failed to close parquet writer on roll[%s]: %s", pf.path, err)
	}
	if err := pf.file.Close(); err != nil {
		return fmt.Errorf("failed to close parquet file on roll[%s]: %s", pf.path, err)
	}
	pf.file = nil // mark finalized; kept for upload at Close
	logger.Infof("Thread[%s]: rolled partition file[%s] at %d bytes", p.options.ThreadID, pf.path, info.Size())
	return nil
}

// Check validates local paths and S3 credentials if applicable.
func (p *Parquet) Check(_ context.Context) error {
	uniqueSuffix := fmt.Sprintf("%d", time.Now().UnixNano())
	threadID := fmt.Sprintf("test_parquet_destination_%s", uniqueSuffix)

	p.options = &destination.Options{
		ThreadID: threadID,
	}

	// check for s3 writer configuration
	err := p.initS3Writer()
	if err != nil {
		return err
	}
	// test for s3 permissions
	if p.s3Client != nil {
		testKey := filepath.Join(p.config.Prefix, "olake_writer_test", utils.TimestampedFileName(".txt"))
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
		logger.Infof("Thread[%s]: s3 writer configuration found", p.options.ThreadID)
	} else if p.config.Path != "" {
		logger.Infof("Thread[%s]: local writer configuration found, writing at location[%s]", p.options.ThreadID, p.config.Path)
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

// s3KeyFor builds the destination S3 object key for a finalized local file under basePath.
func (p *Parquet) s3KeyFor(basePath, fileName string) string {
	key := basePath
	if p.config.Prefix != "" {
		key = filepath.Join(p.config.Prefix, key)
	}
	return filepath.Join(key, fileName)
}

// uploadLocalFileToS3 uploads filePath to the given key via multipart upload (handles files
// > 5GB automatically) and, on success, deletes the local copy. Shared by the roll path and
// the final Close flush.
func (p *Parquet) uploadLocalFileToS3(key, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %s", filePath, err)
	}
	defer file.Close()

	if _, err = p.s3Uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(p.config.Bucket),
		Key:    aws.String(key),
		Body:   file,
	}); err != nil {
		return fmt.Errorf("failed to put object into s3 (%s): %s", key, err)
	}

	// Remove local file after successful upload (best-effort; a leftover file is not fatal).
	if rerr := os.Remove(filePath); rerr != nil {
		logger.Warnf("Thread[%s]: Failed to delete file [%s] after S3 upload: %s", p.options.ThreadID, filePath, rerr)
	}
	logger.Infof("Thread[%s]: successfully uploaded file to S3: s3://%s/%s", p.options.ThreadID, p.config.Bucket, key)
	return nil
}

func (p *Parquet) closePqFiles(ctx context.Context, _ any, closeOnError bool) error {
	removeLocalFile := func(filePath, reason string) {
		err := os.Remove(filePath)
		if err != nil {
			logger.Warnf("Thread[%s]: Failed to delete file [%s], reason (%s): %s", p.options.ThreadID, filePath, reason, err)
			return
		}
		logger.Debugf("Thread[%s]: Deleted file [%s], reason (%s).", p.options.ThreadID, filePath, reason)
	}

	// Struct to hold file upload info
	type uploadInfo struct {
		filePath  string
		s3KeyPath string
	}

	var filesToUpload []uploadInfo

	for basePath, parquetFiles := range p.partitionedFiles {
		for _, parquetFile := range parquetFiles {
			// construct full file path
			filePath := filepath.Join(p.config.Path, basePath, parquetFile.fileName)

			// Files sealed by a roll are already closed (file == nil); only close the still-open
			// (most recent) file of each partition here.
			if parquetFile.file != nil {
				if err := parquetFile.writer.Close(); err != nil {
					return fmt.Errorf("failed to close writer: %s", err)
				}
				if err := parquetFile.file.Close(); err != nil {
					return fmt.Errorf("failed to close file: %s", err)
				}
			}

			logger.Infof("Thread[%s]: Finished writing file [%s].", p.options.ThreadID, filePath)

			// close after closing writers
			if closeOnError {
				removeLocalFile(filePath, "closing parquet files due to retry attempt")
				continue
			}

			if p.s3Client != nil {
				filesToUpload = append(filesToUpload, uploadInfo{
					filePath:  filePath,
					s3KeyPath: p.s3KeyFor(basePath, parquetFile.fileName),
				})
			}
		}
	}

	if len(filesToUpload) > 0 && p.s3Client != nil {
		concurrency := min(runtime.GOMAXPROCS(0)*2, len(filesToUpload))

		err := utils.Concurrent(ctx, filesToUpload, concurrency, func(_ context.Context, info uploadInfo, _ int) error {
			return p.uploadLocalFileToS3(info.s3KeyPath, info.filePath)
		})
		if err != nil {
			return err
		}
	}

	// make map empty
	p.partitionedFiles = make(map[string][]*FileMetadata)
	return nil
}

func (p *Parquet) Close(ctx context.Context, finalMetadataState any) error {
	// TODO: implement 2pc in parquet writer (difficulty: hard)
	return p.closePqFiles(ctx, finalMetadataState, ctx.Err() != nil)
}

// validate schema change & evolution and removes null records
func (p *Parquet) FlattenAndCleanData(ctx context.Context, records []types.RawRecord) (bool, []types.RawRecord, any, error) {
	if !p.stream.NormalizationEnabled() {
		return false, records, nil, nil
	}

	if len(records) == 0 {
		return false, records, p.schema, nil
	}

	diffFound := atomic.Bool{} // to process records concurrently and detect schema difference

	// One flattener per batch: the internal cache amortizes resolve calls
	// across all records so each column name is resolved only once.
	batchFlattener := typeutils.NewFlattener(p.stream.ResolveColumnName)
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
				if _, columnExist := p.schema[columnName]; !columnExist {
					diffFound.Store(true)
					break
				}

				persistedTypes := p.schema[columnName].Types()
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
			change, typeChange, _ := p.schema.Process(record.Data)
			schemaChange = change || typeChange || schemaChange
		}
	}

	if err := utils.Concurrent(ctx, records, runtime.GOMAXPROCS(0)*16, func(_ context.Context, record types.RawRecord, _ int) error {
		return typeutils.ReformatRecord(p.schema, record.Data)
	}); err != nil {
		return false, nil, nil, fmt.Errorf("failed to reformat records: %s", err)
	}
	if p.options.ApplyFilter {
		filter, isLegacy, filterErr := p.stream.GetFilter()
		if filterErr != nil {
			return false, nil, nil, fmt.Errorf("failed to parse stream filter: %s", filterErr)
		}
		records, err = typeutils.FilterRecords(ctx, records, filter, isLegacy, p.schema, p.stream.ResolveColumnName)
		if err != nil {
			return false, nil, nil, fmt.Errorf("failed to filter records: %s", err)
		}
	}
	return schemaChange, records, p.schema, nil
}

// EvolveSchema updates the schema based on changes. Need to pass olakeTimestamp to get the correct partition path based on record ingestion time.
func (p *Parquet) EvolveSchema(_ context.Context, _, _ any) (any, error) {
	if !p.stream.NormalizationEnabled() {
		return false, nil
	}

	logger.Infof("Thread[%s]: schema evolution detected", p.options.ThreadID)

	// create new partition files for all paths as prev are of no use
	for path := range p.partitionedFiles {
		err := p.createNewPartitionFile(path)
		if err != nil {
			return nil, fmt.Errorf("failed to create new partition file: %s", err)
		}
	}

	// TODO: can we implement something https://github.com/parquet-go/parquet-go?tab=readme-ov-file#evolving-parquet-schemas-parquetconvert
	// close prev files as change detected (new files will be created with new schema)
	return p.schema.Clone(), nil
}

// Type returns the type of the writer.
func (p *Parquet) Type() string {
	return string(types.Parquet)
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
					logger.Debugf("Thread[%s]: failed to convert value to timestamp: %s", p.options.ThreadID, err)
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
		resolvedColName := p.stream.ResolveColumnName(colName)
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
		return p.basePath
	}
	return filepath.Join(p.basePath, strings.TrimSuffix(result, "/"))
}

func (p *Parquet) DropStreams(ctx context.Context, selectedStreams []types.StreamInterface) error {
	// check for s3 writer configuration
	err := p.initS3Writer()
	if err != nil {
		return err
	}

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

func init() {
	destination.RegisteredWriters[types.Parquet] = func() destination.Writer {
		return new(Parquet)
	}
}
