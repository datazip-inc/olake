package parquet

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"sync"

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
	fileName string
	writer   any
	file     source.ParquetFile
}

// Parquet destination writes Parquet files to a local path and optionally uploads them to S3.
type Parquet struct {
	options          *destination.Options
	config           *Config
	stream           types.StreamInterface
	basePath         string                   // construct with streamNamespace/streamName
	partitionedFiles map[string]*FileMetadata // mapping of basePath/{regex} -> pqFiles
	s3Client         *s3.S3
	schema           typeutils.Fields
	mu               sync.Mutex
	lastFlushAt      time.Time
	targetBytes      int64
	maxLatency       time.Duration
	flushTicker      *time.Ticker
	stopCh           chan struct{}
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
			return pqgo.NewGenericWriter[any](pqFile, p.schema.ToTypeSchema().ToParquet(), pqgo.Compression(&pqgo.Snappy))
		}
		return pqgo.NewGenericWriter[types.RawRecord](pqFile, pqgo.Compression(&pqgo.Snappy))
	}()

	p.partitionedFiles[basePath] = &FileMetadata{
		fileName: fileName,
		file:     pqFile,
		writer:   writer,
	}

	logger.Infof("Thread[%s]: created new partition file[%s]", p.options.ThreadID, filePath)
	return nil
}

// Setup configures the parquet writer, including local paths, file names, and optional S3 setup.
func (p *Parquet) Setup(_ context.Context, stream types.StreamInterface, schema any, options *destination.Options) (any, error) {
	p.options = options
	p.stream = stream
	p.partitionedFiles = make(map[string]*FileMetadata)
	p.basePath = filepath.Join(p.stream.GetDestinationDatabase(nil), p.stream.GetDestinationTable())
	p.schema = make(typeutils.Fields)
	p.stopCh = make(chan struct{})

	// for s3 p.config.path may not be provided
	if p.config.Path == "" {
		p.config.Path = os.TempDir()
	}

	err := p.initS3Writer()
	if err != nil {
		return nil, err
	}

	// configure continuous thresholds and start background flusher if enabled
	// Note: target_file_size is provided in KB; convert to bytes
	if p.config.TargetFileSizeMB > 0 {
		p.targetBytes = int64(p.config.TargetFileSizeMB) * 1024
	}
	if p.config.MaxLatencySeconds > 0 {
		p.maxLatency = time.Duration(p.config.MaxLatencySeconds) * time.Second
	}
	if p.targetBytes > 0 || p.maxLatency > 0 {
		p.lastFlushAt = time.Now()
		p.flushTicker = time.NewTicker(time.Second)
		go p.backgroundFlusher()
		logger.Infof("Thread[%s]: background flusher started (target_bytes=%d, max_latency=%s)", p.options.ThreadID, p.targetBytes, p.maxLatency)
	}

	if !p.stream.NormalizationEnabled() {
		return p.schema, nil
	}

	if schema != nil {
		fields, ok := schema.(typeutils.Fields)
		if !ok {
			return nil, fmt.Errorf("failed to typecast schema[%T] into typeutils.Fields", schema)
		}
		p.schema = fields.Clone()
		return fields, nil
	}

	fields := make(typeutils.Fields)
	fields.FromSchema(stream.Schema())
	p.schema = fields.Clone() // update schema
	return fields, nil
}

// Write writes a record to the Parquet file.
func (p *Parquet) Write(_ context.Context, records []types.RawRecord) error {
	// TODO: use batch writing feature of pq writer
	for _, record := range records {
		record.OlakeTimestamp = time.Now().UTC()
		partitionedPath := p.getPartitionedFilePath(record.Data, record.OlakeTimestamp)
		p.mu.Lock()
		partitionFile, exists := p.partitionedFiles[partitionedPath]
		if !exists {
			err := p.createNewPartitionFile(partitionedPath)
			if err != nil {
				p.mu.Unlock()
				return fmt.Errorf("failed to create parititon file: %s", err)
			}
			partitionFile = p.partitionedFiles[partitionedPath]
		}

		if partitionFile == nil {
			p.mu.Unlock()
			return fmt.Errorf("failed to create partition file for path[%s]", partitionedPath)
		}

		var err error
		if p.stream.NormalizationEnabled() {
			_, err = partitionFile.writer.(*pqgo.GenericWriter[any]).Write([]any{record.Data})
		} else {
			_, err = partitionFile.writer.(*pqgo.GenericWriter[types.RawRecord]).Write([]types.RawRecord{record})
		}
		p.mu.Unlock()
		if err != nil {
			return fmt.Errorf("failed to write in parquet file: %s", err)
		}
	}

	return nil
}

// Check validates local paths and S3 credentials if applicable.
func (p *Parquet) Check(_ context.Context) error {
	p.options = &destination.Options{
		ThreadID: "test_parquet_destination",
	}

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

func (p *Parquet) closePqFiles() error {
	removeLocalFile := func(filePath, reason string) {
		err := os.Remove(filePath)
		if err != nil {
			logger.Warnf("Thread[%s]: Failed to delete file [%s], reason (%s): %s", p.options.ThreadID, filePath, reason, err)
			return
		}
		logger.Debugf("Thread[%s]: Deleted file [%s], reason (%s).", p.options.ThreadID, filePath, reason)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	for basePath, parquetFile := range p.partitionedFiles {
		// construct full file path
		filePath := filepath.Join(p.config.Path, basePath, parquetFile.fileName)

		// Close writers
		var err error
		if p.stream.NormalizationEnabled() {
			err = parquetFile.writer.(*pqgo.GenericWriter[any]).Close()
		} else {
			err = parquetFile.writer.(*pqgo.GenericWriter[types.RawRecord]).Close()
		}
		if err != nil {
			logger.Warnf("Thread[%s]: Failed to close parquet writer: %s", p.options.ThreadID, err)
		}

		// Close file
		if err := parquetFile.file.Close(); err != nil {
			return fmt.Errorf("failed to close file: %s", err)
		}

		logger.Infof("Thread[%s]: Finished writing file [%s].", p.options.ThreadID, filePath)

		if p.s3Client != nil {
			// Open file for S3 upload
			file, err := os.Open(filePath)
			if err != nil {
				return fmt.Errorf("failed to open file: %s", err)
			}
			defer file.Close()

			// Construct S3 key path
			s3KeyPath := basePath
			if p.config.Prefix != "" {
				s3KeyPath = filepath.Join(p.config.Prefix, s3KeyPath)
			}
			s3KeyPath = filepath.Join(s3KeyPath, parquetFile.fileName)

			// Upload to S3
			_, err = p.s3Client.PutObject(&s3.PutObjectInput{
				Bucket: aws.String(p.config.Bucket),
				Key:    aws.String(s3KeyPath),
				Body:   file,
			})
			if err != nil {
				return fmt.Errorf("failed to put object into s3: %s", err)
			}

			// Remove local file after successful upload
			removeLocalFile(filePath, "uploaded to S3")
			logger.Infof("Thread[%s]: successfully uploaded file to S3: s3://%s/%s", p.options.ThreadID, p.config.Bucket, s3KeyPath)
		}
	}
	// make map empty
	// p.partitionedFiles = make(map[string]*FileMetadata)
	for k := range p.partitionedFiles {
		delete(p.partitionedFiles, k)
	}
	return nil
}

func (p *Parquet) Close(_ context.Context) error {
	if p.flushTicker != nil {
		p.flushTicker.Stop()
	}
	if p.stopCh != nil {
		close(p.stopCh)
	}
	return p.closePqFiles()
}

// validate schema change & evolution and removes null records
func (p *Parquet) FlattenAndCleanData(records []types.RawRecord) (bool, []types.RawRecord, any, error) {
	if !p.stream.NormalizationEnabled() {
		return false, records, nil, nil
	}

	schemaChange := false
	for idx, record := range records {
		// add common fields
		records[idx].Data[constants.OlakeID] = record.OlakeID
		records[idx].Data[constants.OlakeTimestamp] = time.Now().UTC()
		records[idx].Data[constants.OpType] = record.OperationType
		if record.CdcTimestamp != nil {
			records[idx].Data[constants.CdcTimestamp] = *record.CdcTimestamp
		}

		flattenedRecord, err := typeutils.NewFlattener().Flatten(record.Data)
		if err != nil {
			return false, nil, nil, fmt.Errorf("failed to flatten record, pq writer: %s", err)
		}

		// just process the changes and upgrade new schema
		change, typeChange, _ := p.schema.Process(flattenedRecord)
		schemaChange = change || typeChange || schemaChange
		err = typeutils.ReformatRecord(p.schema, flattenedRecord)
		if err != nil {
			return false, nil, nil, fmt.Errorf("failed to reformat records: %s", err)
		}
		records[idx].Data = flattenedRecord // use idx to update slice record
	}

	return schemaChange, records, p.schema, nil
}

// EvolveSchema updates the schema based on changes. Need to pass olakeTimestamp to get the correct partition path based on record ingestion time.
func (p *Parquet) EvolveSchema(_ context.Context, _, _ any) (any, error) {
	if !p.stream.NormalizationEnabled() {
		return false, nil
	}

	logger.Infof("Thread[%s]: schema evolution detected", p.options.ThreadID)

	// TODO: can we implement something https://github.com/parquet-go/parquet-go?tab=readme-ov-file#evolving-parquet-schemas-parquetconvert
	// close prev files as change detected (new files will be created with new schema)
	return p.schema.Clone(), p.closePqFiles()
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
		value, exists := values[colName]
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

func (p *Parquet) DropStreams(ctx context.Context, selectedStreams []string) error {
	if len(selectedStreams) == 0 {
		logger.Infof("Thread[%s]: no streams selected for clearing, skipping clear operation", p.options.ThreadID)
		return nil
	}

	logger.Infof("Thread[%s]: clearing destination for %d selected streams: %v", p.options.ThreadID, len(selectedStreams), selectedStreams)

	if p.s3Client == nil {
		if err := p.clearLocalFiles(selectedStreams); err != nil {
			return fmt.Errorf("failed to clear local files: %s", err)
		}
	} else {
		if err := p.clearS3Files(ctx, selectedStreams); err != nil {
			return fmt.Errorf("failed to clear S3 files: %s", err)
		}
	}

	logger.Infof("Thread[%s]: successfully cleared destination for selected streams", p.options.ThreadID)
	return nil
}

func (p *Parquet) clearLocalFiles(selectedStreams []string) error {
	for _, streamID := range selectedStreams {
		parts := strings.SplitN(streamID, ".", 2)
		if len(parts) != 2 {
			logger.Warnf("Thread[%s]: invalid stream ID format: %s, skipping", p.options.ThreadID, streamID)
			continue
		}

		namespace, streamName := parts[0], parts[1]
		streamPath := filepath.Join(p.config.Path, namespace, streamName)

		logger.Infof("Thread[%s]: clearing local path: %s", p.options.ThreadID, streamPath)

		if _, err := os.Stat(streamPath); os.IsNotExist(err) {
			logger.Debugf("Thread[%s]: local path does not exist, skipping: %s", p.options.ThreadID, streamPath)
			continue
		}

		if err := os.RemoveAll(streamPath); err != nil {
			return fmt.Errorf("failed to remove local path %s: %s", streamPath, err)
		}

		logger.Debugf("Thread[%s]: successfully cleared local path: %s", p.options.ThreadID, streamPath)
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
			logger.Warnf("Thread[%s]: invalid stream ID format: %s, skipping", p.options.ThreadID, streamID)
			continue
		}

		namespace, streamName := parts[0], parts[1]
		s3TablePath := filepath.Join(p.config.Prefix, namespace, streamName, "/")
		logger.Debugf("Thread[%s]: clearing S3 prefix: s3://%s/%s", p.options.ThreadID, p.config.Bucket, s3TablePath)
		if err := deleteS3PrefixStandard(s3TablePath); err != nil {
			return fmt.Errorf("failed to clear S3 prefix %s: %s", s3TablePath, err)
		}

		logger.Debugf("Thread[%s]: successfully cleared S3 prefix: s3://%s/%s", p.options.ThreadID, p.config.Bucket, s3TablePath)
	}
	return nil
}

func init() {
	destination.RegisteredWriters[types.Parquet] = func() destination.Writer {
		return new(Parquet)
	}
}

// backgroundFlusher periodically checks size and latency thresholds to rotate files.
// func (p *Parquet) backgroundFlusher() {
// 	for {
// 		select {
// 		case <-p.stopCh:
// 			return
// 		case <-p.flushTicker.C:
// 			shouldFlush := false
// 			if p.maxLatency > 0 && time.Since(p.lastFlushAt) >= p.maxLatency {
// 				shouldFlush = true
// 			}
// 			if !shouldFlush && p.targetBytes > 0 {
// 				p.mu.Lock()
// 				for basePath, parquetFile := range p.partitionedFiles {
// 					filePath := filepath.Join(p.config.Path, basePath, parquetFile.fileName)
// 					info, err := os.Stat(filePath)
// 					if err == nil && info.Size() >= p.targetBytes {
// 						shouldFlush = true
// 						break
// 					}
// 				}
// 				p.mu.Unlock()
// 			}
// 			if shouldFlush {
// 				if err := p.closePqFiles(); err != nil {
// 					logger.Warnf("Thread[%s]: failed to flush parquet files: %s", p.options.ThreadID, err)
// 					continue
// 				}
// 				p.lastFlushAt = time.Now()
// 			}
// 		}
// 	}
// }

func (p *Parquet) backgroundFlusher() {
	for {
		select {
		case <-p.stopCh:
			return
		case <-p.flushTicker.C:
			shouldFlush := false
			if p.maxLatency > 0 && time.Since(p.lastFlushAt) >= p.maxLatency {
				shouldFlush = true
			}
			if !shouldFlush && p.targetBytes > 0 {
				p.mu.Lock()
				for basePath, parquetFile := range p.partitionedFiles {
					filePath := filepath.Join(p.config.Path, basePath, parquetFile.fileName)
					info, err := os.Stat(filePath)
					if err == nil && info.Size() >= p.targetBytes {
						shouldFlush = true
						logger.Debugf("Thread[%s]: File size threshold reached (%d >= %d), should flush",
							p.options.ThreadID, info.Size(), p.targetBytes)
						break
					}
				}
				p.mu.Unlock()
			}
			if shouldFlush {
				logger.Infof("Thread[%s]: Flushing parquet files", p.options.ThreadID)
				if err := p.closePqFiles(); err != nil {
					logger.Warnf("Thread[%s]: failed to flush parquet files: %s", p.options.ThreadID, err)
					continue
				}
				p.lastFlushAt = time.Now()
				logger.Infof("Thread[%s]: background flusher rotated files", p.options.ThreadID)
			}
		}
	}
}
