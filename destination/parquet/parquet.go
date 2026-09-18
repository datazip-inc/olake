package parquet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/errs"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
	pqgo "github.com/parquet-go/parquet-go"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/source"
)

const parquetTempDirPattern = "olake-parquet-*"

type FileMetadata struct {
	writer       *pqgo.GenericWriter[any]
	file         source.ParquetFile
	path         string
	relativePath string
}

// Parquet destination writes Parquet files to a local path and optionally uploads them to S3 or Azure.
type Parquet struct {
	options          *destination.Options
	config           *Config
	stream           types.StreamInterface
	basePath         string                     // construct with streamNamespace/streamName
	partitionedFiles map[string][]*FileMetadata // mapping of basePath/{regex} -> pqFiles

	store ObjectStore // S3 or Azure Blob Storage

	tempDir string
	schema  typeutils.Fields

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

// initStore initializes the remote object store(S3 or Azure Blob Storage).
func (p *Parquet) initStore() error {
	if p.store != nil {
		return nil
	}
	if !p.config.usingAzure() && p.config.Bucket == "" && p.config.Region == "" {
		return nil
	}
	store, err := newObjectStore(p.config)
	if err != nil {
		return fmt.Errorf("failed to create session: %w", err)
	}
	p.store = store
	return nil
}

func (p *Parquet) createNewPartitionFile(basePath string) error {
	relativeDir, err := filepath.Rel(p.basePath, basePath)
	if err != nil {
		return fmt.Errorf("failed to get relative parquet partition path: %s", err)
	}

	directoryPath := filepath.Join(p.config.Path, basePath)
	if p.store != nil {
		if p.tempDir == "" {
			if err := os.MkdirAll(p.config.Path, os.ModePerm); err != nil {
				return fmt.Errorf("failed to create parquet temp path[%s]: %s", p.config.Path, err)
			}
			p.tempDir, err = os.MkdirTemp(p.config.Path, parquetTempDirPattern)
			if err != nil {
				return fmt.Errorf("failed to create parquet temp directory: %s", err)
			}
		}
		directoryPath = filepath.Join(p.tempDir, relativeDir)
	}

	if err := os.MkdirAll(directoryPath, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create directories[%s]: %w", directoryPath, err)
	}

	fileName := utils.TimestampedFileName(constants.ParquetFileExt)
	filePath := filepath.Join(directoryPath, fileName)

	pqFile, err := local.NewLocalFileWriter(filePath)
	if err != nil {
		return destination.WriteFailure(fmt.Errorf("failed to create parquet file writer: %w", err))
	}

	writer := func() *pqgo.GenericWriter[any] {
		if p.stream.NormalizationEnabled() {
			return pqgo.NewGenericWriter[any](pqFile, p.schema.ToTypeSchema().ToParquet(false, p.stream), pqgo.Compression(&pqgo.Snappy))
		}
		return pqgo.NewGenericWriter[any](pqFile, p.stream.Schema().ToParquet(true, p.stream), pqgo.Compression(&pqgo.Snappy))
	}()

	p.partitionedFiles[basePath] = append(p.partitionedFiles[basePath], &FileMetadata{
		writer:       writer,
		file:         pqFile,
		path:         filePath,
		relativePath: filepath.ToSlash(filepath.Join(relativeDir, fileName)),
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
			return nil, fmt.Errorf("failed to create partition file: %w", err)
		}
		files = p.partitionedFiles[basePath]
	}
	return files[len(files)-1], nil
}

// Setup configures the parquet writer, including local paths, file names, and optional remote store.
func (p *Parquet) Setup(ctx context.Context, stream types.StreamInterface, schema any, options *destination.Options) (any, *types.MetadataState, error) {
	p.options = options
	p.stream = stream
	p.partitionedFiles = make(map[string][]*FileMetadata)
	p.basePath = filepath.Join(p.stream.GetDestinationDatabase(nil), p.stream.GetDestinationTable())
	p.schema = make(typeutils.Fields)

	maxFileSizeMB := float64(defaultMaxFileSizeMB)
	if p.config.MaxFileSizeMB > 0 {
		maxFileSizeMB = p.config.MaxFileSizeMB
	}
	p.maxFileBytes = int64(maxFileSizeMB * 1024 * 1024)

	if p.checkIntervalForRoll == 0 {
		p.checkIntervalForRoll = defaultRollCheckInterval
	}

	// remote writers may omit local_path
	if p.config.Path == "" {
		p.config.Path = os.TempDir()
	}

	err := p.initStore()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to initialize store: %w", err)
	}

	var prevMetadataState *types.MetadataState
	if p.store != nil {
		prevMetadataState, err = p.load2PCState(ctx)
		if err != nil {
			return nil, nil, err
		}
	}

	if !p.stream.NormalizationEnabled() {
		return p.schema, prevMetadataState, nil
	}

	if schema != nil {
		fields, ok := schema.(typeutils.Fields)
		if !ok {
			return nil, nil, fmt.Errorf("failed to typecast schema[%T] into typeutils.Fields", schema)
		}
		p.schema = fields.Clone()
		return fields, prevMetadataState, nil
	}
	fields := make(typeutils.Fields)
	fields.FromSchema(stream.Schema(), stream.ResolveColumnName)
	p.schema = fields.Clone()
	return fields, prevMetadataState, nil
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
				return destination.WriteFailure(fmt.Errorf("failed to marshal data: %w", merr))
			}
			recordsMap := map[string]any{constants.StringifiedData: string(dataBytes)}
			maps.Copy(recordsMap, record.OlakeColumns)

			_, err = partitionFile.writer.Write([]any{recordsMap})
		}
		if err != nil {
			return destination.WriteFailure(fmt.Errorf("failed to write in parquet file: %w", err))
		}

		if p.checkForRoll(i, len(records)) {
			if err := p.rollPartitionFile(partitionFile); err != nil {
				return fmt.Errorf("failed to roll partition file: %w", err)
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
// remote object store (files exist only on local disk until then).
func (p *Parquet) rollPartitionFile(pf *FileMetadata) error {
	if pf.writer.Size() < p.maxFileBytes {
		return nil
	}

	// Threshold reached: write the footer to seal the file. It stays in partitionedFiles (with
	// file == nil marking it finalized) to be uploaded in Close.
	if err := pf.writer.Close(); err != nil {
		return destination.WriteFailure(fmt.Errorf("failed to close parquet writer on roll[%s]: %w", pf.path, err))
	}
	if err := pf.file.Close(); err != nil {
		return destination.WriteFailure(fmt.Errorf("failed to close parquet file on roll[%s]: %w", pf.path, err))
	}
	pf.file = nil // mark finalized; kept for upload at Close
	logger.Infof("Thread[%s]: rolled partition file[%s] at %d bytes", p.options.ThreadID, pf.path, pf.writer.Size())
	return nil
}

// Check validates local paths and remote object store if applicable.
func (p *Parquet) Check(ctx context.Context) error {
	uniqueSuffix := fmt.Sprintf("%d", time.Now().UnixNano())
	threadID := fmt.Sprintf("test_parquet_destination_%s", uniqueSuffix)

	p.options = &destination.Options{
		ThreadID: threadID,
	}

	if err := p.initStore(); err != nil {
		return err
	}

	var err error
	switch {
	case p.store != nil:
		testKey := p.store.ObjectKey(path.Join("olake_writer_test", utils.TimestampedFileName(".txt")))
		if err := p.store.Put(ctx, testKey, []byte("write test")); err != nil {
			return fmt.Errorf("failed to write test file to %s: %w", p.store.Kind(), err)
		}
		p.config.Path = os.TempDir()
		logger.Infof("Thread[%s]: %s writer configuration found", p.options.ThreadID, p.store.Kind())
	case p.config.Path != "":
		logger.Infof("Thread[%s]: local writer configuration found, writing at location[%s]", p.options.ThreadID, p.config.Path)
	default:
		return errs.Precondition(errs.ConfigInvalid, codeNoDestinationConfigured,
			fmt.Errorf("invalid configuration found"))
	}

	// Create the directory if it doesn't exist
	if err := os.MkdirAll(p.config.Path, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create path: %w", err)
	}

	// Test directory writability
	tempFile, err := os.CreateTemp(p.config.Path, "temporary-*.txt")
	if err != nil {
		return fmt.Errorf("directory is not writable: %w", err)
	}
	tempFile.Close()
	os.Remove(tempFile.Name())
	return nil
}

// pendingDataFiles returns files awaiting close and remote object store for this writer.
func (p *Parquet) pendingDataFiles() []*FileMetadata {
	var dataFiles []*FileMetadata
	for _, parquetFiles := range p.partitionedFiles {
		dataFiles = append(dataFiles, parquetFiles...)
	}
	return dataFiles
}

func (p *Parquet) closePqFiles(closeOnError bool) error {
	removeLocalFile := func(filePath, reason string) {
		err := os.Remove(filePath)
		if err != nil {
			logger.Warnf("Thread[%s]: Failed to delete file [%s], reason (%s): %s", p.options.ThreadID, filePath, reason, err)
			return
		}
		logger.Debugf("Thread[%s]: Deleted file [%s], reason (%s).", p.options.ThreadID, filePath, reason)
	}

	for _, parquetFiles := range p.partitionedFiles {
		for _, parquetFile := range parquetFiles {
			if parquetFile.file != nil {
				if err := parquetFile.writer.Close(); err != nil {
					return destination.WriteFailure(fmt.Errorf("failed to close writer: %w", err))
				}
				if err := parquetFile.file.Close(); err != nil {
					return destination.WriteFailure(fmt.Errorf("failed to close file: %w", err))
				}
				parquetFile.file = nil
			}

			logger.Infof("Thread[%s]: Finished writing file [%s].", p.options.ThreadID, parquetFile.path)

			if closeOnError {
				removeLocalFile(parquetFile.path, "closing parquet files due to retry attempt")
				continue
			}
		}
	}

	return nil
}

func (p *Parquet) uploadPqFiles(ctx context.Context, dataFiles []*FileMetadata) error {
	if len(dataFiles) == 0 {
		return nil
	}

	concurrency := min(runtime.GOMAXPROCS(0)*2, len(dataFiles))
	return utils.Concurrent(ctx, dataFiles, concurrency, func(uploadCtx context.Context, info *FileMetadata, _ int) error {
		stagingKey := p.stagingObjectKey(info.relativePath)

		err := p.retryRemote(uploadCtx, func(retryCtx context.Context) error {
			file, err := os.Open(info.path)
			if err != nil {
				return fmt.Errorf("failed to open file %s: %s", info.path, err)
			}
			defer file.Close()
			if err := p.store.UploadFile(retryCtx, stagingKey, file); err != nil {
				return fmt.Errorf("failed to upload file to %s (%s): %w", p.store.Kind(), stagingKey, err)
			}
			return nil
		})

		if err != nil {
			return err
		}

		if err := os.Remove(info.path); err != nil {
			logger.Warnf("Thread[%s]: Failed to delete file [%s], reason (uploaded to %s): %s", p.options.ThreadID, info.path, p.store.Kind(), err)
		}
		logger.Infof("Thread[%s]: successfully uploaded file to /%s : /%s", p.options.ThreadID, p.store.Kind(), stagingKey)
		return nil
	})
}

func (p *Parquet) Close(ctx context.Context, finalMetadataState any) error {
	if p.store == nil {
		// TODO: add 2PC support for local Parquet destinations.
		if err := p.closePqFiles(ctx.Err() != nil); err != nil {
			return err
		}
		p.partitionedFiles = make(map[string][]*FileMetadata)
		return nil
	}

	defer func() {
		if p.tempDir == "" {
			return
		}
		if err := os.RemoveAll(p.tempDir); err != nil {
			logger.Warnf("Thread[%s]: failed to delete parquet temp directory[%s]: %s", p.options.ThreadID, p.tempDir, err)
		}
	}()

	dataFiles := p.pendingDataFiles()
	if !p.options.Backfill && len(dataFiles) == 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		return nil
	}

	var finishData []byte
	var metadataState types.MetadataState
	var err error
	if p.options.Backfill {
		finishData, err = backfillFinishState(p.options.ThreadID)
	} else {
		finishData, metadataState, err = streamFinishState(finalMetadataState)
	}
	if err != nil {
		if closeErr := p.closePqFiles(true); closeErr != nil {
			return fmt.Errorf("%w: failed to close parquet files: %s", err, closeErr)
		}
		return err
	}

	if err := p.closePqFiles(ctx.Err() != nil); err != nil {
		return err
	}
	p.partitionedFiles = make(map[string][]*FileMetadata)
	if err := ctx.Err(); err != nil {
		return err
	}

	// Resolve staging left by an earlier serialized writer before reusing the shared prefix.
	if err := p.recoverStaging(ctx); err != nil {
		return err
	}
	if err := p.uploadPqFiles(ctx, dataFiles); err != nil {
		return err
	}
	if err := p.writeFinish(ctx, finishData); err != nil {
		return err
	}
	if p.options.Backfill {
		return p.finalizeBackfillStaging(ctx, p.currentStagingPrefix(), p.options.ThreadID)
	}
	return p.finalizeStreamStaging(ctx, p.currentStagingPrefix(), metadataState)
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
			return fmt.Errorf("failed to flatten record at index %d, pq writer: %w", idx, err)
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
		return false, nil, nil, fmt.Errorf("failed to process records: %w", err)
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
		return false, nil, nil, fmt.Errorf("failed to reformat records: %w", err)
	}
	if p.options.ApplyFilter {
		filter, isLegacy, filterErr := p.stream.GetFilter()
		if filterErr != nil {
			return false, nil, nil, fmt.Errorf("failed to parse stream filter: %w", filterErr)
		}
		records, err = typeutils.FilterRecords(ctx, records, filter, isLegacy, p.schema, p.stream.ResolveColumnName)
		if err != nil {
			return false, nil, nil, fmt.Errorf("failed to filter records: %w", err)
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
			return nil, fmt.Errorf("failed to create new partition file: %w", err)
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
	// check for remote object store writer configuration
	if err := p.initStore(); err != nil {
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

	if p.store == nil {
		if err := p.clearLocalFiles(paths); err != nil {
			return fmt.Errorf("failed to clear local files: %w", err)
		}
	} else {
		if err := p.clearRemoteFiles(ctx, paths); err != nil {
			return fmt.Errorf("failed to clear %s files: %w", p.store.Kind(), err)
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
			return fmt.Errorf("failed to remove local path %s: %w", streamPath, err)
		}
	}

	return nil
}

// isRateLimitError checks if the error is a rate-limit/throttle response from remote object store.
// AWS S3 returns HTTP 503 for throttling (SlowDown / ServiceUnavailable).
// GCP Cloud Storage returns HTTP 429 (Too Many Requests).
// Azure Blob Storage returns HTTP 429 (Too Many Requests).
//
// For batch delete operations, errors are wrapped in s3manager.BatchError which does NOT
// implement awserr.RequestFailure directly. The actual RequestFailure is nested inside
// BatchError.Errors[].OrigErr, so we must inspect those inner errors as well.
func isRateLimitError(err error) bool {
	isThrottled := func(target error) bool {
		var rf awserr.RequestFailure
		if errors.As(target, &rf) && (rf.StatusCode() == 429 || rf.StatusCode() == 503) {
			return true
		}
		var respErr *azcore.ResponseError
		return errors.As(target, &respErr) && (respErr.StatusCode == 429 || respErr.StatusCode == 503)
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

func (p *Parquet) clearRemoteFiles(ctx context.Context, paths []string) error {
	for _, streamID := range paths {
		parts := strings.SplitN(streamID, ".", 2)
		if len(parts) != 2 {
			logger.Warnf("invalid stream ID format: %s, skipping", streamID)
			continue
		}
		prefix := p.store.ObjectKey(path.Join(parts[0], parts[1])) + "/"
		logger.Debugf("clearing %s prefix: %s", p.store.Kind(), prefix)
		if err := p.retryRemote(ctx, func(ctx context.Context) error {
			return p.store.DeletePrefix(ctx, prefix)
		}); err != nil {
			return fmt.Errorf("failed to clear %s prefix %s: %w", p.store.Kind(), prefix, err)
		}
		logger.Debugf("successfully cleared %s prefix: %s", p.store.Kind(), prefix)
	}
	return nil
}

func init() {
	var parquetConfig *Config
	destination.RegisteredWriters[types.Parquet] = func(config any) (destination.Writer, func(ctx context.Context), error) {
		if parquetConfig != nil {
			// for already initialized writer, return the same config instance
			return &Parquet{
				config: parquetConfig,
			}, nil, nil
		}

		parquetConfig = &Config{}
		err := utils.Unmarshal(config, parquetConfig)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to unmarshal parquet config: %w", err)
		}

		if err := parquetConfig.Validate(); err != nil {
			return nil, nil, fmt.Errorf("failed to validate parquet config: %w", err)
		}

		return &Parquet{
			config: parquetConfig,
		}, nil, nil
	}
}
