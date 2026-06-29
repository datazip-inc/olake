package parquet

import (
	"context"
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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/datazip-inc/olake/writers"
	"github.com/datazip-inc/olake/writers/encoder/parquetenc"
	"github.com/datazip-inc/olake/writers/roller"
	"github.com/datazip-inc/olake/writers/sink"
	"github.com/datazip-inc/olake/writers/sink/localfs"
	s3sink "github.com/datazip-inc/olake/writers/sink/s3"
)

// Parquet destination writes Parquet files to a local path and optionally uploads them to S3.
type Parquet struct {
	options *destination.Options
	config  *s3sink.Config
	schema  typeutils.Fields
	stream  types.StreamInterface

	basePath string
	s3Sink   *s3sink.S3Sink

	rollerCfg   roller.RollerConfig
	generations []*schemaGen
	// writeFailed makes Close abort instead of publish if any write/flush errored.
	// Without it, a write failure unobserved before Close (live ctx) would publish
	// partial data and then the chunk retries -> duplicates.
	writeFailed atomic.Bool
}

type schemaGen struct {
	prw writers.PartitionedRollingWriter
}

// GetConfigRef returns the config reference for the parquet writer.
func (p *Parquet) GetConfigRef() destination.Config {
	p.config = &s3sink.Config{}
	return p.config
}

// Spec returns a new Config instance.
func (p *Parquet) Spec() any {
	return s3sink.Config{}
}

// Write normalizes CDC op-types then routes the batch through the partitioned
// rolling writer, which fans the per-partition writing out concurrently.
func (p *Parquet) Write(ctx context.Context, records []types.RawRecord) error {
	// Normalise "i" -> "c": Parquet has no equality-delete concept; downstream
	// consumers must see a consistent "c" for all CDC inserts. OlakeColumns covers the
	// non-normalized path; Data covers the normalized path where FlattenAndCleanData
	// already merged OlakeColumns into Data. Done before routing so the partitioner
	// sees the same state the legacy inline loop did.
	for i := range records {
		if opType, ok := records[i].OlakeColumns[constants.OpType].(string); ok && opType == "i" {
			records[i].OlakeColumns[constants.OpType] = "c"
			if _, exists := records[i].Data[constants.OpType]; exists {
				records[i].Data[constants.OpType] = "c"
			}
		}
	}

	if err := p.active().Write(ctx, records); err != nil {
		// Remember the failure so Close aborts (not publishes): the WriterThread retries
		// the chunk, and publishing the partial files here would duplicate those rows.
		p.writeFailed.Store(true)
		return fmt.Errorf("failed to write records: %w", err)
	}

	return nil
}

// Check validates local paths and S3 credentials if applicable.
func (p *Parquet) Check(_ context.Context) error {
	uniqueSuffix := fmt.Sprintf("%d", time.Now().UnixNano())
	threadID := fmt.Sprintf("test_parquet_destination_%s", uniqueSuffix)

	p.options = &destination.Options{
		ThreadID: threadID,
	}

	// test for s3 permissions
	if p.s3Sink != nil {
		testKey := filepath.Join(p.config.Prefix, "olake_writer_test", utils.TimestampedFileName(".txt"))
		// Try to upload a small test file
		err := p.s3Sink.PutObject(testKey, strings.NewReader("S3 write test"))
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

// Close flushes the active generation's trailing files (sealed generations were
// already flushed in EvolveSchema), then runs two-phase commit across every
// generation: commit on success, or abort on cancellation / a flush error / an
// earlier write failure. This is the real 2PC the legacy writer only had a TODO for.
func (p *Parquet) Close(ctx context.Context, _ any) error {
	flushErr := p.active().Close(ctx)

	if p.writeFailed.Load() || ctx.Err() != nil || flushErr != nil {
		cleanupCtx := context.WithoutCancel(ctx)
		for _, g := range p.generations {
			_ = g.prw.Abort(cleanupCtx)
		}
		return flushErr
	}

	for i, g := range p.generations {
		if err := g.prw.Commit(ctx); err != nil {
			// A commit failed mid-way. Abort the generations that have NOT committed
			// (this one included) to reclaim their staged files. Already-committed
			// generations are durable and left untouched — Abort would be a no-op on
			// them anyway, since Commit clears each sink's staged list on success.
			cleanupCtx := context.WithoutCancel(ctx)
			for _, g2 := range p.generations[i:] {
				_ = g2.prw.Abort(cleanupCtx)
			}
			return fmt.Errorf("failed to commit generation %d: %w", i, err)
		}
	}
	return nil
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

// EvolveSchema seals the current schema generation (flush + stage its trailing
// files) and starts a new one bound to the just-updated p.schema. Old-schema files
// are kept and published alongside new-schema files at Close — matching the legacy
// "new files for the new schema" behaviour.
func (p *Parquet) EvolveSchema(ctx context.Context, _, _ any) (any, error) {
	if !p.stream.NormalizationEnabled() {
		return false, nil
	}

	logger.Infof("Thread[%s]: schema evolution detected", p.options.ThreadID)

	if err := p.active().Close(ctx); err != nil {
		return nil, fmt.Errorf("failed to seal schema generation: %w", err)
	}
	p.generations = append(p.generations, p.newGeneration())
	return p.schema.Clone(), nil
}

func (p *Parquet) getPartitionedFilePath(basePath string, values map[string]any, olakeTimestamp time.Time) string {
	pattern := p.stream.Self().StreamMetadata.PartitionRegex
	if pattern == "" {
		return basePath
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
		return basePath
	}
	return filepath.Join(basePath, strings.TrimSuffix(result, "/"))
}

// Type returns the type of the writer.
func (p *Parquet) Type() string {
	return string(types.Parquet)
}

func (p *Parquet) DropStreams(ctx context.Context, selectedStreams []types.StreamInterface) error {
	if len(selectedStreams) == 0 {
		logger.Infof("no streams selected for clearing, skipping clear operation")
		return nil
	}

	paths := make([]string, 0, len(selectedStreams))
	for _, stream := range selectedStreams {
		paths = append(paths, stream.GetDestinationDatabase(nil)+"."+stream.GetDestinationTable())
	}

	if p.s3Sink == nil {
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
			return p.s3Sink.ListObjectsPages(ctx, filtPath, func(page *s3.ListObjectsOutput, _ bool) bool {
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
						return p.s3Sink.DeleteObject(ctx, key)
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
		client := p.s3Sink.GetClient()
		err := utils.RetryWithSkip(ctx, 3, time.Minute, isRateLimitError, func(_ context.Context) error {
			iter := s3manager.NewDeleteListIterator(client, &s3.ListObjectsInput{
				Bucket: aws.String(p.config.Bucket),
				Prefix: aws.String(filtPath),
			})
			return s3manager.NewBatchDeleteWithClient(client).Delete(ctx, iter)
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

// newGeneration builds a PartitionedRollingWriter bound to the current schema. Its
// per-partition factory wires one SingleRoller: a parquet encoder over an s3 or
// local sink, with a converter (and its arrow allocator) created per partition so
// concurrently-encoded partitions never share an allocator.
func (p *Parquet) newGeneration() *schemaGen {
	arrowSchema := p.arrowSchema()
	writerProps := []parquet.WriterProperty{parquet.WithCompression(compress.Codecs.Snappy)}
	normalized := p.stream.NormalizationEnabled()

	factory := func(part writers.Partition) (writers.RollingWriter, error) {
		s, err := p.newSink(part.Key)
		if err != nil {
			return nil, err
		}
		enc := parquetenc.New(arrowSchema, writerProps, nil)
		var convert roller.Converter[*types.RawRecord]
		if normalized {
			convert = roller.RawRecordConverter(arrowSchema)
		} else {
			convert = stringifiedConverter(arrowSchema)
		}
		return writers.SingleRoller{
			Roller: roller.NewRoller(s, enc, convert, p.rollerCfg),
			Sink:   s,
		}, nil
	}
	return &schemaGen{
		writers.New(p.basePath, p.partitioner(), factory, writers.Config{}),
	}
}

// newSink builds the per-partition sink: an S3 sink (staging locally, uploading on
// Commit) when S3 is configured, else a local-filesystem sink. The partition key
// already includes basePath, so it is the directory under config.Path / the s3 prefix.
func (p *Parquet) newSink(partitionKey string) (sink.StagedSink, error) {
	if p.s3Sink != nil {
		return s3sink.NewS3Sink(*p.config, func(int) string {
			return filepath.Join(p.config.Prefix, partitionKey, fileName())
		})
	}
	return localfs.NewLocalFSSink(func(int) string {
		return filepath.Join(p.config.Path, partitionKey, fileName())
	}), nil
}

// active returns the current (newest) schema generation.
func (p *Parquet) active() writers.PartitionedRollingWriter {
	return p.generations[len(p.generations)-1].prw
}

// arrowSchema is the physical schema for the current mode, mirroring the legacy
// ToParquet selection: typed columns when normalized; system columns + a JSON
// "data" column when denormalized.
func (p *Parquet) arrowSchema() *arrow.Schema {
	if p.stream.NormalizationEnabled() {
		return p.schema.ToTypeSchema().ToArrow(false, p.stream)
	}
	return p.stream.Schema().ToArrow(true, p.stream)
}

// partitioner routes a record to its partition path (which also names its files),
// reusing the legacy getPartitionedFilePath.
func (p *Parquet) partitioner() writers.Partitioner {
	return func(basePath string, rec *types.RawRecord) (writers.Partition, error) {
		olakeTimestamp := rec.OlakeColumns[constants.OlakeTimestamp].(time.Time)
		return writers.Partition{Key: p.getPartitionedFilePath(basePath, rec.Data, olakeTimestamp)}, nil
	}
}
