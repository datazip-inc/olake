package olake

import (
	// "context"
	// "errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"

	// "strings"
	"sync"
	"time"

	// "github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	// "github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"

	// "github.com/apache/iceberg-go/catalog/rest"
	// iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	// "github.com/aws/aws-sdk-go-v2/aws"
	// "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"

	// "github.com/datazip-inc/olake/typeutils"
	// "github.com/google/uuid"
	"runtime"
)

// A simple implementation of the writer interface for ICEBERGGO
type NewIcebergGo struct {
	config *Config
	stream protocol.Stream

	// Iceberg components
	catalog    catalog.Catalog
	iceTable   *table.Table
	schema     *iceberg.Schema
	tableIdent table.Identifier

	// Batching
	records      []types.RawRecord
	recordsMutex sync.Mutex

	// Arrow components
	allocator     memory.Allocator
	recordBuilder *array.RecordBuilder

	// For column mapping
	schemaMapping map[string]int

	// S3 client for Iceberg operations
	s3Client *s3.Client
	// mutex    sync.Mutex

	writerID string

	configHash 		string
	recordsAtomic   atomic.Int64
	partitionInfo   map[string]string // map of field names to partition transform
}

type Config struct {
	// Catalog configuration
	CatalogType    string `json:"catalog_type"`
	RestCatalogURL string `json:"rest_catalog_url"`

	// S3 configuration
	S3Endpoint   string `json:"s3_endpoint"`
	AwsRegion    string `json:"aws_region"`
	AwsAccessKey string `json:"aws_access_key"`
	AwsSecretKey string `json:"aws_secret_key"`
	S3UseSSL     bool   `json:"s3_use_ssl"`
	S3PathStyle  bool   `json:"s3_path_style"`

	// Iceberg configuration
	IcebergDB string `json:"iceberg_db"`
	Namespace string `json:"namespace"`

	// Table options
	CreateTableIfNotExists bool `json:"create_table_if_not_exists"`
	BatchSize              int  `json:"batch_size"`
	Normalization          bool `json:"normalization"`
	AppendMode             bool `json:"append_mode"`
}

type LocalBuffer struct {
	records []types.RawRecord
	size int64
}

type recordBatch struct {
	records []types.RawRecord
	size int64
	mu sync.Mutex
}

var (
	localBufferThreshold int64 = 50 * 1024 * 1024
	localBuffers sync.Map
)

var (
	batchRegistry sync.Map
	maxBatchSize = determineMaxBatchSize()
)

type serverInstance struct {
	catalog catalog.Catalog
	iceTable *table.Table
	refCount int
	configHash string
}

var serverRegistry = make(map[string]*serverInstance)
var serverMutex sync.Mutex

func getOrCreateServerInstance(configHash string, config *Config) (*serverInstance, error) {
	serverMutex.Lock()
	defer serverMutex.Unlock()

	if instance, exists := serverRegistry[configHash]; exists {
		instance.refCount++
		return instance, nil
	}

	instance := &serverInstance{
		configHash: configHash,
		refCount: 1,
	}

	serverRegistry[configHash] = instance
	return instance, nil
}

func getGoroutineId() string {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	id := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	return id
}

func determineMaxBatchSize() int64 {
	ramGB := utils.DetermineSystemMemoryGB()

	var batchSize int64

	switch {
	case ramGB <= 8:
		batchSize = 200 * 1024 * 1024 // 200MB
	case ramGB <= 16:
		batchSize = 400 * 1024 * 1024 // 400MB
	case ramGB <= 32:
		batchSize = 800 * 1024 * 1024 // 800MB
	default:
		batchSize = 1600 * 1024 * 1024 // 1600MB
	}

	logger.Infof("System has %dGB RAM, setting iceberg writer batch size to %d bytes", ramGB, batchSize)
	return batchSize
}

func getOrCreateBatch(configHash string) *recordBatch {
	batch, _ := batchRegistry.LoadOrStore(configHash, &recordBatch{
		records: make([]types.RawRecord, 0, 1000),
		size: 0,
	})

	return batch.(*recordBatch)
}

func getLocalBuffer(configHash string) *LocalBuffer {
	goroutineId := getGoroutineId()
	bufferID := fmt.Sprintf("%s-%s", configHash, goroutineId)

	if val, ok := localBuffers.Load(bufferID); ok {
		return val.(*LocalBuffer)
	}

	buffer := &LocalBuffer{
		records: make([]types.RawRecord, 0, 1000),
		size: 0,
	}

	localBuffers.Store(bufferID, buffer)
	return buffer
}

// GetConfigRef returns a reference to this writer's configuration
func (w *NewIcebergGo) GetConfigRef() protocol.Config {
	w.config = &Config{}
	return w.config
}

// Spec returns the configuration specification
func (w *NewIcebergGo) Spec() any {
	return Config{}
}

// Helpers for type conversion
func toInt32(value any) (int32, bool) {
	switch v := value.(type) {
	case int:
		return int32(v), true
	case int8:
		return int32(v), true
	case int16:
		return int32(v), true
	case int32:
		return v, true
	case int64:
		if v > 2147483647 || v < -2147483648 {
			return 0, false
		}
		return int32(v), true
	case uint8:
		return int32(v), true
	case uint16:
		return int32(v), true
	case uint32:
		if v > 2147483647 {
			return 0, false
		}
		return int32(v), true
	case float32:
		return int32(v), true
	case float64:
		return int32(v), true
	case string:
		if i, err := strconv.ParseInt(v, 10, 32); err == nil {
			return int32(i), true
		}
	case bool:
		if v {
			return 1, true
		}
		return 0, true
	}
	return 0, false
}

func toInt64(value any) (int64, bool) {
	switch v := value.(type) {
	case int:
		return int64(v), true
	case int8:
		return int64(v), true
	case int16:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case uint8:
		return int64(v), true
	case uint16:
		return int64(v), true
	case uint32:
		return int64(v), true
	case uint64:
		if v > 9223372036854775807 {
			return 0, false
		}
		return int64(v), true
	case float32:
		return int64(v), true
	case float64:
		return int64(v), true
	case string:
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			return i, true
		}
	case bool:
		if v {
			return 1, true
		}
		return 0, true
	}
	return 0, false
}

func toFloat32(value any) (float32, bool) {
	switch v := value.(type) {
	case int:
		return float32(v), true
	case int8:
		return float32(v), true
	case int16:
		return float32(v), true
	case int32:
		return float32(v), true
	case int64:
		return float32(v), true
	case uint8:
		return float32(v), true
	case uint16:
		return float32(v), true
	case uint32:
		return float32(v), true
	case uint64:
		return float32(v), true
	case float32:
		return v, true
	case float64:
		return float32(v), true
	case string:
		if f, err := strconv.ParseFloat(v, 32); err == nil {
			return float32(f), true
		}
	case bool:
		if v {
			return 1.0, true
		}
		return 0.0, true
	}
	return 0.0, false
}

func toFloat64(value any) (float64, bool) {
	switch v := value.(type) {
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f, true
		}
	case bool:
		if v {
			return 1.0, true
		}
		return 0.0, true
	}
	return 0.0, false
}

func toString(value any) (string, bool) {
	if value == nil {
		return "", true
	}
	switch v := value.(type) {
	case string:
		return v, true
	case []byte:
		return string(v), true
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, bool:
		return fmt.Sprintf("%v", v), true
	case time.Time:
		return v.Format(time.RFC3339), true
	}
	return fmt.Sprintf("%v", value), true
}

// Close handles cleanup
func (w *NewIcebergGo) Close() error {
	// Flush any remaining local buffers
	err := w.flushAllLocalBuffers()
	if err != nil {
		logger.Errorf("Error flushing local buffers on close: %v", err)
		return err
	}

	// Flush any remaining records in the shared batch
	err = w.flushBatch()
	if err != nil {
		logger.Errorf("Error flushing batch on close: %v", err)
		return err
	}

	// Handle server instance cleanup with reference counting
	serverMutex.Lock()
	if instance, exists := serverRegistry[w.configHash]; exists {
		instance.refCount--
		logger.Infof("[%s] Decremented ref count for configHash %s, new count: %d", w.writerID, w.configHash, instance.refCount)
		
		if instance.refCount <= 0 {
			delete(serverRegistry, w.configHash)
			logger.Infof("[%s] Removed server instance for configHash: %s", w.writerID, w.configHash)
			
			// Clean up async processor when no more references
			if processor, exists := asyncProcessors.Load(w.configHash); exists {
				processor.(*AsyncBatchProcessor).stop()
				asyncProcessors.Delete(w.configHash)
				logger.Infof("[%s] Stopped async processor for configHash: %s", w.writerID, w.configHash)
			}
		}
	}
	serverMutex.Unlock()

	logger.Infof("[%s] Successfully closed writer", w.writerID)
	return nil
}

// flushAllLocalBuffers flushes all local buffers for this config hash
func (w *NewIcebergGo) flushAllLocalBuffers() error {
	var localBuffersToFlush []*LocalBuffer

	// Collect all local buffers for this config hash
	localBuffers.Range(func(key, value interface{}) bool {
		bufferID := key.(string)
		if strings.HasPrefix(bufferID, w.configHash+"-") {
			localBuffersToFlush = append(localBuffersToFlush, value.(*LocalBuffer))
		}
		return true
	})

	// Flush each local buffer
	for _, buffer := range localBuffersToFlush {
		err := w.flushLocalBuffer(buffer)
		if err != nil {
			return err
		}
	}

	return nil
}

// flushBatch forces a flush of the shared batch for this config hash
func (w *NewIcebergGo) flushBatch() error {
	batchVal, exists := batchRegistry.Load(w.configHash)
	if !exists {
		return nil // Nothing to flush
	}

	batch := batchVal.(*recordBatch)

	// Lock the batch
	batch.mu.Lock()

	// Skip if batch is empty
	if len(batch.records) == 0 {
		batch.mu.Unlock()
		return nil
	}

	// Copy records to flush
	recordsToFlush := make([]types.RawRecord, len(batch.records))
	copy(recordsToFlush, batch.records)

	// Reset the batch
	batch.records = batch.records[:0]
	batch.size = 0

	// Unlock the batch
	batch.mu.Unlock()

	// Send the records
	return w.flushRecordsBatch(recordsToFlush)
}

// Check validates the configuration
func (w *NewIcebergGo) Check() error {
	logger.Infof("Checking ICEBERGGO writer configuration")

	// Validate basic requirements
	if w.config.RestCatalogURL == "" {
		return fmt.Errorf("rest_catalog_url is required")
	}
	if w.config.S3Endpoint == "" {
		return fmt.Errorf("s3_endpoint is required")
	}
	if w.config.AwsRegion == "" {
		return fmt.Errorf("aws_region is required")
	}
	if w.config.AwsAccessKey == "" {
		return fmt.Errorf("aws_access_key is required")
	}
	if w.config.AwsSecretKey == "" {
		return fmt.Errorf("aws_secret_key is required")
	}
	if w.config.IcebergDB == "" {
		return fmt.Errorf("iceberg_db is required")
	}

	return nil
}

// ReInitiationRequiredOnSchemaEvolution returns whether the writer should be re-initialized on schema evolution
func (w *NewIcebergGo) ReInitiationRequiredOnSchemaEvolution() bool {
	return true
}

// Type returns the writer type
func (w *NewIcebergGo) Type() string {
	return "iceberggo"
}

// Flattener returns a function to flatten records
func (w *NewIcebergGo) Flattener() protocol.FlattenFunction {
	return func(rec types.Record) (types.Record, error) {
		return rec, nil
	}
}

// Normalization returns whether normalization is enabled
func (w *NewIcebergGo) Normalization() bool {
	return w.config.Normalization
}

// EvolveSchema handles schema evolution
func (w *NewIcebergGo) EvolveSchema(_ bool, _ bool, _ map[string]*types.Property, _ types.Record, _ time.Time) error {
	// TODO: Implement schema evolution. For now, we'll require reinitialization
	return nil
}

// Validate validates the configuration
func (c *Config) Validate() error {
	// Validate basic requirements
	if c.RestCatalogURL == "" {
		return fmt.Errorf("rest_catalog_url is required")
	}
	if c.S3Endpoint == "" {
		return fmt.Errorf("s3_endpoint is required")
	}
	if c.AwsRegion == "" {
		return fmt.Errorf("aws_region is required")
	}
	if c.AwsAccessKey == "" {
		return fmt.Errorf("aws_access_key is required")
	}
	if c.AwsSecretKey == "" {
		return fmt.Errorf("aws_secret_key is required")
	}
	if c.IcebergDB == "" {
		return fmt.Errorf("iceberg_db is required")
	}
	if c.Namespace == "" {
		c.Namespace = c.IcebergDB // Use IcebergDB as namespace if not specified
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 1000 // Default batch size
	}
	return nil
}

func init() {
	protocol.RegisteredWriters[types.IcebergGo] = func() protocol.Writer {
		return new(NewIcebergGo)
	}
}

func (w *NewIcebergGo) parsePartitionRegex(pattern string) error {
	patternRegex := regexp.MustCompile(`\{([^,]+),\s*([^}]+)\}`)
	matches := patternRegex.FindAllStringSubmatch(pattern, -1)

	for _, match := range matches {
		if len(match) < 3 {
			continue
		}

		colName := strings.Replace(strings.TrimSpace(strings.Trim(match[1], `'"`)), "now()", constants.OlakeTimestamp, 1)
		transform := strings.TrimSpace(strings.Trim(match[2], `'"`))
		w.partitionInfo[colName] = transform
	}

	return nil
}

// estimateRecordSize estimates the size of a record in bytes
func estimateRecordSize(record types.RawRecord) int64 {
	size := int64(0)
	for key, value := range record.Data {
		// Add key size
		size += int64(len(key))
		
		// Add value size based on type
		switch v := value.(type) {
		case string:
			size += int64(len(v))
		case int, int8, int16, int32, int64:
			size += 8
		case uint, uint8, uint16, uint32, uint64:
			size += 8
		case float32, float64:
			size += 8
		case bool:
			size += 1
		case time.Time:
			size += 24 // approximate size for timestamp
		default:
			// For unknown types, estimate as string representation
			size += int64(len(fmt.Sprintf("%v", v)))
		}
	}
	return size
}

// getConfigHash generates a unique identifier for server configuration per stream
func getConfigHash(namespace string, streamID string, appendMode bool) string {
	hashComponents := []string{
		streamID,
		namespace,
		fmt.Sprintf("%t", appendMode),
	}
	return strings.Join(hashComponents, "-")
}

// AsyncBatchProcessor handles asynchronous batch processing
type AsyncBatchProcessor struct {
	flushChannel chan *asyncFlushRequest
	done         chan struct{}
	writer       *NewIcebergGo
}

type asyncFlushRequest struct {
	records    []types.RawRecord
	resultChan chan error
}

var (
	asyncProcessors sync.Map // Key: configHash, Value: *AsyncBatchProcessor
)

// getOrCreateAsyncProcessor gets or creates an async processor for a config hash
func getOrCreateAsyncProcessor(configHash string, writer *NewIcebergGo) *AsyncBatchProcessor {
	if processor, ok := asyncProcessors.Load(configHash); ok {
		return processor.(*AsyncBatchProcessor)
	}

	processor := &AsyncBatchProcessor{
		flushChannel: make(chan *asyncFlushRequest, 100), // Buffer up to 100 requests
		done:         make(chan struct{}),
		writer:       writer,
	}

	// Start the background goroutine
	go processor.processFlushRequests()

	asyncProcessors.Store(configHash, processor)
	return processor
}

// processFlushRequests processes flush requests in the background
func (p *AsyncBatchProcessor) processFlushRequests() {
	for {
		select {
		case req := <-p.flushChannel:
			err := p.writer.commitRecords(req.records)
			req.resultChan <- err
			close(req.resultChan)
		case <-p.done:
			return
		}
	}
}

// flushAsync submits a flush request asynchronously
func (p *AsyncBatchProcessor) flushAsync(records []types.RawRecord) error {
	resultChan := make(chan error, 1)
	
	select {
	case p.flushChannel <- &asyncFlushRequest{
		records:    records,
		resultChan: resultChan,
	}:
		// Request submitted successfully, wait for result
		return <-resultChan
	default:
		// Channel is full, process synchronously as fallback
		logger.Warnf("Async flush channel full, falling back to synchronous processing")
		return p.writer.commitRecords(records)
	}
}

// stop stops the async processor
func (p *AsyncBatchProcessor) stop() {
	close(p.done)
}

// BatchMetrics tracks performance metrics
type BatchMetrics struct {
	TotalRecordsProcessed atomic.Int64
	TotalBatchesFlushed   atomic.Int64
	TotalFlushTime        atomic.Int64 // in milliseconds
	LastFlushSize         atomic.Int64
}

var (
	batchMetrics = &BatchMetrics{}
)

// recordBatchFlush records metrics for a batch flush
func recordBatchFlush(recordCount int64, flushTime time.Duration) {
	batchMetrics.TotalRecordsProcessed.Add(recordCount)
	batchMetrics.TotalBatchesFlushed.Add(1)
	batchMetrics.TotalFlushTime.Add(flushTime.Milliseconds())
	batchMetrics.LastFlushSize.Store(recordCount)
	
	// Log performance metrics every 10 batches
	if batchMetrics.TotalBatchesFlushed.Load()%10 == 0 {
		avgFlushTime := float64(batchMetrics.TotalFlushTime.Load()) / float64(batchMetrics.TotalBatchesFlushed.Load())
		logger.Infof("Batch Performance - Total Records: %d, Total Batches: %d, Avg Flush Time: %.2fms, Last Batch Size: %d",
			batchMetrics.TotalRecordsProcessed.Load(),
			batchMetrics.TotalBatchesFlushed.Load(),
			avgFlushTime,
			batchMetrics.LastFlushSize.Load())
	}
}