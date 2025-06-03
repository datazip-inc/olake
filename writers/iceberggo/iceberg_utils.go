package olake

import (
	"fmt"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
)

type NewIcebergGo struct {
	config *Config
	stream protocol.Stream
	catalog    catalog.Catalog
	iceTable   *table.Table
	schema     *iceberg.Schema
	tableIdent table.Identifier
	records      []types.RawRecord
	allocator     memory.Allocator
	recordBuilder *array.RecordBuilder
	s3Client *s3.Client
	writerID string
	configHash 		string
	partitionInfo   map[string]string 
}

type Config struct {
	CatalogType    string `json:"catalog_type"`
	RestCatalogURL string `json:"rest_catalog_url"`
	S3Endpoint   string `json:"s3_endpoint"`
	AwsRegion    string `json:"aws_region"`
	AwsAccessKey string `json:"aws_access_key"`
	AwsSecretKey string `json:"aws_secret_key"`
	S3UseSSL     bool   `json:"s3_use_ssl"`
	S3PathStyle  bool   `json:"s3_path_style"`
	IcebergDB string `json:"iceberg_db"`
	Namespace string `json:"namespace"`
	CreateTableIfNotExists bool `json:"create_table_if_not_exists"`
	BatchSize              int  `json:"batch_size"`
	Normalization          bool `json:"normalization"`
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

func (w *NewIcebergGo) GetConfigRef() protocol.Config {
	w.config = &Config{}
	return w.config
}

func (w *NewIcebergGo) Spec() any {
	return Config{}
}

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

func (w *NewIcebergGo) Close() error {
	err := w.flushAllLocalBuffers()
	if err != nil {
		logger.Errorf("Error flushing local buffers on close: %v", err)
		return err
	}

	err = w.flushBatch()
	if err != nil {
		logger.Errorf("Error flushing batch on close: %v", err)
		return err
	}

	serverMutex.Lock()
	if instance, exists := serverRegistry[w.configHash]; exists {
		instance.refCount--
		logger.Infof("[%s] Decremented ref count for configHash %s, new count: %d", w.writerID, w.configHash, instance.refCount)
		
		if instance.refCount <= 0 {
			delete(serverRegistry, w.configHash)
			logger.Infof("[%s] Removed server instance for configHash: %s", w.writerID, w.configHash)
		}
	}
	serverMutex.Unlock()

	logger.Infof("[%s] Successfully closed writer", w.writerID)
	return nil
}

func (w *NewIcebergGo) flushAllLocalBuffers() error {
	var localBuffersToFlush []*LocalBuffer

	localBuffers.Range(func(key, value interface{}) bool {
		bufferID := key.(string)
		if strings.HasPrefix(bufferID, w.configHash+"-") {
			localBuffersToFlush = append(localBuffersToFlush, value.(*LocalBuffer))
		}
		return true
	})

	for _, buffer := range localBuffersToFlush {
		err := w.flushLocalBuffer(buffer)
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *NewIcebergGo) flushBatch() error {
	batchVal, exists := batchRegistry.Load(w.configHash)
	if !exists {
		return nil
	}

	batch := batchVal.(*recordBatch)
	batch.mu.Lock()

	if len(batch.records) == 0 {
		batch.mu.Unlock()
		return nil
	}

	recordsToFlush := make([]types.RawRecord, len(batch.records))
	copy(recordsToFlush, batch.records)
	batch.records = batch.records[:0]
	batch.size = 0

	batch.mu.Unlock()
	return w.flushRecordsBatch(recordsToFlush)
}

func (w *NewIcebergGo) Check() error {
	logger.Infof("Checking ICEBERGGO writer configuration")

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

func (w *NewIcebergGo) ReInitiationRequiredOnSchemaEvolution() bool {
	return true
}

func (w *NewIcebergGo) Type() string {
	return "iceberggo"
}

func (w *NewIcebergGo) Flattener() protocol.FlattenFunction {
	return func(rec types.Record) (types.Record, error) {
		return rec, nil
	}
}

func (w *NewIcebergGo) Normalization() bool {
	return w.config.Normalization
}

func (w *NewIcebergGo) EvolveSchema(_ bool, _ bool, _ map[string]*types.Property, _ types.Record, _ time.Time) error {
	return nil
}

func (c *Config) Validate() error {
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
		c.Namespace = c.IcebergDB 
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 1000
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

func estimateRecordSize(record types.RawRecord) int64 {
	size := int64(0)
	for key, value := range record.Data {
		size += int64(len(key))
		
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
			size += 24
		default:
			size += int64(len(fmt.Sprintf("%v", v)))
		}
	}
	return size
}

func getConfigHash(namespace string, streamID string, appendMode bool) string {
	hashComponents := []string{
		streamID,
		namespace,
		fmt.Sprintf("%t", appendMode),
	}
	return strings.Join(hashComponents, "-")
}

type BatchMetrics struct {
	TotalRecordsProcessed atomic.Int64
	TotalBatchesFlushed   atomic.Int64
	TotalFlushTime        atomic.Int64 
	LastFlushSize         atomic.Int64
}

var (
	batchMetrics = &BatchMetrics{}
)

func recordBatchFlush(recordCount int64, flushTime time.Duration) {
	batchMetrics.TotalRecordsProcessed.Add(recordCount)
	batchMetrics.TotalBatchesFlushed.Add(1)
	batchMetrics.TotalFlushTime.Add(flushTime.Milliseconds())
	batchMetrics.LastFlushSize.Store(recordCount)
	
	if batchMetrics.TotalBatchesFlushed.Load()%10 == 0 {
		avgFlushTime := float64(batchMetrics.TotalFlushTime.Load()) / float64(batchMetrics.TotalBatchesFlushed.Load())
		logger.Infof("Batch Performance - Total Records: %d, Total Batches: %d, Avg Flush Time: %.2fms, Last Batch Size: %d",
			batchMetrics.TotalRecordsProcessed.Load(),
			batchMetrics.TotalBatchesFlushed.Load(),
			avgFlushTime,
			batchMetrics.LastFlushSize.Load())
	}
}