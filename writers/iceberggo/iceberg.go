package iceberggo

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	_ "github.com/apache/iceberg-go/catalog/glue"
	"github.com/apache/iceberg-go/config"
	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/google/uuid"
)

var commitMutexes sync.Map
var thread atomic.Int32

func (w *NewIcebergGo) Setup(stream protocol.Stream, options *protocol.Options) error {
	setupStartTime := time.Now()
	w.thread = int32(thread.Load())
	w.writerID = fmt.Sprintf("%s", uuid.New().String()[:8])
	
	w.stream = stream
	w.allocator = memory.NewGoAllocator() 
	w.configHash = getConfigHash(w.config.Namespace, w.config.GlueTableName, true)
	config.EnvConfig.MaxWorkers = 20 // iceberg-go max workers
	w.partitionInfo = make(map[string]string) 

	partitionRegex := w.stream.Self().StreamMetadata.PartitionRegex
	if partitionRegex != "" {
		err := w.parsePartitionRegex(partitionRegex)
		if err != nil {
			return fmt.Errorf("[%s] failed to parse partition regex: %v", w.writerID, err)
		}
	}
	
	err := w.SetupIcebergClient()
	if err != nil {
		return fmt.Errorf("[%s] failed to setup iceberg client: %v", w.writerID, err)
	}

	logger.Infof("[%s] Total Setup took %v", w.writerID, time.Since(setupStartTime))
	return nil
}

func (w *NewIcebergGo) createIcebergSchema() ([]iceberg.NestedField, error) {
	if w.stream == nil || w.stream.Schema() == nil {
		return nil, fmt.Errorf("stream or schema is nil")
	}

	streamSchema := w.stream.Schema()
	fields := make([]iceberg.NestedField, 0)
	streamSchema.Properties.Range(func(key, value interface{}) bool {
		name := key.(string)
		fieldType := iceberg.StringType{}

		fields = append(fields, iceberg.NestedField{
			Name:     name,
			Type:     fieldType,
			Required: false,
		})

		return true
	})

	return fields, nil
}

func (w *NewIcebergGo) createRecordBuilder() {
	fields := make([]arrow.Field, 0, len(w.schema.Fields()))

	for _, field := range w.schema.Fields() {
		arrowType := arrow.BinaryTypes.String
		fields = append(fields, arrow.Field{
			Name:     field.Name,
			Type:     arrowType,
			Nullable: !field.Required,
		})
	}

	arrowSchema := arrow.NewSchema(fields, nil)
	w.recordBuilder = array.NewRecordBuilder(w.allocator, arrowSchema)
}

func (w *NewIcebergGo) Write(_ context.Context, record types.RawRecord) error {
	schema := w.recordBuilder.Schema()
	numFields := len(schema.Fields())

	for i:=0; i<numFields; i++ {
		field := schema.Field(i)
		builder := w.recordBuilder.Field(i).(*array.StringBuilder) // TODO: Only support strings

		val, ok := record.Data[field.Name]
		if !ok || val == nil {
			builder.AppendNull()
		}else{
			builder.Append(fmt.Sprint(val)) // TODO: Only string supported
		}
	}
	
	batchMetrics.TotalRecordsProcessed.Add(1)
	currentCount := w.recordsSize.Add(1)
	
	if currentCount >= arrowBuildersThreshold && !w.flushing.Load() {
		logger.Infof("[%s] Threshold reached, starting flush", w.writerID)
		if w.flushing.CompareAndSwap(false, true) {
			flushStartTime := time.Now()
			err := w.flushArrowBuilder()
			w.flushing.Store(false)
			flushDuration := time.Since(flushStartTime)
			batchMetrics.TotalFlushTime.Add(int64(flushDuration.Seconds()))
			if err != nil {
				logger.Errorf("[%s] Flush failed: %v", w.writerID, err)
				return err
			}
		}
	}
	return nil
}

func (w *NewIcebergGo) flushArrowBuilder() error {
	startTime := time.Now()
	currentRecords := w.recordsSize.Load()
	if currentRecords == 0 {
		return nil
	}
	if w.recordBuilder == nil {
		return fmt.Errorf("record builder is nil")
	}
	if w.iceTable == nil {
		return fmt.Errorf("iceberg table is nil")
	}

	logger.Infof("[%s] Flushing %d Arrow Records", w.writerID, currentRecords)
	batchMetrics.LastFlushSize.Store(currentRecords)

	mutex := getCommitMutex(w.configHash)
	mutex.Lock()
	logger.Infof("[%s] Mutex locked", w.writerID)
	defer func() {
		mutex.Unlock()
		logger.Infof("[%s] Mutex unlocked", w.writerID)
	}()
	
	arrowRecord := w.recordBuilder.NewRecord()
	defer arrowRecord.Release()

	arrowTable := array.NewTableFromRecords(arrowRecord.Schema(), []arrow.Record{arrowRecord})
	defer arrowTable.Release()

	var err error
	ctx := context.Background()
	snapshotProps := iceberg.Properties{
		"operation":  "append",
		"source":     "iceberg-go-sample",
		"timestamp":  fmt.Sprintf("%d", time.Now().Unix()),
		"rows-added": fmt.Sprintf("%d", arrowTable.NumRows()),
	}
	
	w.iceTable, err = w.catalog.LoadTable(ctx, w.tableIdent, nil)
	if err != nil {
		return fmt.Errorf("failed to load table: %v", err)
	}

	batchSize := int64(arrowRecord.NumRows())
	rdr := array.NewTableReader(arrowTable, batchSize)
	defer rdr.Release()

	appendStartTime := time.Now()
	updatedTable, err := w.iceTable.AppendTable(ctx, arrowTable, batchSize, snapshotProps)
	if err != nil {
		logger.Errorf("[%s] failed to append data: %v", w.writerID, err)
		return err
	}
	w.iceTable = updatedTable
	appendDuration := time.Since(appendStartTime)
	logger.Infof("[%s] Successfully committed the transaction using Append in %v", w.writerID, appendDuration)

	w.recordsSize.Store(0)
	w.createRecordBuilder()

	batchMetrics.TotalBatchesFlushed.Add(1)
	logger.Infof("[%s] Total flush operation took %v | Remaining records: %d", w.writerID, time.Since(startTime), w.recordsSize.Load())

	return nil
}

func getCommitMutex(configHash string) *sync.Mutex {
	mutex, _ := commitMutexes.LoadOrStore(configHash, &sync.Mutex{})
	count := 0	
	commitMutexes.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	return mutex.(*sync.Mutex)
}