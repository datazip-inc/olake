package iceberggo

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/config"
	"github.com/apache/iceberg-go/utils"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
)

var commitMutexes sync.Map

func (w *NewIcebergGo) Setup(stream types.StreamInterface, options *destination.Options) error {
	w.options = options
	w.stream = stream
	w.allocator = memory.NewGoAllocator()

	w.configHash = getConfigHash(w.config.IcebergDB, w.stream.Name(), true)
	logger.Infof("Confighash: %s", w.configHash)

	config.EnvConfig.MaxWorkers = 20
	w.partitionInfo = make(map[string]string)

	partitionRegex := w.stream.Self().StreamMetadata.PartitionRegex
	if partitionRegex != "" {
		err := w.parsePartitionRegex(partitionRegex)
		if err != nil {
			return fmt.Errorf("failed to parse partition regex: %v", err)
		}
	}

	err := w.SetupIcebergClient()
	if err != nil {
		return fmt.Errorf("failed to setup iceberg client: %v", err)
	}

	return nil
}

func (w *NewIcebergGo) createIcebergSchema() ([]iceberg.NestedField, error) {
	if w.stream == nil || w.stream.Schema() == nil {
		return nil, fmt.Errorf("stream or schema is nil")
	}
	streamSchema := w.stream.Schema()
	fields := make([]iceberg.NestedField, 0)
	streamSchema.Properties.Range(func(key, value any) bool {
		name := key.(string)
		var fieldType iceberg.Type
		property := value.(*types.Property)
		typeStr := property.DataType()

		switch typeStr {
		case types.Int32:
			fieldType = &iceberg.Int32Type{}
		case types.Int64:
			fieldType = &iceberg.Int64Type{}
		case types.Float32:
			fieldType = &iceberg.Float32Type{}
		case types.Float64:
			fieldType = &iceberg.Float64Type{}
		case types.String:
			fieldType = &iceberg.StringType{}
		case types.Bool:
			fieldType = &iceberg.BooleanType{}
		case types.Timestamp:
			fieldType = &iceberg.TimestampType{}
		case types.TimestampMilli:
			fieldType = &iceberg.TimestampType{}
		case types.TimestampMicro:
			fieldType = &iceberg.TimestampType{}
		case types.TimestampNano:
			fieldType = &iceberg.TimestampType{}
		default:
			fieldType = &iceberg.StringType{}
		}

		switch name {
		case "_olake_id":
			fieldType = &iceberg.StringType{}
		case "_olake_timestamp":
			fieldType = &iceberg.TimestampType{}
		case "_op_type":
			fieldType = &iceberg.StringType{}
		case "_cdc_timestamp":
			fieldType = &iceberg.TimestampType{}
		}

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
		var arrowType arrow.DataType
		fieldType := field.Type
		switch fieldType.(type) {
		case *iceberg.Int32Type, iceberg.Int32Type:
			arrowType = arrow.PrimitiveTypes.Int32
		case *iceberg.Int64Type, iceberg.Int64Type:
			arrowType = arrow.PrimitiveTypes.Int64
		case *iceberg.Float32Type, iceberg.Float32Type:
			arrowType = arrow.PrimitiveTypes.Float32
		case *iceberg.Float64Type, iceberg.Float64Type:
			arrowType = arrow.PrimitiveTypes.Float64
		case *iceberg.StringType, iceberg.StringType:
			arrowType = arrow.BinaryTypes.String
		case *iceberg.BooleanType, iceberg.BooleanType:
			arrowType = arrow.FixedWidthTypes.Boolean
		case *iceberg.TimestampType, iceberg.TimestampType:
			arrowType = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: ""}
		case *iceberg.TimestampTzType, iceberg.TimestampTzType:
			arrowType = arrow.FixedWidthTypes.Timestamp_us
		default:
			arrowType = arrow.BinaryTypes.String
		}

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

	for i := 0; i < numFields; i++ {
		field := schema.Field(i)

		switch field.Name {
		case "_cdc_timestamp":
			if !record.CdcTimestamp.IsZero() {
				w.recordBuilder.Field(i).(*array.TimestampBuilder).Append(arrow.Timestamp(record.CdcTimestamp.UnixMicro()))
			} else {
				w.recordBuilder.Field(i).AppendNull()
			}
			continue
		case "_olake_timestamp":
			if !record.OlakeTimestamp.IsZero() {
				w.recordBuilder.Field(i).(*array.TimestampBuilder).Append(arrow.Timestamp(record.OlakeTimestamp.UnixMicro()))
			} else {
				w.recordBuilder.Field(i).AppendNull()
			}
			continue
		case "_op_type":
			if record.OperationType != "" {
				w.recordBuilder.Field(i).(*array.StringBuilder).Append(record.OperationType)
			} else {
				w.recordBuilder.Field(i).AppendNull()
			}
			continue
		case "_olake_id":
			if record.OlakeID != "" {
				w.recordBuilder.Field(i).(*array.StringBuilder).Append(record.OlakeID)
			} else {
				w.recordBuilder.Field(i).AppendNull()
			}
			continue
		}

		val, ok := record.Data[field.Name]
		if !ok || val == nil {
			w.recordBuilder.Field(i).AppendNull()
			continue
		}

		switch builder := w.recordBuilder.Field(i).(type) {
		case *array.BooleanBuilder:
			if boolVal, ok := val.(bool); ok {
				builder.Append(boolVal)
			} else {
				builder.AppendNull()
			}
		case *array.Int32Builder:
			switch v := val.(type) {
			case int32:
				builder.Append(int32(v))
			default:
				builder.AppendNull()
			}
		case *array.Int64Builder:
			switch v := val.(type) {
			case int32:
				builder.Append(int64(v))
			default:
				builder.AppendNull()
			}
		case *array.Float32Builder:
			switch v := val.(type) {
			case float32:
				builder.Append(float32(v))
			default:
				builder.AppendNull()
			}
		case *array.Float64Builder:
			switch v := val.(type) {
			case float32:
				builder.Append(float64(v))
			default:
				builder.AppendNull()
			}
		case *array.StringBuilder:
			switch v := val.(type) {
			case string:
				builder.Append(v)
			case []byte:
				builder.Append(string(v))
			default:
				strVal := fmt.Sprintf("%v", v)
				builder.Append(strVal)
			}
		case *array.TimestampBuilder:
			switch v := val.(type) {
			case time.Time:
				builder.Append(arrow.Timestamp(v.UnixMicro()))
			case int64:
				builder.Append(arrow.Timestamp(v))
			default:
				builder.AppendNull()
			}
		default:
			builder.AppendNull()
		}
	}

	currentCount := w.recordsSize.Add(1)

	if currentCount >= arrowBuildersThreshold && !w.flushing.Load() {
		logger.Infof("Threshold reached, starting flush")
		if w.flushing.CompareAndSwap(false, true) {
			err := w.flushArrowBuilder()
			w.flushing.Store(false)
			if err != nil {
				logger.Errorf("Flush failed: %v", err)
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

	logger.Infof("Flushing %d Arrow Records", currentRecords)

	mutex := getCommitMutex(w.configHash)
	mutex.Lock()
	logger.Infof("Mutex locked")
	defer func() {
		mutex.Unlock()
		logger.Infof("Mutex unlocked")
	}()

	arrowRecord := w.recordBuilder.NewRecord()
	defer arrowRecord.Release()

	arrowTable := array.NewTableFromRecords(arrowRecord.Schema(), []arrow.Record{arrowRecord})
	defer arrowTable.Release()

	var err error
	ctx := context.Background()
	staticCreds := aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(w.config.AwsAccessKey, w.config.AwsSecretKey, ""))
	cfg := aws.Config{
		Region:      w.config.AwsRegion,
		Credentials: staticCreds,
	}

	ctx = utils.WithAwsConfig(ctx, &cfg)

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
		logger.Errorf("failed to append data: %v", err)
		return err
	}
	w.iceTable = updatedTable
	appendDuration := time.Since(appendStartTime)
	logger.Infof("Successfully committed the transaction using Append in %v", appendDuration)

	w.recordsSize.Store(0)
	w.createRecordBuilder()

	logger.Infof("Total flush operation took %v | Remaining records: %d", time.Since(startTime), w.recordsSize.Load())

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
