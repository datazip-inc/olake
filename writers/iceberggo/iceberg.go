package olake

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/google/uuid"
)

var commitMutexes sync.Map // Key: configHash, Value: sync.Mutex

func (w *NewIcebergGo) Setup(stream protocol.Stream, options *protocol.Options) error {
	w.stream = stream
	w.records = make([]types.RawRecord, 0, w.config.BatchSize) 
	w.allocator = memory.NewGoAllocator()
	w.schemaMapping = make(map[string]int) // Initialize the schema mapping
	w.writerID = fmt.Sprintf("writer-%s", uuid.New().String()[:8])

	// Initialize configHash for batching optimization
	w.configHash = getConfigHash(w.config.Namespace, w.stream.Name(), w.config.AppendMode)

	// Try to reuse existing server instance
	serverInstance, err := getOrCreateServerInstance(w.configHash, w.config)
	if err == nil && serverInstance.catalog != nil && serverInstance.iceTable != nil {
		logger.Infof("[%s] Reusing existing server instance for configHash: %s", w.writerID, w.configHash)
		w.catalog = serverInstance.catalog
		w.iceTable = serverInstance.iceTable
		w.tableIdent = catalog.ToIdentifier(w.config.Namespace, w.stream.Name())
		
		// Still need to initialize our own components
		w.partitionInfo = make(map[string]string)
		if stream.Self().StreamMetadata.PartitionRegex != "" {
			err := w.parsePartitionRegex(stream.Self().StreamMetadata.PartitionRegex)
			if err != nil {
				return fmt.Errorf("failed to parse partition regex: %v", err)
			}
		}
		
		// Create schema and record builder from existing table
		if w.iceTable != nil {
			w.schema = w.iceTable.Schema()
			w.createRecordBuilder()
		}
		
		logger.Infof("Initialized writer with ID: %s, ConfigHash: %s (reused server)", w.writerID, w.configHash)

		// Store the catalog and table in the server instance for reuse
		if serverInstance != nil {
			serverInstance.catalog = w.catalog
			serverInstance.iceTable = w.iceTable
			logger.Infof("[%s] Stored catalog and table in server instance for reuse", w.writerID)
		}

		return nil
	}

	// Fallback to creating new instance
	logger.Infof("Creating new server instance for configHash: %s", w.configHash)

	w.partitionInfo = make(map[string]string)

	partitionRegex := w.stream.Self().StreamMetadata.PartitionRegex
	if partitionRegex != "" {
		err := w.parsePartitionRegex(partitionRegex)
		if err != nil {
			return fmt.Errorf("failed to parse partition regex: %v", err)
		}
	}

	logger.Infof("Setting up ICEBERGGO writer with catalog %s at %s",
		w.config.CatalogType, w.config.RestCatalogURL)
	logger.Infof("S3 endpoint: %s, region: %s", w.config.S3Endpoint, w.config.AwsRegion)
	logger.Infof("Iceberg DB: %s, namespace: %s", w.config.IcebergDB, w.config.Namespace)
	logger.Infof("Handling nil values with appropriate default values (0 for numbers, empty string for text)")
	logger.Infof("Initialized writer with ID: %s, ConfigHash: %s", w.writerID, w.configHash)

	ctx := context.Background()
	s3Endpoint := w.config.S3Endpoint
	if s3Endpoint == "" {
		return fmt.Errorf("s3_endpoint is required")
	}

	// Debug logging for credentials
	logger.Infof("Using explicit S3 credentials - Access Key: %s, Secret Key: %s[redacted]",
		w.config.AwsAccessKey,
		w.config.AwsSecretKey[:1])

	w.s3Client = s3.New(s3.Options{
		Region: w.config.AwsRegion,
		Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(
			w.config.AwsAccessKey,
			w.config.AwsSecretKey,
			"",
		)),
		EndpointResolver: s3.EndpointResolverFunc(func(region string, options s3.EndpointResolverOptions) (aws.Endpoint, error) {
			return aws.Endpoint{
				URL:               s3Endpoint,
				HostnameImmutable: true,
				SigningRegion:     w.config.AwsRegion,
			}, nil
		}),
		UsePathStyle: w.config.S3PathStyle,
		ClientLogMode:    aws.LogRetries,
		RetryMaxAttempts: 1,
	})

	// Test S3 connectivity
	logger.Infof("Testing S3 connectivity to %s", s3Endpoint)
	_, listErr := w.s3Client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if listErr != nil {
		logger.Errorf("S3 connectivity test failed: %v", listErr)
		return fmt.Errorf("S3 connectivity test failed, please check your credentials and endpoint: %v", listErr)
	}
	logger.Infof("S3 connectivity test successful")

	// Create properties for REST catalog with S3 credentials
	catalogProps := iceberg.Properties{
		iceio.S3Region:             w.config.AwsRegion,
		iceio.S3AccessKeyID:        w.config.AwsAccessKey,
		iceio.S3SecretAccessKey:    w.config.AwsSecretKey,
		iceio.S3EndpointURL:        w.config.S3Endpoint,
		"warehouse":                "s3://warehouse/",
		"s3.path-style-access":     "true",
		"s3.connection-timeout-ms": "50000",
		"s3.socket-timeout-ms":     "50000",
	}

	if w.config.S3PathStyle {
		catalogProps[iceio.S3ForceVirtualAddressing] = "false"
	}

	// Create REST catalog
	restCatalog, err := rest.NewCatalog(
		ctx,
		"olake-catalog",
		w.config.RestCatalogURL,
		rest.WithAdditionalProps(catalogProps),
		rest.WithOAuthToken(""), // Add token if needed
	)

	if err != nil {
		return fmt.Errorf("failed to create REST catalog: %v", err)
	}

	w.catalog = restCatalog

	schemaFields, err := w.createIcebergSchema2()
	if err != nil {
		return fmt.Errorf("failed to create iceberg schema: %v", err)
	}

	w.schema = iceberg.NewSchema(0, schemaFields...)
	w.tableIdent = catalog.ToIdentifier(w.config.Namespace, w.stream.Name())

	var tableExists bool
	_, err = w.catalog.LoadTable(ctx, w.tableIdent, catalogProps)
	if err != nil {
		if errors.Is(err, catalog.ErrNoSuchTable) {
			tableExists = false
			logger.Infof("Table %s does not exist", w.tableIdent)
		} else {
			return fmt.Errorf("failed to load table: %v", w.tableIdent, err)
		}
	} else {
		tableExists = true
		logger.Infof("Table %s exists", w.tableIdent)
	}

	if !tableExists {
		if !w.config.CreateTableIfNotExists {
			return fmt.Errorf("table %s does not exist and create_table_if_not_exists is false", w.tableIdent)
		}

		logger.Infof("Creating new Iceberg table: %s", w.tableIdent)
		w.iceTable, err = w.catalog.CreateTable(ctx, w.tableIdent, w.schema, catalog.WithProperties(iceberg.Properties{
			"write.format.default":     "parquet",
			iceio.S3Region:             w.config.AwsRegion,
			iceio.S3AccessKeyID:        w.config.AwsAccessKey,
			iceio.S3SecretAccessKey:    w.config.AwsSecretKey,
			iceio.S3EndpointURL:        w.config.S3Endpoint,
			"s3.path-style-access":     "true",
		}))
		if err != nil {
			return fmt.Errorf("failed to create table: %v", err)
		}
	} else {
		logger.Infof("Loading existing Iceberg table: %s", w.tableIdent)
		w.iceTable, err = w.catalog.LoadTable(ctx, w.tableIdent, catalogProps)
		if err != nil {
			return fmt.Errorf("failed to load table: %v", err)
		}
	}

	// Initialize record builder
	w.createRecordBuilder()

	// Store the catalog and table in the server instance for reuse
	if serverInstance != nil {
		serverInstance.catalog = w.catalog
		serverInstance.iceTable = w.iceTable
		logger.Infof("[%s] Stored catalog and table in server instance for reuse", w.writerID)
	}

	return nil
}

func (w *NewIcebergGo) createIcebergSchema2() ([]iceberg.NestedField, error) {
	if w.stream == nil || w.stream.Schema() == nil {
		return nil, fmt.Errorf("stream or schema is nil")
	}

	streamSchema := w.stream.Schema()
	fields := make([]iceberg.NestedField, 0)

	streamSchema.Properties.Range(func(key, value interface{}) bool {
		name := key.(string)
		// property := value.(*types.Property)

		var fieldType iceberg.Type

		// dataType := property.DataType()
		fieldType = iceberg.StringType{}

		fields = append(fields, iceberg.NestedField{
			Name:     name,
			Type:     fieldType,
			Required: false,
		})

		w.schemaMapping[name] = 1
		return true
	})

	return fields, nil
}

func (w *NewIcebergGo) createRecordBuilder() {
	fields := make([]arrow.Field, 0, len(w.schema.Fields()))

	for _, field := range w.schema.Fields() {
		var arrowType arrow.DataType
		arrowType = arrow.BinaryTypes.String

		fields = append(fields, arrow.Field{
			Name:     field.Name,
			Type:     arrowType,
			Nullable: !field.Required,
		})
	}
	arrowSchema := arrow.NewSchema(fields, nil)
	// logger.Infof("Arrow schema: %v", arrowSchema)
	w.recordBuilder = array.NewRecordBuilder(w.allocator, arrowSchema)
}

func (w *NewIcebergGo) Write(_ context.Context, record types.RawRecord) error {
	recordSize := int64(estimateRecordSize(record))

	buffer := getLocalBuffer(w.configHash)
	buffer.records = append(buffer.records, record)
	buffer.size += recordSize

	if buffer.size >= localBufferThreshold {
		return w.flushLocalBuffer(buffer)
	}
	
	return nil
}

func (w *NewIcebergGo) flushLocalBuffer(buffer *LocalBuffer) error {
	if len(buffer.records) == 0 {
		return nil
	}
	batch := getOrCreateBatch(w.configHash)

	batch.mu.Lock()
	batch.records = append(batch.records, buffer.records...)
	batch.size += buffer.size
	
	needsFlush := batch.size >= maxBatchSize
	var recordsToFlush []types.RawRecord

	if needsFlush {
		recordsToFlush = make([]types.RawRecord, len(batch.records))
		copy(recordsToFlush, batch.records)
		batch.records = batch.records[:0]
		batch.size = 0
	}

	batch.mu.Unlock()

	buffer.records = buffer.records[:0]
	buffer.size = 0

	if needsFlush {
		return w.flushRecordsBatch(recordsToFlush)
	}

	return nil
}

func getCommitMutex(configHash string) *sync.Mutex {
	mutex, _ := commitMutexes.LoadOrStore(configHash, &sync.Mutex{})
	return mutex.(*sync.Mutex)
}

func (w *NewIcebergGo) flushRecordsBatch(records []types.RawRecord) error {
	// Process synchronously instead of using async processing
	return w.commitRecords(records)
}

func (w *NewIcebergGo) commitRecords(records []types.RawRecord) error {
	if len(records) == 0 {
		return nil
	}

	// Track flush time for performance metrics
	startTime := time.Now()
	defer func() {
		flushDuration := time.Since(startTime)
		recordBatchFlush(int64(len(records)), flushDuration)
	}()

	// Use config-specific mutex for thread safety
	mutex := getCommitMutex(w.configHash)
	mutex.Lock()
	defer mutex.Unlock()

	if w.recordBuilder == nil {
		return fmt.Errorf("record builder is nil")
	}

	if w.iceTable == nil {
		return fmt.Errorf("iceberg table is nil")
	}

	logger.Infof("[%s] Committing %d records to Iceberg", w.writerID, len(records))

	// Create a new record builder for each commit
	schema := w.recordBuilder.Schema()
	recordBuilder := array.NewRecordBuilder(w.allocator, schema)
	defer recordBuilder.Release()

	// Reserve space for all records
	recordBuilder.Reserve(len(records))

	// Get all field builders
	numFields := len(schema.Fields())
	fieldBuilders := make([]array.Builder, numFields)
	for i := 0; i < numFields; i++ {
		fieldBuilders[i] = recordBuilder.Field(i)
	}

	// Process records - converting the data into arrow format
	for _, record := range records {
		// Process each field in the schema to ensure consistent counts
		for i := 0; i < numFields; i++ {
			fieldBuilder := fieldBuilders[i]
			fieldName := schema.Field(i).Name

			value, exists := record.Data[fieldName]
			if !exists || value == nil {
				fieldBuilder.AppendNull()
				continue
			}

			// Convert and append the value
			switch builder := fieldBuilder.(type) {
			case *array.StringBuilder:
				if strVal, ok := toString(value); ok {
					builder.Append(strVal)
				} else {
					builder.Append("")
				}
			case *array.Int32Builder:
				if intVal, ok := toInt32(value); ok {
					builder.Append(intVal)
				} else {
					builder.Append(0)
				}
			case *array.Int64Builder:
				if intVal, ok := toInt64(value); ok {
					builder.Append(intVal)
				} else {
					builder.Append(0)
				}
			case *array.Float32Builder:
				if floatVal, ok := toFloat32(value); ok {
					builder.Append(floatVal)
				} else {
					builder.Append(0.0)
				}
			case *array.Float64Builder:
				if floatVal, ok := toFloat64(value); ok {
					builder.Append(floatVal)
				} else {
					builder.Append(0.0)
				}
			case *array.BooleanBuilder:
				if boolVal, ok := value.(bool); ok {
					builder.Append(boolVal)
				} else {
					builder.Append(false)
				}
			case *array.TimestampBuilder:
				if timeVal, ok := value.(time.Time); ok {
					builder.Append(arrow.Timestamp(timeVal.UnixMicro()))
				} else if strVal, ok := value.(string); ok {
					if timeVal, err := time.Parse(time.RFC3339, strVal); err == nil {
						builder.Append(arrow.Timestamp(timeVal.UnixMicro()))
					} else {
						builder.Append(arrow.Timestamp(time.Now().UnixMicro()))
					}
				} else {
					builder.Append(arrow.Timestamp(time.Now().UnixMicro()))
				}
			default:
				fieldBuilder.AppendNull()
			}
		}
	}

	// Create the record - a structured, columnar representation of the data
	arrowRecord := recordBuilder.NewRecord()
	defer arrowRecord.Release()

	// Create Arrow table
	arrowTable := array.NewTableFromRecords(arrowRecord.Schema(), []arrow.Record{arrowRecord})
	defer arrowTable.Release()

	ctx := context.Background()

	// Step 3: Inside the lock, reload the latest table state
	props := iceberg.Properties{
		iceio.S3Region:          w.config.AwsRegion,
		iceio.S3AccessKeyID:     w.config.AwsAccessKey,
		iceio.S3SecretAccessKey: w.config.AwsSecretKey,
		iceio.S3EndpointURL:     w.config.S3Endpoint,
		"s3.path-style-access":  "true",
	}
	
	// Reloading the table to get the latest state
	var err error
	w.iceTable, err = w.catalog.LoadTable(ctx, w.tableIdent, props)
	if err != nil {
		return fmt.Errorf("failed to reload table: %v", err)
	}

	logger.Infof("[%s] Loaded latest table state inside commit", w.writerID)

	// Create a new transaction
	txn := w.iceTable.NewTransaction()
	if txn == nil {
		return fmt.Errorf("failed to create transaction (txn is nil)")
	}

	// Create a record reader from the Arrow table
	batchSize := int64(arrowRecord.NumRows())
	if batchSize == 0 {
		batchSize = 1000 // default batch size
	}
	
	rdr := array.NewTableReader(arrowTable, batchSize)
	defer rdr.Release()

	// Use the context-aware Append method for iceberg-go v0.3.0
	err = txn.Append(ctx, rdr, iceberg.Properties{})
	if err != nil {
		return fmt.Errorf("failed to append data to transaction: %v", err)
	}

	// Try to commit the transaction
	updatedTable, err := txn.Commit(ctx)
	if err != nil {
		// Check if it's a commit conflict
		if strings.Contains(err.Error(), "CommitFailedException") || 
			strings.Contains(err.Error(), "concurrent") ||
			strings.Contains(err.Error(), "conflict") ||
			strings.Contains(err.Error(), "branch main has changed") {
			logger.Warnf("[%s] Commit failed due to concurrent modification: %v", 
				w.writerID, err)
		}
		
		// For other errors, check if it's a catalog/connection issue
		if strings.Contains(err.Error(), "Failed to get table") ||
			strings.Contains(err.Error(), "catalog") ||
			strings.Contains(err.Error(), "UncheckedSQLException") {
			logger.Warnf("[%s] Catalog/connection error: %v", 
				w.writerID, err)
		}

		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	w.iceTable = updatedTable
	logger.Infof("[%s] Successfully committed transaction with %d records using Append", w.writerID, len(records))

	return nil
}