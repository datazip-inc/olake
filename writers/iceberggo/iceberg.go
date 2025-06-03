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
	"github.com/apache/iceberg-go/config"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/google/uuid"
)

var commitMutexes sync.Map
var thread int = 1

func (w *NewIcebergGo) Setup(stream protocol.Stream, options *protocol.Options) error {
	setupStartTime := time.Now()
	w.stream = stream
	w.allocator = memory.NewGoAllocator()
	w.writerID = fmt.Sprintf("Thread %d : writer-%s", thread, uuid.New().String()[:8])
	thread++
	logger.Infof("Namespace: %s", w.config.Namespace)
	w.configHash = getConfigHash(w.config.Namespace, w.stream.Name(), true)
	config.EnvConfig.MaxWorkers = 20 // max workers for iceberg go
	logger.Infof("Set Iceberg max workers to %d", config.EnvConfig.MaxWorkers)

	serverInstance, err := getOrCreateServerInstance(w.configHash, w.config)
	if err == nil && serverInstance.catalog != nil && serverInstance.iceTable != nil {
		logger.Infof("[%s] Reusing existing server instance for configHash: %s", w.writerID, w.configHash)
		w.catalog = serverInstance.catalog
		w.iceTable = serverInstance.iceTable
		w.tableIdent = catalog.ToIdentifier(w.config.Namespace, w.stream.Name())

		w.partitionInfo = make(map[string]string)
		if stream.Self().StreamMetadata.PartitionRegex != "" {
			err := w.parsePartitionRegex(stream.Self().StreamMetadata.PartitionRegex)
			if err != nil {
				return fmt.Errorf("failed to parse partition regex: %v", err)
			}
		}

		if w.iceTable != nil {
			w.schema = w.iceTable.Schema()
			w.createRecordBuilder()
		}

		logger.Infof("Initialized writer with ID: %s, ConfigHash: %s (reused server)", w.writerID, w.configHash)

		if serverInstance != nil {
			serverInstance.catalog = w.catalog
			serverInstance.iceTable = w.iceTable
			logger.Infof("[%s] Stored catalog and table in server instance for reuse", w.writerID)
		}
		logger.Infof("Setup with existing server completed in %v", time.Since(setupStartTime))
		return nil
	}

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
		UsePathStyle:     w.config.S3PathStyle,
		ClientLogMode:    aws.LogRetries,
		RetryMaxAttempts: 1,
	})

	logger.Infof("Testing S3 connectivity to %s", s3Endpoint)
	_, listErr := w.s3Client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if listErr != nil {
		logger.Errorf("S3 connectivity test failed: %v", listErr)
		return fmt.Errorf("S3 connectivity test failed, please check your credentials and endpoint: %v", listErr)
	}
	logger.Infof("S3 connectivity test successful")

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

	catalogStartTime := time.Now()
	restCatalog, err := rest.NewCatalog(
		ctx,
		"olake-catalog",
		w.config.RestCatalogURL,
		rest.WithAdditionalProps(catalogProps),
		rest.WithOAuthToken(""),
	)
	logger.Infof("REST catalog creation took %v", time.Since(catalogStartTime))

	if err != nil {
		return fmt.Errorf("failed to create REST catalog: %v", err)
	}

	w.catalog = restCatalog

	schemaStartTime := time.Now()
	schemaFields, err := w.createIcebergSchema2()
	logger.Infof("Schema creation took %v", time.Since(schemaStartTime))

	w.schema = iceberg.NewSchema(0, schemaFields...)
	w.tableIdent = catalog.ToIdentifier(w.config.Namespace, w.stream.Name())

	tableLoadStartTime := time.Now()
	var tableExists bool
	_, err = w.catalog.LoadTable(ctx, w.tableIdent, catalogProps)
	logger.Infof("Table load check took %v", time.Since(tableLoadStartTime))

	if err != nil {
		if errors.Is(err, catalog.ErrNoSuchTable) {
			tableExists = false
			logger.Infof("Table %s does not exist", w.tableIdent)
		} else {
			return fmt.Errorf("failed to load table: %v", err)
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
		tableCreateStartTime := time.Now()
		w.iceTable, err = w.catalog.CreateTable(ctx, w.tableIdent, w.schema, catalog.WithProperties(iceberg.Properties{
			"write.format.default":  "parquet",
			iceio.S3Region:          w.config.AwsRegion,
			iceio.S3AccessKeyID:     w.config.AwsAccessKey,
			iceio.S3SecretAccessKey: w.config.AwsSecretKey,
			iceio.S3EndpointURL:     w.config.S3Endpoint,
			"s3.path-style-access":  "true",
		}))
		logger.Infof("Table creation took %v", time.Since(tableCreateStartTime))
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

	w.createRecordBuilder()

	if serverInstance != nil {
		serverInstance.catalog = w.catalog
		serverInstance.iceTable = w.iceTable
		logger.Infof("[%s] Stored catalog and table in server instance for reuse", w.writerID)
	}

	logger.Infof("## Server instance: %v", len(serverRegistry))
	logger.Infof("Total setup time: %v", time.Since(setupStartTime))

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
	// startTime := time.Now()

	recordSize := int64(estimateRecordSize(record))

	buffer := getLocalBuffer(w.configHash)
	buffer.records = append(buffer.records, record)
	buffer.size += recordSize

	if buffer.size >= localBufferThreshold {
		err := w.flushLocalBuffer(buffer)
		// logger.Infof("Write operation took %v", time.Since(startTime))
		return err
	}

	// logger.Infof("Write operation took %v", time.Since(startTime))

	return nil
}

func (w *NewIcebergGo) flushLocalBuffer(buffer *LocalBuffer) error {
	length := len(buffer.records)
	if length == 0 {
		return nil
	}

	startTime := time.Now()

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

	logger.Infof("[%v] FlushLocalBuffer took %v : %v records", w.writerID, time.Since(startTime), length)

	return nil
}

func getCommitMutex(configHash string) *sync.Mutex {
	mutex, _ := commitMutexes.LoadOrStore(configHash, &sync.Mutex{})
	count := 0
	commitMutexes.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	logger.Infof("Number of commit mutexes: %d", count)
	return mutex.(*sync.Mutex)
}

func (w *NewIcebergGo) flushRecordsBatch(records []types.RawRecord) error {
	startTime := time.Now()

	if len(records) == 0 {
		return nil
	}

	mutex := getCommitMutex(w.configHash)
	mutexAcquireTime := time.Now()
	mutex.Lock()
	logger.Infof("Mutex acquisition took %v", time.Since(mutexAcquireTime))
	defer mutex.Unlock()

	if w.recordBuilder == nil {
		return fmt.Errorf("record builder is nil")
	}

	if w.iceTable == nil {
		return fmt.Errorf("iceberg table is nil")
	}

	logger.Infof("[%s] Committing %d records to Iceberg", w.writerID, len(records))

	schemaSetupTime := time.Now()
	schema := w.recordBuilder.Schema()
	numFields := len(schema.Fields())
	columnData := make([][]string, numFields)
	columnValidity := make([][]bool, numFields)
	logger.Infof("Schema setup took %v", time.Since(schemaSetupTime))

	startTime2 := time.Now()
	for i := 0; i < numFields; i++ {
		columnData[i] = make([]string, 0, len(records))
		columnValidity[i] = make([]bool, 0, len(records))
	}
	logger.Infof("Column arrays initialization took %v", time.Since(startTime2))

	var wg sync.WaitGroup
	wg.Add(numFields)

	startTime3 := time.Now()
	for fieldIdx := 0; fieldIdx < numFields; fieldIdx++ {
		go func(idx int) {
			defer wg.Done()
			fieldName := schema.Field(idx).Name

			data := make([]string, len(records))
			validity := make([]bool, len(records))
			
			for recIdx, record := range records {
				value, exists := record.Data[fieldName]
				if !exists || value == nil {
					validity[recIdx] = false
					continue
				}
				validity[recIdx] = true
				data[recIdx] = fmt.Sprint(value)
			}
			
			columnData[idx] = data
			columnValidity[idx] = validity
		}(fieldIdx)
	}
	wg.Wait()
	logger.Infof("Parallel data processing took %v", time.Since(startTime3))
	
	recordBuilderTime := time.Now()
	recordBuilder := array.NewRecordBuilder(w.allocator, schema)
	defer recordBuilder.Release()
	logger.Infof("Record builder creation took %v", time.Since(recordBuilderTime))

	startTime4 := time.Now()
	for i := 0; i < numFields; i++ {
		builder := recordBuilder.Field(i).(*array.StringBuilder)
		builder.Reserve(len(records))
		
		for j := 0; j < len(records); j++ {
			if columnValidity[i][j] {
				builder.Append(columnData[i][j])
			} else {
				builder.AppendNull()
			}
		}
	}
	logger.Infof("Record building took %v", time.Since(startTime4))

	arrowRecordTime := time.Now()
	arrowRecord := recordBuilder.NewRecord()
	logger.Infof("Arrow record num rows: %d", arrowRecord.NumRows())
	defer arrowRecord.Release()
	logger.Infof("Arrow record creation took %v", time.Since(arrowRecordTime))

	tableCreationTime := time.Now()
	arrowTable := array.NewTableFromRecords(arrowRecord.Schema(), []arrow.Record{arrowRecord})
	defer arrowTable.Release()
	logger.Infof("Arrow table creation took %v", time.Since(tableCreationTime))

	ctx := context.Background()
	props := iceberg.Properties{
		iceio.S3Region:          w.config.AwsRegion,
		iceio.S3AccessKeyID:     w.config.AwsAccessKey,
		iceio.S3SecretAccessKey: w.config.AwsSecretKey,
		iceio.S3EndpointURL:     w.config.S3Endpoint,
		"s3.path-style-access":  "true",          
	}

	tableLoadTime := time.Now()
	var err error
	w.iceTable, err = w.catalog.LoadTable(ctx, w.tableIdent, props)
	logger.Infof("Table reload took %v", time.Since(tableLoadTime))
	if err != nil {
		return fmt.Errorf("failed to reload table: %v", err)
	}

	logger.Infof("[%s] Loaded latest table state inside commit", w.writerID)

	transactionTime := time.Now()
	txn := w.iceTable.NewTransaction()
	if txn == nil {
		return fmt.Errorf("failed to create transaction (txn is nil)")
	}
	logger.Infof("Transaction creation took %v", time.Since(transactionTime))

	batchSize := int64(arrowRecord.NumRows())
	logger.Info("asfasdfa batchsize: %d", batchSize)
	if batchSize == 0 {
		logger.Infof("Batch size is 0, setting to 1000")
		batchSize = 1000
	}

	readerTime := time.Now()
	rdr := array.NewTableReader(arrowTable, batchSize)
	defer rdr.Release()
	logger.Infof("Table reader creation took %v", time.Since(readerTime))

	appendStartTime := time.Now()
	err = txn.Append(ctx, rdr, iceberg.Properties{}) // 16 to 17 seconds : 5 threads
	logger.Infof("Transaction append took %v", time.Since(appendStartTime))
	if err != nil {
		return fmt.Errorf("failed to append data to transaction: %v", err)
	}

	commitStartTime := time.Now()
	updatedTable, err := txn.Commit(ctx)
	logger.Infof("Transaction commit took %v", time.Since(commitStartTime))
	if err != nil {
		if strings.Contains(err.Error(), "CommitFailedException") ||
			strings.Contains(err.Error(), "concurrent") ||
			strings.Contains(err.Error(), "conflict") ||
			strings.Contains(err.Error(), "branch main has changed") {
			logger.Warnf("[%s] Commit failed due to concurrent modification: %v",
				w.writerID, err)
		}

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

	logger.Infof("Total flush operation took %v", time.Since(startTime))

	return nil
}
