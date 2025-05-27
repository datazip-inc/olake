package olake

import (
	// "context"
	// "errors"
	"fmt"
	"strconv"
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
	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"

	// "github.com/datazip-inc/olake/typeutils"
	// "github.com/google/uuid"
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

// Setup initializes the writer
// func (w *NewIcebergGo) Setup(stream protocol.Stream, options *protocol.Options) error {
// 	w.stream = stream
// 	w.records = make([]types.RawRecord, 0, w.config.BatchSize)
// 	w.allocator = memory.NewGoAllocator()

// 	// Log configuration details
// 	logger.Infof("Setting up ICEBERGGO writer with catalog %s at %s",
// 		w.config.CatalogType, w.config.RestCatalogURL)
// 	logger.Infof("S3 endpoint: %s, region: %s", w.config.S3Endpoint, w.config.AwsRegion)
// 	logger.Infof("Iceberg DB: %s, namespace: %s", w.config.IcebergDB, w.config.Namespace)
// 	logger.Infof("Handling nil values with appropriate default values (0 for numbers, empty string for text)")

// 	ctx := context.Background()

// 	// Configure S3 client
// 	s3Endpoint := w.config.S3Endpoint
// 	if s3Endpoint == "" {
// 		return fmt.Errorf("s3_endpoint is required")
// 	}

// 	// Create S3 client
// 	w.s3Client = s3.New(s3.Options{
// 		Region: w.config.AwsRegion,
// 		Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(
// 			w.config.AwsAccessKey,
// 			w.config.AwsSecretKey,
// 			"",
// 		)),
// 		EndpointResolver: s3.EndpointResolverFunc(func(region string, options s3.EndpointResolverOptions) (aws.Endpoint, error) {
// 			return aws.Endpoint{
// 				URL:               s3Endpoint,
// 				HostnameImmutable: true,
// 				SigningRegion:     w.config.AwsRegion,
// 			}, nil
// 		}),
// 		UsePathStyle: w.config.S3PathStyle,
// 	})

// 	// Create REST catalog with proper S3 properties
// 	props := iceberg.Properties{
// 		iceio.S3Region:          w.config.AwsRegion,
// 		iceio.S3AccessKeyID:     w.config.AwsAccessKey,
// 		iceio.S3SecretAccessKey: w.config.AwsSecretKey,
// 		iceio.S3EndpointURL:     w.config.S3Endpoint,
// 		"warehouse":             "s3://warehouse/iceberg/",
// 	}

// 	if w.config.S3PathStyle {
// 		props[iceio.S3ForceVirtualAddressing] = "false"
// 	}

// 	restCatalog, err := rest.NewCatalog(
// 		ctx,
// 		"olake-catalog",
// 		w.config.RestCatalogURL,
// 		rest.WithOAuthToken(""), // Add token if needed
// 	)

// 	if err != nil {
// 		return fmt.Errorf("failed to create REST catalog: %v", err)
// 	}

// 	w.catalog = restCatalog

// 	// Create schema from stream schema
// 	schemaFields, err := w.createIcebergSchema()
// 	if err != nil {
// 		return fmt.Errorf("failed to create iceberg schema: %v", err)
// 	}

// 	// Create schema with ID 0 (since it will be assigned by Iceberg)
// 	w.schema = iceberg.NewSchema(0, schemaFields...)

// 	// Set up table identifier
// 	w.tableIdent = catalog.ToIdentifier(w.config.Namespace, w.stream.Name())

// 	// Load table if it exists, otherwise create it
// 	var tableExists bool
// 	_, err = w.catalog.LoadTable(ctx, w.tableIdent, props)
// 	if err != nil {
// 		if errors.Is(err, catalog.ErrNoSuchTable) {
// 			tableExists = false
// 			logger.Infof("Table %s does not exist", w.tableIdent)
// 		} else {
// 			return fmt.Errorf("failed to check if table exists: %v", err)
// 		}
// 	} else {
// 		tableExists = true
// 		logger.Infof("Table %s exists", w.tableIdent)
// 	}

// 	if !tableExists {
// 		if !w.config.CreateTableIfNotExists {
// 			return fmt.Errorf("table %s does not exist and create_table_if_not_exists is false", w.tableIdent)
// 		}

// 		logger.Infof("Creating new Iceberg table: %s", w.tableIdent)
// 		w.iceTable, err = w.catalog.CreateTable(ctx, w.tableIdent, w.schema, catalog.WithProperties(iceberg.Properties{
// 			"write.format.default": "parquet",
// 		}))
// 		if err != nil {
// 			return fmt.Errorf("failed to create table: %v", err)
// 		}
// 	} else {
// 		logger.Infof("Loading existing Iceberg table: %s", w.tableIdent)
// 		w.iceTable, err = w.catalog.LoadTable(ctx, w.tableIdent, props)
// 		if err != nil {
// 			return fmt.Errorf("failed to load table: %v", err)
// 		}
// 	}

// 	// Initialize record builder
// 	w.createRecordBuilder()

// 	return nil
// }

// createIcebergSchema converts the stream schema to Iceberg schema fields
// func (w *NewIcebergGo) createIcebergSchema() ([]iceberg.NestedField, error) {
// 	if w.stream == nil || w.stream.Schema() == nil {
// 		return nil, fmt.Errorf("stream or schema is nil")
// 	}

// 	streamSchema := w.stream.Schema()
// 	fields := make([]iceberg.NestedField, 0)

// 	// Create the schema mapping
// 	w.schemaMapping = make(map[string]int)

// 	// Add olake_id field first with optional type
// 	fields = append(fields, iceberg.NestedField{
// 		Name:     "_olake_id",
// 		Type:     iceberg.StringType{},
// 		Required: false,
// 		Doc:      "Unique identifier for the record",
// 	})
// 	w.schemaMapping["_olake_id"] = 0

// 	// Add data fields
// 	streamSchema.Properties.Range(func(key, value interface{}) bool {
// 		name := key.(string)
// 		// Skip _olake_id if it's already in the schema
// 		if name == "_olake_id" {
// 			return true
// 		}
// 		property := value.(*types.Property)

// 		var fieldType iceberg.Type

// 		// Map olake types to iceberg types
// 		dataType := property.DataType()
// 		switch dataType {
// 		case types.Int32:
// 			fieldType = iceberg.Int32Type{}
// 		case types.Int64:
// 			fieldType = iceberg.Int64Type{}
// 		case types.Float32:
// 			fieldType = iceberg.Float32Type{}
// 		case types.Float64:
// 			fieldType = iceberg.Float64Type{}
// 		case types.String:
// 			fieldType = iceberg.StringType{}
// 		case types.Bool:
// 			fieldType = iceberg.BooleanType{}
// 		case types.Timestamp, types.TimestampMilli, types.TimestampMicro, types.TimestampNano:
// 			fieldType = iceberg.TimestampType{}
// 		default:
// 			fieldType = iceberg.StringType{}
// 		}

// 		fields = append(fields, iceberg.NestedField{
// 			Name:     name,
// 			Type:     fieldType,
// 			Required: false,
// 		})

// 		w.schemaMapping[name] = len(fields) - 1
// 		return true
// 	})

// 	// Add metadata fields
// 	fields = append(fields, iceberg.NestedField{
// 		Name:     "_olake_timestamp",
// 		Type:     iceberg.TimestampType{},
// 		Required: true,
// 		Doc:      "Timestamp when the record was processed",
// 	})
// 	w.schemaMapping["_olake_timestamp"] = len(fields) - 1

// 	fields = append(fields, iceberg.NestedField{
// 		Name:     "_op_type",
// 		Type:     iceberg.StringType{},
// 		Required: true,
// 		Doc:      "Operation type (insert, update, delete)",
// 	})
// 	w.schemaMapping["_op_type"] = len(fields) - 1

// 	fields = append(fields, iceberg.NestedField{
// 		Name:     "_cdc_timestamp",
// 		Type:     iceberg.TimestampType{},
// 		Required: true,
// 		Doc:      "CDC timestamp from the source",
// 	})
// 	w.schemaMapping["_cdc_timestamp"] = len(fields) - 1

// 	// Log the schema mapping for debugging
// 	logger.Infof("Schema mapping: %v", w.schemaMapping)

// 	return fields, nil
// }

// createRecordBuilder initializes the Arrow record builder
// func (w *NewIcebergGo) createRecordBuilder() {
// 	// Convert Iceberg schema to Arrow schema
// 	// Create mapping for metadata
// 	metadata := make(map[string]string)
// 	metadata["_olake_id"] = "string" // Explicitly add _olake_id to metadata

// 	// Log the schema for debugging
// 	logger.Infof("Schema fields: %v", w.schema.Fields())

// 	// Create a simple Arrow schema directly from the Iceberg schema fields
// 	fields := make([]arrow.Field, 0, len(w.schema.Fields()))

// 	// Add _olake_id field first
// 	fields = append(fields, arrow.Field{
// 		Name:     "_olake_id",
// 		Type:     arrow.BinaryTypes.String,
// 		Nullable: true,
// 		Metadata: arrow.NewMetadata([]string{"_olake_id"}, []string{"string"}),
// 	})

// 	// Add the rest of the fields
// 	for _, field := range w.schema.Fields() {
// 		if field.Name == "_olake_id" {
// 			continue // Skip _olake_id as we already added it
// 		}

// 		var arrowType arrow.DataType
// 		switch field.Type.(type) {
// 		case iceberg.Int32Type:
// 			arrowType = arrow.PrimitiveTypes.Int32
// 		case iceberg.Int64Type:
// 			arrowType = arrow.PrimitiveTypes.Int64
// 		case iceberg.Float32Type:
// 			arrowType = arrow.PrimitiveTypes.Float32
// 		case iceberg.Float64Type:
// 			arrowType = arrow.PrimitiveTypes.Float64
// 		case iceberg.StringType:
// 			arrowType = arrow.BinaryTypes.String
// 		case iceberg.BooleanType:
// 			arrowType = arrow.FixedWidthTypes.Boolean
// 		case iceberg.TimestampType:
// 			arrowType = arrow.FixedWidthTypes.Timestamp_us
// 		default:
// 			arrowType = arrow.BinaryTypes.String
// 		}

// 		fields = append(fields, arrow.Field{
// 			Name:     field.Name,
// 			Type:     arrowType,
// 			Nullable: !field.Required,
// 		})
// 	}

// 	// Create the Arrow schema
// 	arrowSchema := arrow.NewSchema(fields, nil)

// 	// Log the Arrow schema for debugging
// 	logger.Infof("Arrow schema: %v", arrowSchema)

// 	// Create a new record builder with the arrow schema
// 	w.recordBuilder = array.NewRecordBuilder(w.allocator, arrowSchema)
// }

// Write handles writing a record
// func (w *NewIcebergGo) Write(_ context.Context, record types.RawRecord) error {
// 	w.recordsMutex.Lock()
// 	defer w.recordsMutex.Unlock()

// 	// Add the record to the batch
// 	w.records = append(w.records, record)

// 	// If we've reached the batch size, flush the records
// 	if len(w.records) >= w.config.BatchSize {
// 		return w.flushRecords()
// 	}

// 	return nil
// }

// flushRecords writes the accumulated records to Iceberg
// func (w *NewIcebergGo) flushRecords() error {
// 	if len(w.records) == 0 {
// 		return nil
// 	}

// 	logger.Infof("Flushing %d records to Iceberg", len(w.records))

// 	// Reset the record builder
// 	w.recordBuilder.Reserve(len(w.records))

// 	// Get field builders for metadata fields
// 	olakeIDBuilder := w.recordBuilder.Field(0).(*array.StringBuilder) // _olake_id is always at index 0
// 	olakeTimestampBuilder := w.recordBuilder.Field(w.schemaMapping["_olake_timestamp"] - 1).(*array.TimestampBuilder)
// 	opTypeBuilder := w.recordBuilder.Field(w.schemaMapping["_op_type"] - 1).(*array.StringBuilder)
// 	cdcTimestampBuilder := w.recordBuilder.Field(w.schemaMapping["_cdc_timestamp"] - 1).(*array.TimestampBuilder)

// 	// Map of field builders for data fields
// 	fieldBuilders := make(map[string]array.Builder)

// 	// Initialize field builders for all columns
// 	for name, id := range w.schemaMapping {
// 		if name != "_olake_id" && name != "_olake_timestamp" && name != "_op_type" && name != "_cdc_timestamp" {
// 			fieldBuilders[name] = w.recordBuilder.Field(id - 1)
// 		}
// 	}

// 	// Populate the record builder with data
// 	for _, record := range w.records {
// 		// Add metadata fields
// 		if record.OlakeID == "" {
// 			record.OlakeID = uuid.New().String()
// 		}
// 		olakeIDBuilder.Append(record.OlakeID)
// 		olakeTimestampBuilder.Append(arrow.Timestamp(record.OlakeTimestamp.UnixMicro()))
// 		opTypeBuilder.Append(record.OperationType)
// 		cdcTimestampBuilder.Append(arrow.Timestamp(record.CdcTimestamp.UnixMicro()))

// 		// Add data fields
// 		for fieldName, fieldBuilder := range fieldBuilders {
// 			value, exists := record.Data[fieldName]
// 			if !exists || value == nil {
// 				fieldBuilder.AppendNull()
// 				continue
// 			}

// 			// Append the value based on the builder type
// 			switch builder := fieldBuilder.(type) {
// 			case *array.Int32Builder:
// 				if intVal, ok := toInt32(value); ok {
// 					builder.Append(intVal)
// 				} else {
// 					builder.Append(0)
// 				}
// 			case *array.Int64Builder:
// 				if intVal, ok := toInt64(value); ok {
// 					builder.Append(intVal)
// 				} else {
// 					builder.Append(0)
// 				}
// 			case *array.Float32Builder:
// 				if floatVal, ok := toFloat32(value); ok {
// 					builder.Append(floatVal)
// 				} else {
// 					builder.Append(0.0)
// 				}
// 			case *array.Float64Builder:
// 				if floatVal, ok := toFloat64(value); ok {
// 					builder.Append(floatVal)
// 				} else {
// 					builder.Append(0.0)
// 				}
// 			case *array.BooleanBuilder:
// 				if boolVal, ok := value.(bool); ok {
// 					builder.Append(boolVal)
// 				} else {
// 					builder.Append(false)
// 				}
// 			case *array.StringBuilder:
// 				if strVal, ok := toString(value); ok {
// 					builder.Append(strVal)
// 				} else {
// 					builder.Append("")
// 				}
// 			case *array.TimestampBuilder:
// 				if timeVal, ok := value.(time.Time); ok {
// 					builder.Append(arrow.Timestamp(timeVal.UnixMicro()))
// 				} else if strVal, ok := value.(string); ok {
// 					if timeVal, err := time.Parse(time.RFC3339, strVal); err == nil {
// 						builder.Append(arrow.Timestamp(timeVal.UnixMicro()))
// 					} else {
// 						builder.Append(arrow.Timestamp(time.Now().UnixMicro()))
// 					}
// 				} else {
// 					builder.Append(arrow.Timestamp(time.Now().UnixMicro()))
// 				}
// 			default:
// 				if strVal, ok := toString(value); ok {
// 					if strBuilder, ok := builder.(*array.StringBuilder); ok {
// 						strBuilder.Append(strVal)
// 					} else {
// 						builder.AppendNull()
// 					}
// 				} else {
// 					builder.AppendNull()
// 				}
// 			}
// 		}
// 	}

// 	// Create record batch
// 	record := w.recordBuilder.NewRecord()
// 	defer record.Release()

// 	// Create an Arrow table from the record
// 	arrowTable := array.NewTableFromRecords(record.Schema(), []arrow.Record{record})
// 	defer arrowTable.Release()

// 	// Get a context
// 	ctx := context.Background()

// 	// Maximum number of retries
// 	maxRetries := 3
// 	var lastErr error

// 	for attempt := 0; attempt < maxRetries; attempt++ {
// 		// Create a transaction
// 		txn := w.iceTable.NewTransaction()

// 		// Generate a unique file path for this batch
// 		fileUUID := uuid.New().String()
// 		filePath := fmt.Sprintf("%s/%s/data-%s.parquet",
// 			w.iceTable.Location(), w.stream.Name(), fileUUID)

// 		// Write record to a parquet file using the table's filesystem
// 		writeFileIO, ok := w.iceTable.FS().(iceio.WriteFileIO)
// 		if !ok {
// 			return fmt.Errorf("filesystem does not support writing")
// 		}

// 		// Create parquet file with the record
// 		fw, err := writeFileIO.Create(filePath)
// 		if err != nil {
// 			return fmt.Errorf("failed to create parquet file: %v", err)
// 		}

// 		// Write the record to parquet with default properties and without field IDs
// 		props := pqarrow.DefaultWriterProps()
// 		// Note: We can't disable field IDs directly, but we can use a simpler schema conversion

// 		if err := pqarrow.WriteTable(arrowTable, fw, record.NumRows(), nil, props); err != nil {
// 			fw.Close()
// 			return fmt.Errorf("failed to write record to parquet: %v", err)
// 		}

// 		if err := fw.Close(); err != nil {
// 			return fmt.Errorf("failed to close parquet file: %v", err)
// 		}

// 		// Add the file to the transaction with append operation
// 		icebergProps := iceberg.Properties{
// 			"operation": "append",
// 			"format":    "parquet",
// 		}

// 		err = txn.AddFiles([]string{filePath}, icebergProps, false)
// 		if err != nil {
// 			return fmt.Errorf("failed to add file to transaction: %v", err)
// 		}

// 		// Try to commit the transaction
// 		updatedTable, err := txn.Commit(ctx)
// 		if err != nil {
// 			lastErr = err
// 			// Check if it's a commit conflict
// 			if strings.Contains(err.Error(), "CommitFailedException") {
// 				logger.Warnf("Commit failed due to concurrent modification (attempt %d/%d), retrying...", attempt+1, maxRetries)
// 				// Reload the table to get the latest metadata
// 				w.iceTable, err = w.catalog.LoadTable(ctx, w.tableIdent, iceberg.Properties{})
// 				if err != nil {
// 					return fmt.Errorf("failed to reload table after commit conflict: %v", err)
// 				}
// 				// Wait a bit before retrying
// 				time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
// 				continue
// 			}
// 			return fmt.Errorf("failed to commit transaction: %v", err)
// 		}

// 		// Update our table reference
// 		w.iceTable = updatedTable

// 		// Clear the batch
// 		w.records = w.records[:0]

// 		return nil
// 	}

// 	return fmt.Errorf("failed to commit after %d attempts, last error: %v", maxRetries, lastErr)
// }

// Close handles cleanup
func (w *NewIcebergGo) Close() error {
	logger.Infof("Closing ICEBERGGO writer", w.writerID)

	// Flush any remaining records
	w.recordsMutex.Lock()
	defer w.recordsMutex.Unlock()

	if len(w.records) > 0 {
		err := w.flushRecords()
		if err != nil {
			return fmt.Errorf("failed to flush records during close: %v", err)
		}
	}

	// Clean up resources
	if w.recordBuilder != nil {
		w.recordBuilder.Release()
	}
	if w.allocator != nil {
		w.allocator = nil
	}

	return nil
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
