package iceberggo

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/utils"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
)

type NewIcebergGo struct {
	options     	*protocol.Options
	config 			*Config
	stream      	protocol.Stream
	catalog    		catalog.Catalog
	iceTable   		*table.Table
	schema     		*iceberg.Schema
	tableIdent 		table.Identifier
	allocator     	memory.Allocator
	recordBuilder 	*array.RecordBuilder
	configHash 		string
	partitionInfo   map[string]string
	recordsSize     atomic.Int64
	flushing        atomic.Bool
}

type Config struct {
	CatalogType    string `json:"catalog_type"`
	RestCatalogURL string `json:"rest_catalog_url"`
	GlueDatabase   string `json:"glue_database"`   
	GlueRegion     string `json:"glue_region"`    
	S3Endpoint   string `json:"s3_endpoint"`
	S3Bucket     string `json:"s3_bucket"`
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
	TableName          string `json:"glue_table_name"`
	TableLocation          string `json:"table_location"`
	S3Prefix               string `json:"S3Prefix"`
}

var (
	arrowBuildersThreshold int64 = 1024 * 1024 * 1024 // 1GB
)

func (w *NewIcebergGo) SetupIcebergClient() error {
	err := w.config.Validate()
	if err != nil {
		return fmt.Errorf("failed to validate config: %s", err)
	}

	ctx := context.Background()
	staticCreds := aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(w.config.AwsAccessKey, w.config.AwsSecretKey, ""))
	cfg := aws.Config{
		Region: w.config.AwsRegion,
		Credentials: staticCreds,
	}
	ctx = utils.WithAwsConfig(ctx, &cfg)
	
	if w.config.CatalogType == "glue" {
		w.config.TableLocation = fmt.Sprintf("s3://%s/%s/%s", w.config.S3Bucket, w.config.S3Prefix, w.config.TableName)
		glueCatalog, err := w.initializeGlueCatalog(ctx)
		w.catalog = glueCatalog
		if err != nil {
			logger.Errorf("Failed to initialize Glue catalog: %v", err)
		}

		tbl, err := w.createOrLoadTable(ctx, glueCatalog) // need to check if it is even required or not
		w.iceTable = tbl
		if err != nil {
			logger.Errorf("Failed to create or load table: %v", err)
		}
	} else if w.config.CatalogType == "rest" {
		w.config.TableLocation = fmt.Sprintf("s3://%s/%s", w.config.S3Prefix, w.config.TableName)
		
		restCatalog, err := w.initializeRestCatalog(ctx)
		if err != nil {
			logger.Errorf("Failed to initialize Rest catalog: %v", err)
		}
		w.catalog = restCatalog
		tbl, err := w.createOrLoadTable(ctx, restCatalog)
		if err != nil {
			logger.Errorf("Failed to create or load table: %v", err)
		}
		logger.Infof("Table location: %s", tbl.Location())

		w.iceTable = tbl
	}

	w.createRecordBuilder()

	return  nil
}

func (w *NewIcebergGo) initializeRestCatalog(ctx context.Context) (catalog.Catalog, error) {
	logger.Infof("Initializing Rest catalog")
	
	props := iceberg.Properties{
		io.S3Region:          w.config.AwsRegion,
		io.S3EndpointURL:     w.config.S3Endpoint,
		io.S3AccessKeyID:     w.config.AwsAccessKey,
		io.S3SecretAccessKey: w.config.AwsSecretKey,
		"s3.path-style":      "true",
	}
	
	cat, err := rest.NewCatalog(ctx, "rest", w.config.RestCatalogURL, 
	rest.WithAdditionalProps(props),
	rest.WithWarehouseLocation("warehouse"),
	)
	return cat, err
}

func (w *NewIcebergGo) initializeGlueCatalog(ctx context.Context) (catalog.Catalog, error) {
	props := iceberg.Properties{
		"type":                   "glue",
		"glue.region":            w.config.AwsRegion, // fix it to aws region keep one only
		"glue.access-key-id":     w.config.AwsAccessKey,
		"glue.secret-access-key": w.config.AwsSecretKey,
		"warehouse":              fmt.Sprintf("s3://%s/%s", w.config.S3Bucket, w.config.S3Prefix),
		"s3.region":              w.config.AwsRegion, // keep it one only
		"s3.access-key-id":       w.config.AwsAccessKey,
		"s3.secret-access-key":   w.config.AwsSecretKey,
	}

	cat, err := catalog.Load(ctx, "glue", props)
	if err != nil {
		return nil, fmt.Errorf("failed to load Glue catalog: %w", err)
	}

	return cat, nil
}

func (w *NewIcebergGo) createOrLoadTable(ctx context.Context, cat catalog.Catalog) (*table.Table, error) {
	w.tableIdent = table.Identifier{w.config.Namespace, w.config.TableName}	

	exists, err := cat.CheckTableExists(ctx, w.tableIdent)
	if err != nil {
		return nil, fmt.Errorf("failed to check if table exists: %w", err)
	}

	if exists {
		tbl, err := cat.LoadTable(ctx, w.tableIdent, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to load existing table: %w", err)
		}
		w.schema = tbl.Schema()
		
		return tbl, nil
	}

	dbIdentifier := table.Identifier{w.config.Namespace}
	dbExists, err := cat.CheckNamespaceExists(ctx, dbIdentifier)
	if err != nil {
		return nil, fmt.Errorf("failed to check if namespace exists: %w", err)
	}

	if !dbExists {
		err = cat.CreateNamespace(ctx, dbIdentifier, iceberg.Properties{
			"description": "Database created by iceberg-go sample script",
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create namespace: %w", err)
		}
		logger.Infof("Created Namespace: %s", w.config.Namespace)
	}

	schemaFields, err := w.createIcebergSchema()
	if err != nil {
		return nil, fmt.Errorf("failed to create iceberg schema: %w", err)
	}
	w.schema = iceberg.NewSchema(0, schemaFields...)

	tbl, err := cat.CreateTable(ctx, w.tableIdent, w.schema,
		catalog.WithLocation(w.config.TableLocation),
		catalog.WithProperties(iceberg.Properties{
			"format-version": "2",
			"description":    "Sample Iceberg table created by iceberg-go",
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}
	logger.Infof("Table Created %s", w.tableIdent)

	return tbl, nil
}

func (w *NewIcebergGo) GetConfigRef() protocol.Config {
	w.config = &Config{}
	return w.config
}

func (w *NewIcebergGo) Spec() any {
	return Config{}
}

func (w *NewIcebergGo) Close() error {
	if w.recordsSize.Load() > 0 {
		logger.Infof("Close | Flushing %d remaining records on close", w.recordsSize.Load())
		flushStartTime := time.Now()
		err := w.flushArrowBuilder()
		if err != nil {
			logger.Errorf("Error flushing remaining records on close: %v", err)
			return err
		}
		logger.Infof("Final flush on close took %v", time.Since(flushStartTime))
	}

	return nil
}

func (w *NewIcebergGo) Check() error {
	if w.config.CatalogType == "" {
		w.config.CatalogType = "rest" 
	}

	if w.config.CatalogType == "rest" {
		if w.config.RestCatalogURL == "" {
			return fmt.Errorf("rest_catalog_url is required when catalog_type is 'rest'")
		}
	} else if w.config.CatalogType == "glue" {
		if w.config.GlueDatabase == "" {
			return fmt.Errorf("glue_database is required when catalog_type is 'glue'")
		}
		if w.config.GlueRegion == "" && w.config.AwsRegion == "" {
			return fmt.Errorf("glue_region or aws_region is required when catalog_type is 'glue'")
		}
	} else {
		return fmt.Errorf("unsupported catalog_type: %s. Supported types are 'rest' and 'glue'", w.config.CatalogType)
	}

	if w.config.S3Endpoint == "" {
		return fmt.Errorf("s3_endpoint is required")
	}
	if w.config.S3Bucket == "" {
		return fmt.Errorf("s3_bucket is required")
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
	if c.CatalogType == "" {
		c.CatalogType = "rest"
	}
	
	if c.CatalogType == "rest" {
		if c.RestCatalogURL == "" {
			return fmt.Errorf("rest_catalog_url is required when catalog_type is 'rest'")
		}
	} else if c.CatalogType == "glue" {
		if c.GlueDatabase == "" {
			return fmt.Errorf("glue_database is required when catalog_type is 'glue'")
		}
		if c.GlueRegion == "" {
			c.GlueRegion = c.AwsRegion
		}
	} else {
		return fmt.Errorf("unsupported catalog_type: %s. Supported types are 'rest' and 'glue'", c.CatalogType)
	}
	
	if c.S3Endpoint == "" {
		return fmt.Errorf("s3_endpoint is required")
	}
	if c.S3Bucket == "" {
		return fmt.Errorf("s3_bucket is required")
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

func getConfigHash(namespace string, streamID string, appendMode bool) string {
	hashComponents := []string{
		streamID,
		namespace,
		fmt.Sprintf("%t", appendMode),
	}
	return strings.Join(hashComponents, "-")
}