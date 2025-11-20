package driver

import (
	"fmt"
	"strings"

	"github.com/datazip-inc/olake/constants"
)

// FileFormat represents the format of files in S3
type FileFormat string

const (
	FormatCSV     FileFormat = "csv"
	FormatJSON    FileFormat = "json"
	FormatParquet FileFormat = "parquet"
)

// CompressionType represents the compression type of files
type CompressionType string

const (
	CompressionNone CompressionType = "none"
	CompressionGzip CompressionType = "gzip"
	CompressionZip  CompressionType = "zip"
)

// Config represents the configuration for S3 source connector
type Config struct {
	// AWS Connection details
	BucketName      string `json:"bucket_name"`
	Region          string `json:"region"`
	PathPrefix      string `json:"path_prefix"`
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	Endpoint        string `json:"endpoint"` // Optional: for S3-compatible services like MinIO

	// File format configuration
	FileFormat  FileFormat      `json:"file_format"`
	Compression CompressionType `json:"compression"`

	// CSV specific configuration
	Delimiter      string `json:"delimiter"`       // Default: ","
	HasHeader      bool   `json:"has_header"`      // Default: true
	SkipRows       int    `json:"skip_rows"`       // Number of rows to skip at the beginning
	QuoteCharacter string `json:"quote_character"` // Default: "\""

	// JSON specific configuration
	JSONLineDelimited bool `json:"json_line_delimited"` // Default: true (JSONL format)

	// Performance tuning
	MaxThreads int `json:"max_threads"` // Number of concurrent file processors
	BatchSize  int `json:"batch_size"`  // Number of records to batch before sending
	RetryCount int `json:"retry_count"` // Number of retries for failed operations

	// File filtering
	FilePattern string `json:"file_pattern"` // Regex pattern to filter files (optional)

	// Stream grouping strategy
	StreamGroupingEnabled bool   `json:"stream_grouping_enabled"` // Default: true - groups files by folder
	StreamGroupingLevel   int    `json:"stream_grouping_level"`   // Folder depth for grouping (default: 1)
	StreamPattern         string `json:"stream_pattern"`          // Regex pattern for custom grouping (Phase 2)

	// Parquet streaming configuration (uses S3 range requests to avoid loading full file in memory)
	ParquetStreamingEnabled     bool `json:"parquet_streaming_enabled"`       // Default: true - use S3 range requests
	ParquetRowGroupChunkSizeMB  int  `json:"parquet_row_group_chunk_size_mb"` // Default: 64MB per row group
	ParquetFooterReadSizeKB     int  `json:"parquet_footer_read_size_kb"`     // Default: 512KB for footer
	MaxParquetRowGroupsInMemory int  `json:"max_parquet_row_groups_in_memory"` // Default: 1 - process one at a time
}

// Validate validates the S3 configuration
func (c *Config) Validate() error {
	// Validate bucket name
	if c.BucketName == "" {
		return fmt.Errorf("bucket_name is required")
	}

	// Validate region (only if not using custom endpoint)
	if c.Endpoint == "" && c.Region == "" {
		return fmt.Errorf("region is required when not using custom endpoint")
	}

	// Validate credentials - both must be provided together or omitted together
	// If omitted, the driver will fall back to default credential chain (IAM roles, env vars, etc.)
	if (c.AccessKeyID != "" && c.SecretAccessKey == "") || (c.AccessKeyID == "" && c.SecretAccessKey != "") {
		return fmt.Errorf("access_key_id and secret_access_key must be provided together or both omitted (for IAM role authentication)")
	}

	// Validate file format
	if c.FileFormat == "" {
		return fmt.Errorf("file_format is required (csv, json, or parquet)")
	}

	validFormat := false
	for _, format := range []FileFormat{FormatCSV, FormatJSON, FormatParquet} {
		if c.FileFormat == format {
			validFormat = true
			break
		}
	}
	if !validFormat {
		return fmt.Errorf("invalid file_format: must be csv, json, or parquet")
	}

	// Set default values
	if c.Compression == "" {
		c.Compression = CompressionNone
	}

	// Validate compression type
	validCompression := false
	for _, compression := range []CompressionType{CompressionNone, CompressionGzip, CompressionZip} {
		if c.Compression == compression {
			validCompression = true
			break
		}
	}
	if !validCompression {
		return fmt.Errorf("invalid compression: must be none, gzip, or zip")
	}

	// CSV specific validation
	if c.FileFormat == FormatCSV {
		if c.Delimiter == "" {
			c.Delimiter = ","
		}
		if c.QuoteCharacter == "" {
			c.QuoteCharacter = "\""
		}
		// Default to true if not specified
		if !c.HasHeader {
			c.HasHeader = true
		}
	}

	// JSON specific defaults
	if c.FileFormat == FormatJSON {
		if !c.JSONLineDelimited {
			c.JSONLineDelimited = true
		}
	}

	// Set default batch size
	if c.BatchSize <= 0 {
		c.BatchSize = 10000
	}

	// Set default thread count
	if c.MaxThreads <= 0 {
		c.MaxThreads = constants.DefaultThreadCount
	}

	// Set default retry count
	if c.RetryCount <= 0 {
		c.RetryCount = constants.DefaultRetryCount
	}

	// Normalize path prefix (remove leading/trailing slashes)
	if c.PathPrefix != "" {
		c.PathPrefix = strings.Trim(c.PathPrefix, "/")
	}

	// Stream grouping defaults
	// Default to enabled for folder-based stream grouping
	if !c.StreamGroupingEnabled {
		c.StreamGroupingEnabled = true
	}

	// Validate and set default grouping level
	if c.StreamGroupingLevel <= 0 {
		c.StreamGroupingLevel = 1
	}

	// Parquet streaming defaults (Phase 1 - Comment 2 implementation)
	// Default to enabled for memory-efficient processing
	if !c.ParquetStreamingEnabled {
		c.ParquetStreamingEnabled = true
	}

	// Default row group chunk size: 64MB (typical Parquet row group size)
	if c.ParquetRowGroupChunkSizeMB <= 0 {
		c.ParquetRowGroupChunkSizeMB = 64
	}

	// Default footer read size: 512KB (enough for most Parquet footers)
	if c.ParquetFooterReadSizeKB <= 0 {
		c.ParquetFooterReadSizeKB = 512
	}

	// Default to processing one row group at a time (minimize memory)
	if c.MaxParquetRowGroupsInMemory <= 0 {
		c.MaxParquetRowGroupsInMemory = 1
	}

	return nil
}

// FileObject represents a file object in S3
type FileObject struct {
	FileKey      string `json:"file_key"`
	Size         int64  `json:"size"`
	LastModified string `json:"last_modified"`
	ETag         string `json:"etag"`
}
