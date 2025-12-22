package driver

import (
	"fmt"
	"strings"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/pkg/parser"
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
	// ===== AWS Connection Configuration =====
	BucketName      string `json:"bucket_name"`
	Region          string `json:"region"`
	PathPrefix      string `json:"path_prefix"`
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	Endpoint        string `json:"endpoint"` // Optional: for S3-compatible services like MinIO

	// ===== File Format Configuration =====
	FileFormat  FileFormat      `json:"file_format"`
	Compression CompressionType `json:"compression"`

	// ===== Performance & Filtering Configuration =====
	MaxThreads  int    `json:"max_threads"`  // Number of concurrent file processors
	RetryCount  int    `json:"retry_count"`  // Number of retries for failed operations
	FilePattern string `json:"file_pattern"` // Regex pattern to filter files (optional)

	// ===== Stream Grouping Configuration =====
	// Stream grouping is always enabled at level 1 (first folder after path_prefix)
	// StreamPattern for custom grouping can be added in Phase 2
	StreamPattern string `json:"stream_pattern"` // Regex pattern for custom grouping (Phase 2)

	// ===== Format-Specific Parser Configurations =====
	CSV     *parser.CSVConfig     `json:"csv,omitempty"`
	JSON    *parser.JSONConfig    `json:"json,omitempty"`
	Parquet *parser.ParquetConfig `json:"parquet,omitempty"`
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

	// Format-specific validation and defaults
	if c.FileFormat == FormatCSV && c.CSV == nil {
		// Initialize with defaults if not provided
		c.CSV = &parser.CSVConfig{
			Delimiter:      ",",
			HasHeader:      true,
			SkipRows:       0,
			QuoteCharacter: "\"",
		}
	}

	if c.FileFormat == FormatJSON && c.JSON == nil {
		// Initialize with defaults if not provided
		c.JSON = &parser.JSONConfig{
			LineDelimited: true,
		}
	}

	if c.FileFormat == FormatParquet && c.Parquet == nil {
		// Initialize with defaults if not provided
		c.Parquet = &parser.ParquetConfig{
			StreamingEnabled: true,
		}
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

	return nil
}

// FileObject represents a file object in S3
type FileObject struct {
	FileKey      string `json:"file_key"`
	Size         int64  `json:"size"`
	LastModified string `json:"last_modified"`
	ETag         string `json:"etag"`
}

// GetCSVConfig returns the CSV parser configuration
func (c *Config) GetCSVConfig() *parser.CSVConfig {
	return c.CSV
}

// GetJSONConfig returns the JSON parser configuration
func (c *Config) GetJSONConfig() *parser.JSONConfig {
	return c.JSON
}

// GetParquetConfig returns the Parquet parser configuration
func (c *Config) GetParquetConfig() *parser.ParquetConfig {
	return c.Parquet
}
