package parquet

import (
	"fmt"

	"github.com/datazip-inc/olake/utils"
)

type Config struct {
	Path      string `json:"local_path,omitempty"` // Local file path (for local file system usage)
	Bucket    string `json:"s3_bucket,omitempty"`
	Region    string `json:"s3_region,omitempty"`
	AccessKey string `json:"s3_access_key,omitempty"`
	SecretKey string `json:"s3_secret_key,omitempty"`
	Prefix    string `json:"s3_path,omitempty"`
	// S3 endpoint for custom S3-compatible services (like MinIO)
	S3Endpoint string `json:"s3_endpoint,omitempty"`

	// Parquet file optimization settings
	Compression  string `json:"compression,omitempty"`   // Compression codec: snappy (default), gzip, zstd, lz4, none
	MaxFileSize  int64  `json:"max_file_size,omitempty"` // Maximum file size in bytes (default: 128MB)
	RowGroupSize int    `json:"row_group_size,omitempty"` // Row group size (default: 128MB worth of rows)
	SortColumns  string `json:"sort_columns,omitempty"`  // Comma-separated list of columns to sort by
}

func (c *Config) Validate() error {
	// Validate compression codec if specified
	if c.Compression != "" {
		validCompressions := []string{"snappy", "gzip", "zstd", "lz4", "none", "uncompressed"}
		valid := false
		for _, v := range validCompressions {
			if c.Compression == v {
				valid = true
				break
			}
		}
		if !valid {
			return fmt.Errorf("invalid compression codec: %s. Valid options are: snappy, gzip, zstd, lz4, none, uncompressed", c.Compression)
		}
	}

	// Validate max file size if specified
	if c.MaxFileSize < 0 {
		return fmt.Errorf("max_file_size must be a positive value")
	}

	// Validate row group size if specified
	if c.RowGroupSize < 0 {
		return fmt.Errorf("row_group_size must be a positive value")
	}

	return utils.Validate(c)
}
