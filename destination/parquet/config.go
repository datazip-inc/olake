package parquet

import (
	"github.com/datazip-inc/olake/utils"
)

const (
	DefaultMaxFileSizeMB  = 512
	DefaultMaxRowsPerFile = 1000000
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
	// The maximum file size in MB before rotation (default: 512MB)
	MaxFileSizeMB int `json:"max_file_size_mb,omitempty"`
	// The maximum number of rows per file before rotation (default: 1,000,000)
	MaxRowsPerFile int `json:"max_rows_per_file,omitempty"`
}

func (c *Config) Validate() error {
	if c.MaxFileSizeMB <= 0 {
		c.MaxFileSizeMB = DefaultMaxFileSizeMB
	}
	if c.MaxRowsPerFile <= 0 {
		c.MaxRowsPerFile = DefaultMaxRowsPerFile
	}
	return utils.Validate(c)
}
