package parquet

import (
	"github.com/datazip-inc/olake/utils"
)

type Config struct {
	Path      string `json:"local_path,omitempty"` // Local file path (for local file system usage)
	Bucket    string `json:"s3_bucket,omitempty"`
	Region    string `json:"s3_region,omitempty"`
	AccessKey string `json:"s3_access_key,omitempty"`
	SecretKey string `json:"s3_secret_key,omitempty"`
	Prefix    string `json:"s3_path,omitempty"`
	// File size control options
	FileSizeMB    int `json:"file_size_mb,omitempty"`    // Target file size in MB (default: 512)
	MinFileSizeMB int `json:"min_file_size_mb,omitempty"` // Minimum file size in MB (default: 256)
	MaxRows       int `json:"max_rows,omitempty"`        // Maximum rows per file (default: 1000000)
}

func (c *Config) Validate() error {
	// Set default values if not provided
	if c.FileSizeMB == 0 {
		c.FileSizeMB = 512 // Default: 512MB
	}
	if c.MinFileSizeMB == 0 {
		c.MinFileSizeMB = 256 // Default: 256MB
	}
	if c.MaxRows == 0 {
		c.MaxRows = 1000000 // Default: 1M rows
	}
	
	return utils.Validate(c)
}
