package parquet

import (
	"github.com/datazip-inc/olake/utils"
)

// defaultMaxFileSizeMB is the parquet roll threshold used when MaxFileSizeMB is unset.
const (
	defaultMaxFileSizeMB     = 512
	defaultRollCheckInterval = 10000
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
	// MaxFileSizeMB rolls a partition into a new parquet file once its on-disk size reaches
	// this many MB. Fractional values are allowed (e.g. 0.125 for a 128KB roll size), which
	// keeps integration tests light. When unset (<= 0) the writer falls back to defaultMaxFileSizeMB.
	MaxFileSizeMB float64 `json:"max_file_size_mb,omitempty"`
}

func (c *Config) Validate() error {
	return utils.Validate(c)
}
