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
	// S3 endpoint for custom S3-compatible services (like MinIO)
	S3Endpoint string `json:"s3_endpoint,omitempty"`
	// Continuous streaming thresholds
	TargetFileSizeMB  int `json:"target_file_size_mb,omitempty"`
	MaxLatencySeconds int `json:"max_latency_seconds,omitempty"`
}

func (c *Config) Validate() error {
	return utils.Validate(c)
}
