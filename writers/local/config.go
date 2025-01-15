package parquet

import (
	"github.com/datazip-inc/olake/utils"
)

type Config struct {
	Path      string `json:"path"` // Local file path (for local file system usage)
	Bucket    string `json:"s3_bucket,omitempty"`
	Region    string `json:"s3_region,omitempty"`
	AccessKey string `json:"s3_access_key,omitempty"`
	SecretKey string `json:"s3_secret_key,omitempty"`
	Prefix    string `json:"s3_prefix,omitempty"`
}

func (c *Config) Validate() error {
	return utils.Validate(c)
}
