package parquet

import (
	"github.com/datazip-inc/olake/utils"
)

type Config struct {
	Path      string `json:"local_path,omitempty"`
	Bucket    string `json:"s3_bucket,omitempty"`
	Region    string `json:"s3_region,omitempty"`
	AccessKey string `json:"s3_access_key,omitempty"`
	SecretKey string `json:"s3_secret_key,omitempty"`
	Prefix    string `json:"s3_path,omitempty"`
	FileSizeMB    int `json:"file_size_mb,omitempty"`
	MinFileSizeMB int `json:"min_file_size_mb,omitempty"`
	MaxRows       int `json:"max_rows,omitempty"`
}

func (c *Config) Validate() error {
	if c.FileSizeMB == 0 {
		c.FileSizeMB = 512
	}
	if c.MinFileSizeMB == 0 {
		c.MinFileSizeMB = 256
	}
	if c.MaxRows == 0 {
		c.MaxRows = 1000000
	}
	
	return utils.Validate(c)
}
