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
}

// ParquetSchema represents the schema for the parquet writer
//
// @jsonSchema(
//
//	title="Writer Settings"
//
// )
type ParquetSchema struct {
	// Type
	//
	// @jsonSchema(
	//   title="File Type",
	//   description="Type of file to write (e.g., PARQUET)",
	//   type="string",
	//   enum=["PARQUET"],
	//   default="PARQUET"
	// )
	Type string `json:"type"`

	Writer Config `json:"writer"`
}

func (c *Config) Validate() error {
	return utils.Validate(c)
}
