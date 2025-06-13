package parquet

import (
	"github.com/datazip-inc/olake/utils"
)

type Config struct {
	Path string `json:"local_path,omitempty"` // Local file path (for local file system usage)

	// Bucket
	//
	// @jsonSchema(
	//   title="S3 Bucket",
	//   description="The name of an existing Amazon S3 bucket with appropriate access permissions to store output files",
	//   type="string",
	//   order=1,
	//   required=true
	// )
	Bucket string `json:"s3_bucket,omitempty"`

	// Region
	//
	// @jsonSchema(
	//   title="S3 Region",
	//   description="Specify the AWS region where the S3 bucket is hosted",
	//   type="string",
	//   order=2,
	//   required=true
	// )
	Region string `json:"s3_region,omitempty"`

	// AccessKey
	//
	// @jsonSchema(
	//   title="S3 Access Key",
	//   description="The AWS access key for authenticating S3 requests, typically a 20 character alphanumeric string",
	//   type="string",
	//   order=3,
	//   required=true
	// )
	AccessKey string `json:"s3_access_key,omitempty"`

	// SecretKey
	//
	// @jsonSchema(
	//   title="AWS Secret Key",
	//   description="The AWS secret key for S3 authentication—typically 40+ characters long",
	//   type="string",
	//   format="password",
	//   order=4,
	//   required=true
	// )
	SecretKey string `json:"s3_secret_key,omitempty"`

	// Prefix
	//
	// @jsonSchema(
	//   title="S3 Prefix",
	//   description="Specify the S3 bucket path (prefix) where data files will be written, typically starting with a '/' (e.g., '/data')",
	//   type="string",
	//   default="",
	//   order=5
	// )
	Prefix string `json:"s3_path,omitempty"`
}

func (c *Config) Validate() error {
	return utils.Validate(c)
}
