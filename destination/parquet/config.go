package parquet

import (
	"fmt"

	"github.com/datazip-inc/olake/utils"
)

// defaultMaxFileSizeMB is the parquet roll threshold used when MaxFileSizeMB is unset.
const (
	defaultMaxFileSizeMB     = 512
	defaultRollCheckInterval = 500
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

	AzureStorageAccountName string `json:"azure_storage_account_name,omitempty"`
	AzureStorageAccountKey  string `json:"azure_storage_account_key,omitempty"`
	AzureContainerName      string `json:"azure_container_name,omitempty"`
	AzurePath               string `json:"azure_path,omitempty"`
	AzureEndpoint           string `json:"azure_endpoint,omitempty"`

	MaxFileSizeMB float64 `json:"max_file_size_mb,omitempty" validate:"gte=0"`
}

func (c *Config) Validate() error {
	if err := utils.Validate(c); err != nil {
		return err
	}

	// Azure name and key must both be set
	if (c.AzureStorageAccountName == "") != (c.AzureStorageAccountKey == "") {
		return fmt.Errorf("azure_storage_account_name and azure_storage_account_key must both be set together")
	}

	azureConfigured := c.AzureStorageAccountName != ""
	s3Configured := c.Bucket != "" && c.Region != ""

	if azureConfigured && s3Configured {
		return fmt.Errorf("only one of azure or s3 can be configured")
	}

	if azureConfigured && c.AzureContainerName == "" {
		return fmt.Errorf("azure_container_name is required when using azure blob")
	}

	return nil
}

func (c *Config) usingAzure() bool {
	return c.AzureStorageAccountName != "" && c.AzureStorageAccountKey != "" && c.AzureContainerName != ""
}
