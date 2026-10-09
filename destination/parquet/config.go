package parquet

import (
	"fmt"

	"github.com/datazip-inc/olake/utils"
)

// defaultMaxFileSizeMB is the parquet roll threshold used when MaxFileSizeMB is unset.
const (
	defaultMaxFileSizeMB     = 512
	defaultRollCheckInterval = 500
	storageTypeS3            = "S3"
	storageTypeAzure         = "Azure"
)

type Config struct {
	StorageType string `json:"storage_type,omitempty"`
	Path        string `json:"local_path,omitempty"` // Local file path (for local file system usage)
	Bucket      string `json:"s3_bucket,omitempty"`
	Region      string `json:"s3_region,omitempty"`
	AccessKey   string `json:"s3_access_key,omitempty"`
	SecretKey   string `json:"s3_secret_key,omitempty"`
	Prefix      string `json:"s3_path,omitempty"`
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

func (c *Config) usingAzure() bool {
	return c.storageKind() == storageTypeAzure
}

func (c *Config) usingS3() bool {
	return c.storageKind() == storageTypeS3
}

func (c *Config) storageKind() string {
	switch c.StorageType {
	case storageTypeAzure, storageTypeS3:
		return c.StorageType
	case "":
		if c.AzureStorageAccountName != "" || c.AzureStorageAccountKey != "" || c.AzureContainerName != "" {
			return storageTypeAzure
		}
		if c.Bucket != "" || c.Region != "" {
			return storageTypeS3
		}
		return ""
	default:
		return c.StorageType
	}
}

func (c *Config) Validate() error {
	if err := utils.Validate(c); err != nil {
		return err
	}

	switch c.StorageType {
	case "", storageTypeS3, storageTypeAzure:
	default:
		return fmt.Errorf("storage_type must be %q or %q", storageTypeS3, storageTypeAzure)
	}

	kind := c.storageKind()
	switch kind {
	case storageTypeAzure:
		if (c.AzureStorageAccountName == "") != (c.AzureStorageAccountKey == "") {
			return fmt.Errorf("azure_storage_account_name and azure_storage_account_key must both be set together")
		}
		if c.AzureStorageAccountName == "" || c.AzureStorageAccountKey == "" || c.AzureContainerName == "" {
			return fmt.Errorf("azure_storage_account_name, azure_storage_account_key, and azure_container_name are required when storage_type is Azure")
		}
	case storageTypeS3:
		if c.Bucket == "" || c.Region == "" {
			return fmt.Errorf("s3_bucket and s3_region must both be set together")
		}
	}
	return nil
}
