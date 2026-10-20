package parquet

import (
	"fmt"
	"strings"

	"github.com/datazip-inc/olake/destination/parquet/azure"
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
	AzureAuthType           string `json:"azure_auth_type,omitempty"`
	AzureSASToken           string `json:"azure_sas_token,omitempty"`
	AzureConnectionString   string `json:"azure_connection_string,omitempty"`

	MaxFileSizeMB float64 `json:"max_file_size_mb,omitempty" validate:"gte=0"`
}

func (c *Config) usingAzure() bool {
	return c.storageKind() == storageTypeAzure
}

func (c *Config) usingS3() bool {
	return c.storageKind() == storageTypeS3 && c.Bucket != "" && c.Region != ""
}

// storageKind is Azure only when storage_type is Azure. Anything else is S3
func (c *Config) storageKind() string {
	switch strings.ToLower(strings.TrimSpace(c.StorageType)) {
	case "azure":
		return storageTypeAzure
	default:
		return storageTypeS3
	}
}

func (c *Config) Validate() error {
	if err := utils.Validate(c); err != nil {
		return err
	}

	switch strings.ToLower(strings.TrimSpace(c.StorageType)) {
	case "azure":
		return c.validateAzureAuthenticationType()
	case "", "s3":
		return nil
	default:
		return fmt.Errorf("storage_type must be %q or %q", storageTypeS3, storageTypeAzure)
	}
}

func (c *Config) validateAzureAuthenticationType() error {
	if c.AzureContainerName == "" {
		return fmt.Errorf("azure_container_name is required")
	}

	switch c.AzureAuthType {
	case azure.AuthSharedKey:
		if c.AzureStorageAccountKey == "" || c.AzureStorageAccountName == "" {
			return fmt.Errorf("azure_storage_account_key and azure_storage_account_name are required for %s", azure.AuthSharedKey)
		}
	case azure.AuthSASToken:
		if c.AzureSASToken == "" || c.AzureStorageAccountName == "" {
			return fmt.Errorf("azure_sas_token and azure_storage_account_name are required for %s", azure.AuthSASToken)
		}
	case azure.AuthConnectionString:
		if c.AzureConnectionString == "" {
			return fmt.Errorf("azure_connection_string is required for %s", azure.AuthConnectionString)
		}
	case azure.AuthManagedIdentity:
		if c.AzureStorageAccountName == "" {
			return fmt.Errorf("azure_storage_account_name is required")
		}
		// Token client in azure.New. VM IMDS / AKS workload identity.
		// Optional AZURE_CLIENT_ID for user-assigned MI.
	default:
		return fmt.Errorf("azure_auth_type must be %q, %q, %q, or %q",
			azure.AuthSharedKey, azure.AuthSASToken, azure.AuthConnectionString, azure.AuthManagedIdentity)
	}
	return nil
}
