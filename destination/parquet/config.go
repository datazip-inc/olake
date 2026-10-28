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
	AzureClientID           string `json:"azure_client_id,omitempty"`

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
		return c.validateConnectionString()
	case azure.AuthManagedIdentity:
		if c.AzureStorageAccountName == "" {
			return fmt.Errorf("azure_storage_account_name is required")
		}
		// Token client in azure.New. VM IMDS / AKS workload identity.
		// Optional azure_client_id (or AZURE_CLIENT_ID) for user-assigned MI.
	default:
		return fmt.Errorf("azure_auth_type must be %q, %q, %q, or %q",
			azure.AuthSharedKey, azure.AuthSASToken, azure.AuthConnectionString, azure.AuthManagedIdentity)
	}
	return nil
}

// validateConnectionString checks the pairs the Azure client reads from the string.
// azure_storage_account_name and azure_endpoint are ignored
func (c *Config) validateConnectionString() error {
	fields, err := connectionStringFields(c.AzureConnectionString)
	if err != nil {
		return err
	}
	// The SDK does not expand UseDevelopmentStorage=true. Azurite needs the three explicit fields.
	if strings.EqualFold(fields["UseDevelopmentStorage"], "true") &&
		(fields["AccountName"] == "" || fields["AccountKey"] == "" || fields["BlobEndpoint"] == "") {
		return fmt.Errorf("UseDevelopmentStorage=true is not supported; set AccountName, AccountKey, and BlobEndpoint")
	}
	// Same requirements as azblob ParseConnectionString. BlobEndpoint is optional on Azure.
	if fields["AccountName"] == "" && fields["BlobEndpoint"] == "" {
		return fmt.Errorf("azure_connection_string needs AccountName or BlobEndpoint")
	}
	if fields["AccountKey"] == "" && fields["SharedAccessSignature"] == "" {
		return fmt.Errorf("azure_connection_string needs AccountKey or SharedAccessSignature")
	}
	return nil
}

func connectionStringFields(connectionString string) (map[string]string, error) {
	raw := strings.TrimRight(strings.TrimSpace(connectionString), ";")
	if raw == "" {
		return nil, fmt.Errorf("azure_connection_string is required for %s", azure.AuthConnectionString)
	}
	fields := make(map[string]string)
	for _, part := range strings.Split(raw, ";") {
		key, value, ok := strings.Cut(part, "=")
		if !ok || strings.TrimSpace(key) == "" {
			return nil, fmt.Errorf("azure_connection_string is malformed")
		}
		fields[strings.TrimSpace(key)] = strings.TrimSpace(value)
	}
	return fields, nil
}
