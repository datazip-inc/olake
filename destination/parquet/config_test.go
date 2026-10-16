package parquet

import (
	"testing"

	"github.com/datazip-inc/olake/destination/parquet/azure"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigValidateMaxFileSizeMB(t *testing.T) {
	tests := []struct {
		name    string
		size    float64
		wantErr bool
	}{
		{
			name:    "zero is allowed (treated as unset, falls back to default)",
			size:    0,
			wantErr: false,
		},
		{
			name:    "positive value is allowed",
			size:    512,
			wantErr: false,
		},
		{
			name:    "fractional positive value is allowed",
			size:    0.125,
			wantErr: false,
		},
		{
			name:    "negative value is rejected",
			size:    -1,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Config{MaxFileSizeMB: tt.size}
			err := c.Validate()

			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "max_file_size_mb")
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConfigValidateStore(t *testing.T) {
	azureCfg := Config{
		StorageType:             storageTypeAzure,
		AzureStorageAccountName: "myaccount",
		AzureContainerName:      "mycontainer",
	}

	tests := []struct {
		name    string
		cfg     *Config
		wantErr string
	}{
		{
			name: "omitted storage_type is valid",
			cfg: &Config{
				Bucket: "my-bucket",
				Region: "us-east-1",
			},
		},
		{
			name: "storage_type s3 is valid",
			cfg: &Config{
				StorageType: storageTypeS3,
				Bucket:      "my-bucket",
				Region:      "us-east-1",
			},
		},
		{
			name: "azure fields without storage_type skip azure validate",
			cfg: &Config{
				AzureStorageAccountName: "myaccount",
				AzureContainerName:      "mycontainer",
			},
		},
		{
			name: "invalid storage_type",
			cfg: &Config{
				StorageType: "GCS",
			},
			wantErr: "storage_type must be",
		},
		{
			name: "azure shared key",
			cfg: func() *Config {
				c := azureCfg
				c.AzureAuthType = azure.AuthSharedKey
				c.AzureStorageAccountKey = "mykey"
				return &c
			}(),
		},
		{
			name: "azure sas token",
			cfg: func() *Config {
				c := azureCfg
				c.AzureAuthType = azure.AuthSASToken
				c.AzureSASToken = "sv=2023-11-03&sig=fake"
				return &c
			}(),
		},
		{
			name: "azure connection string",
			cfg: func() *Config {
				c := azureCfg
				c.AzureAuthType = azure.AuthConnectionString
				c.AzureConnectionString = "AccountName=myaccount;AccountKey=key"
				return &c
			}(),
		},
		{
			name: "azure token auth does not require dest JSON ids",
			cfg: func() *Config {
				c := azureCfg
				c.AzureAuthType = azure.AuthServicePrincipal
				return &c
			}(),
		},
		{
			name: "azure requires account name and container",
			cfg: &Config{
				StorageType:   storageTypeAzure,
				AzureAuthType: azure.AuthSharedKey,
			},
			wantErr: "azure_storage_account_name and azure_container_name are required",
		},
		{
			name: "azure shared key requires account key",
			cfg: func() *Config {
				c := azureCfg
				c.AzureAuthType = azure.AuthSharedKey
				return &c
			}(),
			wantErr: "azure_storage_account_key is required for shared-key",
		},
		{
			name: "azure sas requires token",
			cfg: func() *Config {
				c := azureCfg
				c.AzureAuthType = azure.AuthSASToken
				return &c
			}(),
			wantErr: "azure_sas_token is required for sas-token",
		},
		{
			name: "azure connection string requires connection string",
			cfg: func() *Config {
				c := azureCfg
				c.AzureAuthType = azure.AuthConnectionString
				return &c
			}(),
			wantErr: "azure_connection_string is required for connection-string",
		},
		{
			name: "azure requires auth type",
			cfg: func() *Config {
				c := azureCfg
				return &c
			}(),
			wantErr: "azure_auth_type must be",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr != "" {
				assert.ErrorContains(t, err, tt.wantErr)
				return
			}
			assert.NoError(t, err)
		})
	}
}

func TestConfigUsingObjectStore(t *testing.T) {
	tests := []struct {
		name      string
		cfg       *Config
		wantAzure bool
		wantS3    bool
		wantKind  string
	}{
		{
			name: "bucket and region use s3",
			cfg: &Config{
				Bucket: "my-bucket",
				Region: "us-east-1",
			},
			wantS3:   true,
			wantKind: storageTypeS3,
		},
		{
			name: "bucket without region does not use s3",
			cfg: &Config{
				Bucket: "my-bucket",
			},
			wantKind: storageTypeS3,
		},
		{
			name: "azure fields without storage_type do not use azure",
			cfg: &Config{
				AzureStorageAccountName: "myaccount",
				AzureContainerName:      "mycontainer",
			},
			wantKind: storageTypeS3,
		},
		{
			name: "storage_type azure uses azure even with s3 fields",
			cfg: &Config{
				StorageType:             storageTypeAzure,
				AzureStorageAccountName: "myaccount",
				AzureContainerName:      "mycontainer",
				Bucket:                  "my-bucket",
				Region:                  "us-east-1",
			},
			wantAzure: true,
			wantKind:  storageTypeAzure,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.wantKind, tt.cfg.storageKind())
			require.Equal(t, tt.wantAzure, tt.cfg.usingAzure())
			require.Equal(t, tt.wantS3, tt.cfg.usingS3())
		})
	}
}
