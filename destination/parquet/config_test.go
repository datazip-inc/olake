package parquet

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
	tests := []struct {
		name    string
		cfg     *Config
		wantErr string
	}{
		{
			name: "valid s3 config (inferred)",
			cfg: &Config{
				Bucket: "my-bucket",
				Region: "us-east-1",
			},
		},
		{
			name: "valid azure config (inferred)",
			cfg: &Config{
				AzureStorageAccountName: "myaccount",
				AzureStorageAccountKey:  "mykey",
				AzureContainerName:      "mycontainer",
			},
		},
		{
			name: "valid s3 with storage_type",
			cfg: &Config{
				StorageType: storageTypeS3,
				Bucket:      "my-bucket",
				Region:      "us-east-1",
			},
		},
		{
			name: "valid azure with storage_type",
			cfg: &Config{
				StorageType:             storageTypeAzure,
				AzureStorageAccountName: "myaccount",
				AzureStorageAccountKey:  "mykey",
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
			name: "missing azure account name",
			cfg: &Config{
				AzureStorageAccountKey: "mykey",
				AzureContainerName:     "mycontainer",
			},
			wantErr: "must both be set together",
		},
		{
			name: "missing azure account key",
			cfg: &Config{
				AzureStorageAccountName: "myaccount",
				AzureContainerName:      "mycontainer",
			},
			wantErr: "must both be set together",
		},
		{
			name: "missing azure container name",
			cfg: &Config{
				AzureStorageAccountName: "myaccount",
				AzureStorageAccountKey:  "mykey",
			},
			wantErr: "azure_container_name",
		},
		{
			name: "s3 bucket without region",
			cfg: &Config{
				Bucket: "my-bucket",
			},
			wantErr: "s3_bucket and s3_region must both be set together",
		},
		{
			name: "s3 region without bucket",
			cfg: &Config{
				Region: "us-east-1",
			},
			wantErr: "s3_bucket and s3_region must both be set together",
		},
		{
			name: "azure and s3 together infers azure",
			cfg: &Config{
				Bucket:                  "my-bucket",
				Region:                  "us-east-1",
				AzureStorageAccountName: "myaccount",
				AzureStorageAccountKey:  "mykey",
				AzureContainerName:      "mycontainer",
			},
		},
		{
			name: "storage_type s3 ignores azure fields",
			cfg: &Config{
				StorageType:             storageTypeS3,
				Bucket:                  "my-bucket",
				Region:                  "us-east-1",
				AzureStorageAccountName: "myaccount",
				AzureStorageAccountKey:  "mykey",
				AzureContainerName:      "mycontainer",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr != "" {
				assert.ErrorContains(t, err, tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
