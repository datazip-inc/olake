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
			name:    "valid s3 config",
			cfg:     &Config{Bucket: "my-bucket", Region: "us-east-1"},
			wantErr: "",
		},
		{
			name: "valid azure config",
			cfg:  &Config{AzureStorageAccountName: "myaccount", AzureStorageAccountKey: "mykey", AzureContainerName: "mycontainer"},
		},
		{
			name:    "missing azure account name",
			cfg:     &Config{AzureStorageAccountKey: "mykey", AzureContainerName: "mycontainer"},
			wantErr: "must both be set together",
		},
		{
			name:    "missing azure account key",
			cfg:     &Config{AzureStorageAccountName: "myaccount", AzureContainerName: "mycontainer"},
			wantErr: "must both be set together",
		},
		{
			name:    "missing azure container name",
			cfg:     &Config{AzureStorageAccountName: "myaccount", AzureStorageAccountKey: "mykey"},
			wantErr: "azure_container_name",
		},
		{
			name:    "azure and s3 together",
			cfg:     &Config{Bucket: "my-bucket", Region: "us-east-1", AzureStorageAccountName: "myaccount", AzureStorageAccountKey: "mykey", AzureContainerName: "mycontainer"},
			wantErr: "only one of azure or s3",
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
