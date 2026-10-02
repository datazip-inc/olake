package parquet

import (
	"encoding/base64"
	"errors"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/stretchr/testify/require"
)

func testAzureConfig() *Config {
	return &Config{
		AzureStorageAccountName: "olakeaccount",
		AzureStorageAccountKey:  base64.StdEncoding.EncodeToString([]byte("olake-azure-test-key")),
		AzureContainerName:      "container",
	}
}

func TestAzureObjectKey(t *testing.T) {
	tests := []struct {
		name     string
		prefix   string
		relative string
		want     string
	}{
		{
			name:     "no prefix",
			relative: "namespace/table/data.parquet",
			want:     "namespace/table/data.parquet",
		},
		{
			name:     "joins azure path",
			prefix:   "root",
			relative: "namespace/table/data.parquet",
			want:     "root/namespace/table/data.parquet",
		},
		{
			name:     "nested azure path",
			prefix:   "allowed/path",
			relative: "namespace/table/_olake_2pc/finish.json",
			want:     "allowed/path/namespace/table/_olake_2pc/finish.json",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &azureStore{prefix: tt.prefix}
			require.Equal(t, tt.want, store.ObjectKey(tt.relative))
		})
	}
}

func TestAzureIsNotFound(t *testing.T) {
	store := &azureStore{}
	require.False(t, store.IsNotFound(nil))
	require.False(t, store.IsNotFound(errors.New("other")))
	require.True(t, store.IsNotFound(&azcore.ResponseError{
		ErrorCode:  string(bloberror.BlobNotFound),
		StatusCode: 404,
	}))
	require.False(t, store.IsNotFound(&azcore.ResponseError{
		ErrorCode:  string(bloberror.ContainerNotFound),
		StatusCode: 404,
	}))
}

func TestNewAzureStore(t *testing.T) {
	t.Run("trims azure path", func(t *testing.T) {
		cfg := testAzureConfig()
		cfg.AzurePath = "/root/"
		store, err := newAzureStore(cfg)
		require.NoError(t, err)
		require.Equal(t, "azure", store.Kind())
		require.Equal(t, "container", store.container)
		require.Equal(t, "root", store.prefix)
		require.Equal(t, "root", cfg.AzurePath)
		require.Equal(t, "root/namespace/table", store.ObjectKey("namespace/table"))
	})

	t.Run("rejects invalid account key", func(t *testing.T) {
		cfg := testAzureConfig()
		cfg.AzureStorageAccountKey = "not-base64"
		_, err := newAzureStore(cfg)
		require.Error(t, err)
		require.ErrorContains(t, err, "failed to create Azure SharedKeyCredential")
	})
}
