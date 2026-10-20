package azure

import (
	"encoding/base64"
	"errors"
	"fmt"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/stretchr/testify/require"
)

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
			store := &Store{prefix: tt.prefix}
			require.Equal(t, tt.want, store.ObjectKey(tt.relative))
		})
	}
}

func TestAzureIsNotFound(t *testing.T) {
	store := &Store{}
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
	key := base64.StdEncoding.EncodeToString([]byte("olake-azure-test-key"))
	conn := fmt.Sprintf(
		"DefaultEndpointsProtocol=https;AccountName=olakeaccount;AccountKey=%s;EndpointSuffix=core.windows.net",
		key,
	)

	tests := []struct {
		name          string
		cfg           Config
		wantErr       string
		wantPrefix    string
		wantURL       string
		wantContainer string
	}{
		{
			name: "shared key trims prefix and defaults service URL",
			cfg: Config{
				AuthType:    AuthSharedKey,
				AccountName: "olakeaccount",
				AccountKey:  key,
				Container:   "container",
				Prefix:      "/root/",
			},
			wantPrefix:    "root",
			wantURL:       "https://olakeaccount.blob.core.windows.net/",
			wantContainer: "container",
		},
		{
			name: "shared key uses configured service URL",
			cfg: Config{
				AuthType:    AuthSharedKey,
				AccountName: "olakeaccount",
				AccountKey:  key,
				Container:   "container",
				Endpoint:    "http://127.0.0.1:10000/devstoreaccount1",
			},
			wantURL: "http://127.0.0.1:10000/devstoreaccount1",
		},
		{
			name: "shared key requires service URL",
			cfg: Config{
				AuthType:   AuthSharedKey,
				AccountKey: key,
				Container:  "container",
			},
			wantErr: "azure service URL is required with shared key",
		},
		{
			name: "shared key rejects invalid account key",
			cfg: Config{
				AuthType:    AuthSharedKey,
				AccountName: "olakeaccount",
				AccountKey:  "not-base64",
				Container:   "container",
			},
			wantErr: "failed to create Azure Shared Key Credential",
		},
		{
			name:    "rejects empty auth type",
			cfg:     Config{AuthType: "", AccountName: "olakeaccount", AccountKey: key},
			wantErr: `unsupported azure auth type ""`,
		},
		{
			name: "sas token",
			cfg: Config{
				AuthType:    AuthSASToken,
				AccountName: "olakeaccount",
				Container:   "container",
				SASToken:    "?sv=2023-11-03&sig=fake",
			},
			wantContainer: "container",
		},
		{
			name: "connection string",
			cfg: Config{
				AuthType:         AuthConnectionString,
				Container:        "container",
				ConnectionString: conn,
			},
			wantContainer: "container",
			wantURL:       "https://olakeaccount.blob.core.windows.net/",
		},
		{
			name: "connection string uses BlobEndpoint for Azurite copy check",
			cfg: Config{
				AuthType:  AuthConnectionString,
				Container: "container",
				ConnectionString: fmt.Sprintf(
					"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=%s;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;",
					key,
				),
			},
			wantContainer: "container",
			wantURL:       "http://127.0.0.1:10000/devstoreaccount1/",
		},
		{
			name: "rejects invalid connection string",
			cfg: Config{
				AuthType:         AuthConnectionString,
				ConnectionString: "not-a-connection-string",
			},
			wantErr: "failed to create Azure Blob Storage client",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, err := New(tt.cfg)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				require.Nil(t, store)
				return
			}
			require.NoError(t, err)
			require.Equal(t, "azure", store.Kind())
			if tt.wantPrefix != "" {
				require.Equal(t, tt.wantPrefix, store.prefix)
			}
			if tt.wantURL != "" {
				require.Equal(t, tt.wantURL, store.serviceURL)
			}
			if tt.wantContainer != "" {
				require.Equal(t, tt.wantContainer, store.container)
			}
		})
	}
}
