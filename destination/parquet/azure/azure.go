package azure

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/datazip-inc/olake/utils"
)

const (
	// Olake waits for 10 minutes for a copy operation to complete
	azureCopyTimeout = 10 * time.Minute
	azureCopyPoll    = 200 * time.Millisecond
)

const (
	AuthSharedKey        = "shared-key"
	AuthSASToken         = "sas-token"
	AuthConnectionString = "connection-string"
	AuthManagedIdentity  = "managed-identity"
)

type Config struct {
	AuthType                 string
	AccountName              string
	AccessKey                string
	Container                string
	Prefix                   string // optional prefix for the blob path
	ServiceURL               string
	SharedAccessKeySignature string // user client SAS with own expiration time
	ConnectionString         string
}

// Store is the implementation of the ObjectStore interface for Azure Blob Storage.
type Store struct {
	client     *azblob.Client
	container  string
	prefix     string
	serviceURL string
}

func New(cfg Config) (*Store, error) {
	serviceURL := blobServiceURL(cfg)
	client, err := newServiceClient(cfg, serviceURL)
	if err != nil {
		return nil, err
	}

	return &Store{
		client:     client,
		container:  cfg.Container,
		prefix:     strings.Trim(cfg.Prefix, "/"),
		serviceURL: serviceURL,
	}, nil
}

func blobServiceURL(cfg Config) string {
	if cfg.ServiceURL != "" {
		return cfg.ServiceURL
	}
	if cfg.AccountName == "" {
		return ""
	}
	return fmt.Sprintf("https://%s.blob.core.windows.net/", cfg.AccountName)
}

func newServiceClient(cfg Config, serviceURL string) (*azblob.Client, error) {
	switch cfg.AuthType {
	case AuthConnectionString:
		return getServiceClientConnectionString(cfg.ConnectionString)
	case AuthSASToken:
		return getServiceClientSAS(serviceURL, cfg.SharedAccessKeySignature)
	case AuthSharedKey:
		return getServiceClientSharedKey(cfg.AccountName, cfg.AccessKey, serviceURL)
	case AuthManagedIdentity:
		// Entra, VM (IMDS) and AKS workload identity
		return getServiceClientTokenCredential(serviceURL)
	default:
		return nil, fmt.Errorf("unsupported azure auth type %q", cfg.AuthType)
	}
}

func (a *Store) Kind() string {
	return "azure"
}

func (a *Store) ObjectKey(relativePath string) string {
	return path.Join(a.prefix, relativePath)
}

func (a *Store) blobClient(key string) *blob.Client {
	return a.client.ServiceClient().NewContainerClient(a.container).NewBlobClient(key)
}

// Upload binary data to a block blob
func (a *Store) Put(ctx context.Context, key string, data []byte) error {
	_, err := a.client.UploadBuffer(ctx, a.container, key, data, nil)
	return err
}

// Upload a local file as a block blob
func (a *Store) UploadFile(ctx context.Context, key string, file *os.File) error {
	_, err := a.client.UploadFile(ctx, a.container, key, file, nil)
	return err
}

// Download blob contents
// BlockSize: The size of each block when downloading a block blob. The default value is 4 MB.
// Concurrency: The maximum number of parallel connections to use during download. The default value is 5.
func (a *Store) Get(ctx context.Context, key string) ([]byte, error) {
	out, err := a.client.DownloadStream(ctx, a.container, key, nil)
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

// Use a flat listing
// By default, a listing operation returns blobs in a flat listing. In a flat listing, blobs aren't organized by virtual directory.
func (a *Store) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	pager := a.client.NewListBlobsFlatPager(a.container, &container.ListBlobsFlatOptions{
		Prefix: &prefix,
	})
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, item := range page.Segment.BlobItems {
			if item.Name != nil {
				keys = append(keys, *item.Name)
			}
		}
	}
	return keys, nil
}

func (a *Store) Copy(ctx context.Context, srcKey, dstKey string) error {
	// 2PC: same account, same container (staging key - dest key).

	// Azurite does not support StartCopyFromURL, so we need to download and upload the blob instead.
	if strings.HasPrefix(a.serviceURL, "http://") {
		out, err := a.client.DownloadStream(ctx, a.container, srcKey, nil)
		if err != nil {
			return fmt.Errorf("failed to read azure copy source %s: %w", srcKey, err)
		}
		defer out.Body.Close()
		if _, err := a.client.UploadStream(ctx, a.container, dstKey, out.Body, nil); err != nil {
			return fmt.Errorf("failed to write azure copy dest %s: %w", dstKey, err)
		}
		return nil
	}

	// Copy a blob from a URL to another blob
	// Large blobs need asynchronous copy. StartCopyFromURL returns immediately, but the copy operation may take time to complete
	dst := a.blobClient(dstKey)
	srcURL := a.blobClient(srcKey).URL()

	if _, err := dst.StartCopyFromURL(ctx, srcURL, nil); err != nil {
		if !bloberror.HasCode(err, bloberror.PendingCopyOperation) {
			return fmt.Errorf("failed to start azure copy %s -> %s: %w", srcKey, dstKey, err)
		}
	}
	return a.waitForCopy(ctx, dst, srcKey, dstKey)
}

func (a *Store) waitForCopy(ctx context.Context, dst *blob.Client, srcKey, dstKey string) error {
	// client timeout for the copy operation
	ctx, cancel := context.WithTimeout(ctx, azureCopyTimeout)
	defer cancel()
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("azure copy %s, %s: %w", srcKey, dstKey, err)
		}
		props, err := dst.GetProperties(ctx, nil)
		if err != nil {
			return fmt.Errorf("failed to poll azure copy %s -> %s: %w", srcKey, dstKey, err)
		}
		if props.CopyStatus == nil {
			return fmt.Errorf("azure copy %s -> %s: missing copy status", srcKey, dstKey)
		}
		switch *props.CopyStatus {
		case blob.CopyStatusTypeSuccess:
			return nil
		case blob.CopyStatusTypePending:
			select {
			case <-ctx.Done():
				return fmt.Errorf("azure copy %s -> %s: %w", srcKey, dstKey, ctx.Err())
			case <-time.After(azureCopyPoll):
			}
		default:
			return fmt.Errorf("azure copy %s -> %s: %s", srcKey, dstKey, *props.CopyStatus)
		}
	}
}

func (a *Store) Delete(ctx context.Context, key string) error {
	_, err := a.client.DeleteBlob(ctx, a.container, key, nil)
	// ignore delete when the blob or container is already gone
	if a.IsNotFound(err) || bloberror.HasCode(err, bloberror.ContainerNotFound) {
		return nil
	}
	return err
}

func (a *Store) DeletePrefix(ctx context.Context, prefix string) error {
	return utils.RetryWithSkip(ctx, 3, time.Minute, a.IsRateLimitError, func(ctx context.Context) error {
		keys, err := a.List(ctx, prefix)
		if err != nil {
			return err
		}
		for _, key := range keys {
			if err := a.Delete(ctx, key); err != nil {
				return err
			}
		}
		return nil
	})
}

// IsNotFound checks missing blob errr
func (a *Store) IsNotFound(err error) bool {
	return bloberror.HasCode(err, bloberror.BlobNotFound)
}

// IsRateLimitError reports Azure HTTP 429/503 throttle responses
func (a *Store) IsRateLimitError(err error) bool {
	var respErr *azcore.ResponseError
	return errors.As(err, &respErr) && (respErr.StatusCode == 429 || respErr.StatusCode == 503)
}

// DefaultAzureCredential: AKS federated token, then VM/App Service MI.
// Token refresh is the SDK. Needs a valid serviceURL.
func getServiceClientTokenCredential(accountURL string) (*azblob.Client, error) {
	// Create a new service client with token credential
	credential, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create default Azure credential: %w", err)
	}

	client, err := azblob.NewClient(accountURL, credential, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create Azure Blob Storage client: %w", err)
	}

	return client, nil
}

// SAS token credential
// User-supplied SAS on the *client*. Expriy is set when the token is created.
func getServiceClientSAS(accountURL string, sasToken string) (*azblob.Client, error) {
	// Create a new service client with an existing SAS token
	// Append the SAS to the account URL with a "?" delimiter
	accountURLWithSAS := fmt.Sprintf("%s?%s",
		strings.TrimRight(accountURL, "?"),
		strings.TrimPrefix(sasToken, "?"))
	client, err := azblob.NewClientWithNoCredential(accountURLWithSAS, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create Azure Blob Storage client: %w", err)
	}
	return client, nil
}

// Shared key credential - use storage account shared key to build HMAC signature for each request
func getServiceClientSharedKey(accountName string, accountKey string, accountURL string) (*azblob.Client, error) {
	if accountURL == "" {
		return nil, fmt.Errorf("azure service URL is required with shared key")
	}
	// Create a new service client with shared key credential
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create Azure Shared Key Credential: %w", err)
	}
	client, err := azblob.NewClientWithSharedKeyCredential(accountURL, credential, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create Azure Blob Storage client: %w", err)
	}
	return client, nil
}

// Connection string credential - use storage account connection string
func getServiceClientConnectionString(connectionString string) (*azblob.Client, error) {
	// Create a new service client with connection string
	client, err := azblob.NewClientFromConnectionString(connectionString, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create Azure Blob Storage client: %w", err)
	}
	return client, nil
}
