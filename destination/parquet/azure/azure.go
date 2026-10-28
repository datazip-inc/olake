package azure

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
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
	// Deadline for one HTTP attempt. azcore's default is 0, which waits until the caller cancels.
	// Files up to 256 MB are a single PUT, so this covers that body.
	azureTryTimeout = 5 * time.Minute
	// ADLS Gen2 flat listings mark a directory with this metadata entry.
	azureFolderMetadataKey = "hdi_isfolder"
)

const (
	AuthSharedKey        = "shared-key"
	AuthSASToken         = "sas-token"
	AuthConnectionString = "connection-string"
	AuthManagedIdentity  = "managed-identity"
)

type Config struct {
	AuthType         string
	AccountName      string
	AccountKey       string
	Container        string
	Prefix           string // optional prefix for the blob path
	Endpoint         string
	SASToken         string // user client SAS with own expiration time
	ConnectionString string
	ClientID         string // user-assigned MI; empty uses AZURE_CLIENT_ID / system-assigned
}

// Store is the implementation of the ObjectStore interface for Azure Blob Storage.
type Store struct {
	client     *azblob.Client
	container  string
	prefix     string
	serviceURL string
	Kind       string
}

func New(cfg Config) (*Store, error) {
	client, err := newServiceClient(cfg, blobServiceURL(cfg))
	if err != nil {
		return nil, err
	}

	return &Store{
		client:     client,
		container:  cfg.Container,
		prefix:     strings.Trim(cfg.Prefix, "/"),
		serviceURL: client.ServiceClient().URL(), // serviceURL from the client
		Kind:       "azure",
	}, nil
}

func blobServiceURL(cfg Config) string {
	if cfg.Endpoint != "" {
		return cfg.Endpoint
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
		return getServiceClientSAS(serviceURL, cfg.SASToken)
	case AuthSharedKey:
		return getServiceClientSharedKey(cfg.AccountName, cfg.AccountKey, serviceURL)
	case AuthManagedIdentity:
		// DefaultAzureCredential: service principal, workload identity, then managed identity.
		return getServiceClientTokenCredential(serviceURL, cfg.ClientID)
	default:
		return nil, fmt.Errorf("unsupported azure auth type %q", cfg.AuthType)
	}
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
// ADLS Gen2 also returns directory entries. Those are omitted here so callers do not copy them as files.
func (a *Store) List(ctx context.Context, prefix string) ([]string, error) {
	files, _, err := a.listBlobNames(ctx, prefix)
	return files, err
}

// listBlobNames splits a flat listing into file blobs and ADLS Gen2 directory markers.
func (a *Store) listBlobNames(ctx context.Context, prefix string) (files, folders []string, err error) {
	pager := a.client.NewListBlobsFlatPager(a.container, &container.ListBlobsFlatOptions{
		Prefix:  &prefix,
		Include: container.ListBlobsInclude{Metadata: true},
	})
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, nil, err
		}
		for _, item := range page.Segment.BlobItems {
			if item.Name == nil {
				continue
			}
			if isADLSFolder(item.Metadata) {
				folders = append(folders, *item.Name)
				continue
			}
			files = append(files, *item.Name)
		}
	}
	return files, folders, nil
}

// isADLSFolder reports a hierarchical-namespace directory. The metadata value is set true
func isADLSFolder(metadata map[string]*string) bool {
	for key, value := range metadata {
		if strings.EqualFold(key, azureFolderMetadataKey) && value != nil && strings.EqualFold(*value, "true") {
			return true
		}
	}
	return false
}

func (a *Store) Copy(ctx context.Context, srcKey, dstKey string) error {
	// 2PC: same account, same container (staging key - dest key).
	// Azurite supports this for a same-account copy with no SAS on the source URL.

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
		files, folders, err := a.listBlobNames(ctx, prefix)
		if err != nil {
			return err
		}
		// A non-empty ADLS directory returns 409 DirectoryIsNotEmpty, so files go first.
		// Files have no order, so several deletes run at once. Folders stay one at a time.
		if len(files) > 0 {
			err := utils.Concurrent(ctx, files, min(len(files), 8), func(ctx context.Context, key string, _ int) error {
				return a.Delete(ctx, key)
			})
			if err != nil {
				return err
			}
		}
		sortAzureFolders(folders)
		for _, key := range folders {
			if err := a.Delete(ctx, key); err != nil {
				return err
			}
		}
		return nil
	})
}

// sortAzureFolders orders directory markers from the deepest path to the shallowest.
func sortAzureFolders(folders []string) {
	sort.Slice(folders, func(i, j int) bool {
		di, dj := azureFolderDepth(folders[i]), azureFolderDepth(folders[j])
		if di != dj {
			return di > dj
		}
		return folders[i] > folders[j]
	})
}

func azureFolderDepth(key string) int {
	key = strings.Trim(key, "/")
	if key == "" {
		return 0
	}
	return strings.Count(key, "/") + 1
}

// IsNotFound reports a missing blob.
func (a *Store) IsNotFound(err error) bool {
	return bloberror.HasCode(err, bloberror.BlobNotFound)
}

// IsRateLimitError reports Azure HTTP 429/503 throttle responses
func (a *Store) IsRateLimitError(err error) bool {
	var respErr *azcore.ResponseError
	return errors.As(err, &respErr) && (respErr.StatusCode == 429 || respErr.StatusCode == 503)
}

// azureTokenCredential: empty clientID uses DefaultAzureCredential (AKS WI, then system-assigned MI).
// azure_client_id selects a user-assigned identity via ManagedIdentityCredential.
func azureTokenCredential(clientID string) (azcore.TokenCredential, error) {
	if clientID == "" {
		credential, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create default Azure credential: %w", err)
		}
		return credential, nil
	}
	credential, err := azidentity.NewManagedIdentityCredential(&azidentity.ManagedIdentityCredentialOptions{
		ID: azidentity.ClientID(clientID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create Azure managed identity credential: %w", err)
	}
	return credential, nil
}

// Token refresh is the SDK. Needs a valid serviceURL.
func getServiceClientTokenCredential(accountURL, clientID string) (*azblob.Client, error) {
	credential, err := azureTokenCredential(clientID)
	if err != nil {
		return nil, err
	}

	client, err := azblob.NewClient(accountURL, credential, blobClientOptions())
	if err != nil {
		return nil, fmt.Errorf("failed to create Azure Blob Storage client: %w", err)
	}

	return client, nil
}

// SAS token credential
// User-supplied SAS on the *client*. Expiry is set when the token is created.
func getServiceClientSAS(accountURL string, sasToken string) (*azblob.Client, error) {
	// Create a new service client with an existing SAS token
	// Append the SAS to the account URL with a "?" delimiter
	accountURLWithSAS := fmt.Sprintf("%s?%s",
		strings.TrimRight(accountURL, "?"),
		strings.TrimPrefix(sasToken, "?"))
	client, err := azblob.NewClientWithNoCredential(accountURLWithSAS, blobClientOptions())
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
	client, err := azblob.NewClientWithSharedKeyCredential(accountURL, credential, blobClientOptions())
	if err != nil {
		return nil, fmt.Errorf("failed to create Azure Blob Storage client: %w", err)
	}
	return client, nil
}

// blobClientOptions sets the per-try deadline and the User-Agent application id.
func blobClientOptions() *azblob.ClientOptions {
	options := &azblob.ClientOptions{}
	options.Retry.TryTimeout = azureTryTimeout
	options.Telemetry.ApplicationID = "olake"
	return options
}

// Connection string credential - use storage account connection string
func getServiceClientConnectionString(connectionString string) (*azblob.Client, error) {
	// Create a new service client with connection string
	client, err := azblob.NewClientFromConnectionString(connectionString, blobClientOptions())
	if err != nil {
		return nil, fmt.Errorf("failed to create Azure Blob Storage client: %w", err)
	}
	return client, nil
}
