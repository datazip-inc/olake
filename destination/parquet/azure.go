package parquet

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
)

// azureStore is the implementation of the ObjectStore interface for Azure Blob Storage.
type azureStore struct {
	client    *azblob.Client
	cred      *azblob.SharedKeyCredential
	container string
	prefix    string
}

// newAzureStore creates a new Azure Blob Storage client with shared key credential and returns a new AzureStore.
func newAzureStore(cfg *Config) (*azureStore, error) {
	cred, err := azblob.NewSharedKeyCredential(cfg.AzureStorageAccountName, cfg.AzureStorageAccountKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create Azure SharedKeyCredential: %w", err)
	}
	serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net/", cfg.AzureStorageAccountName)
	client, err := azblob.NewClientWithSharedKeyCredential(serviceURL, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create Azure Blob Storage client: %w", err)
	}
	cfg.AzurePath = strings.Trim(cfg.AzurePath, "/")
	return &azureStore{
		client:    client,
		cred:      cred,
		container: cfg.AzureContainerName,
		prefix:    cfg.AzurePath,
	}, nil
}

func (a *azureStore) Kind() string { return "azure" }

func (a *azureStore) ObjectKey(relativePath string) string {
	if a.prefix == "" {
		return relativePath
	}
	return path.Join(a.prefix, relativePath)
}

// blobClient creates a new blob client for the given key.
func (a *azureStore) blobClient(key string) *blob.Client {
	return a.client.ServiceClient().NewContainerClient(a.container).NewBlobClient(key)
}

func (a *azureStore) Put(ctx context.Context, key string, data []byte) error {
	_, err := a.client.UploadBuffer(ctx, a.container, key, data, nil)
	return err
}

func (a *azureStore) UploadFile(ctx context.Context, key string, file *os.File) error {
	_, err := a.client.UploadFile(ctx, a.container, key, file, nil)
	return err
}

func (a *azureStore) Get(ctx context.Context, key string) ([]byte, error) {
	resp, err := a.client.DownloadStream(ctx, a.container, key, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func (a *azureStore) List(ctx context.Context, prefix string) ([]string, error) {
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
	sort.Strings(keys)
	return keys, nil
}

func (a *azureStore) Copy(ctx context.Context, srcKey, dstKey string) error {
	dst := a.blobClient(dstKey)
	props, err := dst.GetProperties(ctx, nil)
	if err != nil && !a.IsNotFound(err) {
		return fmt.Errorf("failed to poll azure copy %s -> %s: %w", srcKey, dstKey, err)
	}
	if err == nil && props.CopyStatus != nil && *props.CopyStatus == blob.CopyStatusTypePending {
		return a.waitForCopy(ctx, dst, srcKey, dstKey)
	}

	srcURL, err := a.blobReadURL(srcKey)
	if err != nil {
		return err
	}
	if _, err := dst.StartCopyFromURL(ctx, srcURL, nil); err != nil {
		if !bloberror.HasCode(err, bloberror.PendingCopyOperation) {
			return fmt.Errorf("failed to start azure copy %s -> %s: %w", srcKey, dstKey, err)
		}
	}
	return a.waitForCopy(ctx, dst, srcKey, dstKey)
}

func (a *azureStore) waitForCopy(ctx context.Context, dst *blob.Client, srcKey, dstKey string) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
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
				return ctx.Err()
			case <-time.After(200 * time.Millisecond):
			}
		default:
			return fmt.Errorf("azure copy %s -> %s: %s", srcKey, dstKey, *props.CopyStatus)
		}
	}
}

// blobReadURL creates a read SAS token for the given key.
func (a *azureStore) blobReadURL(key string) (string, error) {
	permissions := sas.BlobPermissions{Read: true}
	sasValues := sas.BlobSignatureValues{
		Protocol:      sas.ProtocolHTTPS,
		ExpiryTime:    time.Now().UTC().Add(time.Hour),
		Permissions:   permissions.String(),
		ContainerName: a.container,
		BlobName:      key,
	}
	query, err := sasValues.SignWithSharedKey(a.cred)
	if err != nil {
		return "", fmt.Errorf("failed to sign azure copy source %s: %w", key, err)
	}
	return a.blobClient(key).URL() + "?" + query.Encode(), nil
}

func (a *azureStore) Delete(ctx context.Context, key string) error {
	_, err := a.client.DeleteBlob(ctx, a.container, key, nil)
	if a.IsNotFound(err) {
		return nil
	}
	return err
}

func (a *azureStore) DeletePrefix(ctx context.Context, prefix string) error {
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
}

func (a *azureStore) IsNotFound(err error) bool {
	return bloberror.HasCode(err, bloberror.BlobNotFound)
}
