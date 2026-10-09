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
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
)

const (
	azureCopyTimeout = 10 * time.Minute
	azureCopyPoll    = 200 * time.Millisecond
)

type Config struct {
	AccountName   string
	AccountKey    string
	ContainerName string
	Path          string
	Endpoint      string
}

// Store is the implementation of the ObjectStore interface for Azure Blob Storage.
type Store struct {
	client        *azblob.Client
	cred          *azblob.SharedKeyCredential
	container     string
	prefix        string
	azureEndpoint string
}

// New creates a new Azure Blob Storage client with shared key credential and returns a new Store.
func New(cfg Config) (*Store, error) {
	cred, err := azblob.NewSharedKeyCredential(cfg.AccountName, cfg.AccountKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create Azure SharedKeyCredential: %w", err)
	}
	azureEndpoint := cfg.Endpoint
	if azureEndpoint == "" {
		azureEndpoint = fmt.Sprintf("https://%s.blob.core.windows.net/", cfg.AccountName)
	}
	client, err := azblob.NewClientWithSharedKeyCredential(azureEndpoint, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create Azure Blob Storage client: %w", err)
	}
	return &Store{
		client:        client,
		cred:          cred,
		container:     cfg.ContainerName,
		prefix:        strings.Trim(cfg.Path, "/"),
		azureEndpoint: azureEndpoint,
	}, nil
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

func (a *Store) Put(ctx context.Context, key string, data []byte) error {
	_, err := a.client.UploadBuffer(ctx, a.container, key, data, nil)
	return err
}

func (a *Store) UploadFile(ctx context.Context, key string, file *os.File) error {
	_, err := a.client.UploadFile(ctx, a.container, key, file, nil)
	return err
}

func (a *Store) Get(ctx context.Context, key string) ([]byte, error) {
	out, err := a.client.DownloadStream(ctx, a.container, key, nil)
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

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
	dst := a.blobClient(dstKey)
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

func (a *Store) waitForCopy(ctx context.Context, dst *blob.Client, srcKey, dstKey string) error {
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

func (a *Store) blobReadURL(key string) (string, error) {
	permissions := sas.BlobPermissions{Read: true}
	protocol := sas.ProtocolHTTPS
	if strings.HasPrefix(a.azureEndpoint, "http://") {
		protocol = sas.ProtocolHTTPSandHTTP
	}
	sasValues := sas.BlobSignatureValues{
		Protocol:      protocol,
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

func (a *Store) Delete(ctx context.Context, key string) error {
	_, err := a.client.DeleteBlob(ctx, a.container, key, nil)
	// ignore delete when the blob or container is already gone
	if a.IsNotFound(err) || bloberror.HasCode(err, bloberror.ContainerNotFound) {
		return nil
	}
	return err
}

func (a *Store) DeletePrefix(ctx context.Context, prefix string) error {
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

// IsNotFound checks missing blob errr
func (a *Store) IsNotFound(err error) bool {
	return bloberror.HasCode(err, bloberror.BlobNotFound)
}

// IsRateLimitError reports Azure HTTP 429/503 throttle responses
func (a *Store) IsRateLimitError(err error) bool {
	var respErr *azcore.ResponseError
	return errors.As(err, &respErr) && (respErr.StatusCode == 429 || respErr.StatusCode == 503)
}
