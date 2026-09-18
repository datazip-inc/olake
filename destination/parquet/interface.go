package parquet

import (
	"context"
	"fmt"
	"os"
)

// ObjectStore is S3 or Azure Blob. 2PC uses Get/List/Copy/Delete.
// kind is used to identify the store type.
// ObjectKey is used to construct the key for the object.
// Put is used to upload data to the object.
// UploadFile is used to upload a file to the object.
// Get is used to get data from the object.
// List is used to list objects with a prefix.
// Copy is used to copy an object to another object.
// Delete is used to delete an object
// DeletePrefix is used to delete objects with a prefix.
// IsNotFound is used to check if an object is not found.
type ObjectStore interface {
	Kind() string
	ObjectKey(relativePath string) string
	Put(ctx context.Context, key string, data []byte) error
	UploadFile(ctx context.Context, key string, file *os.File) error
	Get(ctx context.Context, key string) ([]byte, error)
	List(ctx context.Context, prefix string) ([]string, error)
	Copy(ctx context.Context, srcKey, dstKey string) error
	Delete(ctx context.Context, key string) error
	DeletePrefix(ctx context.Context, prefix string) error
	IsNotFound(err error) bool
}

// newObjectStore creates a new ObjectStore based on the configuration Azure or S3.
func newObjectStore(cfg *Config) (ObjectStore, error) {
	switch {
	case cfg.usingAzure():
		return newAzureStore(cfg)
	case cfg.Bucket != "" && cfg.Region != "":
		return newS3Store(cfg)
	default:
		return nil, fmt.Errorf("no remote object store configured")
	}
}
