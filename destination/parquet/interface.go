package parquet

import (
	"context"
	"errors"
	"os"
)

// errObjectNotFound is the missing-object error returned by an in-memory store.
// S3 and Azure keep their own classifiers on the store.
var errObjectNotFound = errors.New("object not found")

// ObjectStore is S3 or Azure Blob. 2PC uses Get/List/Copy/Delete.
// The store name is the Kind string on the S3 or Azure store.
// ObjectKey is used to construct the key for the object.
// Put is used to upload data to the object.
// UploadFile is used to upload a file to the object.
// Get reads a whole small object into memory, such as finish or metadata JSON. It is not for Parquet files.
// List is used to list objects with a prefix.
// Copy is used to copy an object to another object.
// Delete is used to delete an object
// DeletePrefix is used to delete objects with a prefix.
type objectStore interface {
	ObjectKey(relativePath string) string
	Put(ctx context.Context, key string, data []byte) error
	UploadFile(ctx context.Context, key string, file *os.File) error
	Get(ctx context.Context, key string) ([]byte, error)
	List(ctx context.Context, prefix string) ([]string, error)
	Copy(ctx context.Context, srcKey, dstKey string) error
	Delete(ctx context.Context, key string) error
	DeletePrefix(ctx context.Context, prefix string) error
}
