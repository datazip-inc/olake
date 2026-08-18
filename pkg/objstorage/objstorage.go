// Package objstorage provides a storage-agnostic abstraction over bucket-scoped
// object stores (AWS S3 and S3-compatible services today; GCS/Azure later).
// Drivers depend on the Store interface and never on a vendor SDK, keeping
// storage operations separate from parsing and sync logic.
package objstorage

import (
	"context"
	"errors"
	"io"
	"time"
)

// ErrNotFound indicates the requested key does not exist. Implementations
// must wrap it into errors returned by Open/OpenRange for missing keys so
// callers can match with errors.Is, without knowing provider error shapes.
var ErrNotFound = errors.New("object not found")

// ObjectInfo describes an object in a bucket-scoped store.
type ObjectInfo struct {
	Key          string    // full key within the bucket
	Size         int64     // size in bytes
	LastModified time.Time // provider timestamp (UTC for S3)
	ETag         string    // provider entity tag, surrounding quotes stripped; may be empty
}

// Store is a minimal read-oriented interface over a single bucket/container.
// Implementations must be safe for concurrent use by multiple goroutines.
// Write operations (Put/Delete) can be added when destinations migrate onto
// this package.
type Store interface {
	// Check verifies the bucket exists and is accessible.
	Check(ctx context.Context) error
	// List walks every object whose key starts with prefix, invoking fn per
	// object. Pagination is handled internally; keys arrive in provider
	// listing order (lexicographic for S3). An error returned by fn aborts
	// the walk and is returned as-is.
	List(ctx context.Context, prefix string, fn func(ObjectInfo) error) error
	// Open returns the full object body. The caller must close the reader.
	Open(ctx context.Context, key string) (io.ReadCloser, error)
}

// RangeOpener reads a byte range of an object. It is kept separate from Store
// because only Parquet parsing needs ranged reads (via ReaderAt); general
// consumers depend on the minimal Store and shouldn't be forced to implement
// ranges. S3-backed stores implement both interfaces.
type RangeOpener interface {
	// OpenRange returns object bytes [offset, offset+length) with length > 0.
	// A range extending past the object end is truncated. The caller must
	// close the reader.
	OpenRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error)
}
