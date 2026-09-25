package objstorage

import (
	"context"
	"fmt"
	"io"

	"github.com/datazip-inc/olake/utils/logger"
)

// ReaderAt adapts ranged reads into io.ReaderAt (as required by the parquet
// parser via parser.NewParquetReaderWrapper). It depends only on RangeOpener,
// not the full Store, since ranged reads are all it needs. It is stateless per
// call - every ReadAt issues one OpenRange request - and therefore safe for
// concurrent use. The context is captured at construction because io.ReaderAt
// cannot thread a context through ReadAt; canceling it aborts subsequent reads.
type ReaderAt struct {
	ctx   context.Context
	store RangeOpener
	key   string
	size  int64
}

// NewReaderAt creates a ReaderAt over the object at key with a known size.
func NewReaderAt(ctx context.Context, store RangeOpener, key string, size int64) *ReaderAt {
	return &ReaderAt{
		ctx:   ctx,
		store: store,
		key:   key,
		size:  size,
	}
}

// ReadAt reads len(p) bytes from the object starting at byte offset off.
func (r *ReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	// Validate offset
	if off < 0 {
		return 0, fmt.Errorf("invalid offset: %d", off)
	}

	if off >= r.size {
		return 0, io.EOF
	}

	// Calculate the range to read (end byte is inclusive)
	endByte := off + int64(len(p)) - 1
	if endByte >= r.size {
		endByte = r.size - 1
	}

	logger.Debugf("Range request: bytes=%d-%d for %s (size: %d bytes)", off, endByte, r.key, endByte-off+1)

	body, err := r.store.OpenRange(r.ctx, r.key, off, endByte-off+1)
	if err != nil {
		return 0, fmt.Errorf("failed to read range bytes=%d-%d: %w", off, endByte, err)
	}
	defer body.Close()

	// Read the data into the buffer
	totalRead := 0
	for totalRead < len(p) {
		nr, err := body.Read(p[totalRead:])
		totalRead += nr

		if err == io.EOF {
			// Reached end of this range
			if off+int64(totalRead) >= r.size {
				// Also reached end of file
				return totalRead, io.EOF
			}
			// Range complete but not EOF
			return totalRead, nil
		}

		if err != nil {
			return totalRead, fmt.Errorf("failed to read response body: %w", err)
		}
	}

	return totalRead, nil
}

// Size returns the total size of the object.
func (r *ReaderAt) Size() int64 {
	return r.size
}
