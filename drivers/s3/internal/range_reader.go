package driver

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/datazip-inc/olake/utils/logger"
)

// S3RangeReader implements io.ReaderAt interface for parquet-go library
// It uses S3 range requests to read specific byte ranges without loading the entire file
type S3RangeReader struct {
	ctx    context.Context
	client *s3.Client
	bucket string
	key    string
	size   int64
}

// NewS3RangeReader creates a new S3RangeReader
func NewS3RangeReader(ctx context.Context, client *s3.Client, bucket, key string, size int64) *S3RangeReader {
	return &S3RangeReader{
		ctx:    ctx,
		client: client,
		bucket: bucket,
		key:    key,
		size:   size,
	}
}

// ReadAt reads len(p) bytes from the S3 object starting at byte offset off
// This implements io.ReaderAt interface required by parquet-go
func (r *S3RangeReader) ReadAt(p []byte, off int64) (n int, err error) {
	// Validate offset
	if off < 0 {
		return 0, fmt.Errorf("invalid offset: %d", off)
	}

	if off >= r.size {
		return 0, io.EOF
	}

	// Calculate the range to read
	endByte := off + int64(len(p)) - 1
	if endByte >= r.size {
		endByte = r.size - 1
	}

	// S3 range format: "bytes=start-end" (inclusive)
	rangeHeader := fmt.Sprintf("bytes=%d-%d", off, endByte)

	logger.Debugf("S3 Range Request: %s for %s (size: %d bytes)", rangeHeader, r.key, endByte-off+1)

	// Make the range request to S3
	result, err := r.client.GetObject(r.ctx, &s3.GetObjectInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(r.key),
		Range:  aws.String(rangeHeader),
	})
	if err != nil {
		return 0, fmt.Errorf("failed to read range %s: %s", rangeHeader, err)
	}
	defer result.Body.Close()

	// Read the data into the buffer
	totalRead := 0
	for totalRead < len(p) {
		nr, err := result.Body.Read(p[totalRead:])
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
			return totalRead, fmt.Errorf("failed to read response body: %s", err)
		}
	}

	return totalRead, nil
}

// Size returns the total size of the S3 object
func (r *S3RangeReader) Size() int64 {
	return r.size
}
