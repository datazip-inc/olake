// Package sink defines the staged-output contract for the writers pipeline: how
// encoded bytes are staged to a backend and made durable via two-phase commit.
// Implementations live in subpackages (sink/s3, sink/localfs); the Iceberg gRPC
// server is another. The roller drives the staging half (Open/Stage); the
// top-level PartitionedRollingWriter drives the finalize half (Commit/Abort).
package sink

import (
	"context"
	"io"
)

// StagedSink is the seam that differs between backends. It spans both halves of
// two-phase commit:
//
//	Open   - give me an io.Writer for the next file (and a handle to identify it).
//	Stage  - the encoder finished and flushed this file; stage it for committing.
//	Commit - make every staged file durably visible (the unit of work succeeded).
//	Abort  - discard every staged file (the unit of work failed).
//
// Stage does NOT make a file durably visible — only Commit does, so output stays
// transactional (nothing is published until the whole unit of work succeeds).
// Open/Stage are driven per file by the Roller; Commit/Abort are driven once, at
// the end of the stream, by the PartitionedRollingWriter. Local disk, S3, and the
// Iceberg gRPC server implement these.
type StagedSink interface {
	Open(ctx context.Context) (io.Writer, FileHandle, error)
	Stage(ctx context.Context, handle FileHandle, rowCount int64) error
	Commit(ctx context.Context) error
	Abort(ctx context.Context) error
}

// FileHandle is opaque state a Sink threads from Open to Stage — e.g. the open
// *os.File for local disk, or the staged temp path + key for S3. The Roller never
// inspects it.
type FileHandle any

// FilePathFunc names the i-th rolled file. Backends plug in their own naming: S3
// uses its partition-pattern path, Iceberg asks the Java server. index is the
// 0-based roll counter for this Sink.
type FilePathFunc func(index int) string
