package localfs

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/datazip-inc/olake/writers/sink"
)

// LocalFSSink writes parquet files to the local filesystem transactionally:
// each rolled file is written to a temp file in its target directory and only
// renamed to its final name on Commit (atomic within a filesystem), or removed
// on Abort. It implements sink.StagedSink.
type LocalFSSink struct {
	path   sink.FilePathFunc
	index  int
	staged []localStaged
}

type localStaged struct {
	tmpPath   string
	finalPath string
}

func NewLocalFSSink(path sink.FilePathFunc) *LocalFSSink {
	return &LocalFSSink{path: path}
}

type localFileHandle struct {
	file      *os.File
	tmpPath   string
	finalPath string
}

func (l *LocalFSSink) Open(_ context.Context) (io.Writer, sink.FileHandle, error) {
	finalPath := l.path(l.index)
	l.index++

	dir := filepath.Dir(finalPath)
	if dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, nil, fmt.Errorf("failed to create dir %q: %w", dir, err)
		}
	}

	// Stage in the target directory so Publish's rename stays on one filesystem.
	f, err := os.CreateTemp(dir, "olake-*.tmp")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create staging file in %q: %w", dir, err)
	}
	return f, &localFileHandle{file: f, tmpPath: f.Name(), finalPath: finalPath}, nil
}

func (l *LocalFSSink) Stage(_ context.Context, handle sink.FileHandle, _ int64) error {
	h, ok := handle.(*localFileHandle)
	if !ok {
		return fmt.Errorf("unexpected handle type %T for LocalFSSink", handle)
	}
	if err := h.file.Close(); err != nil {
		return fmt.Errorf("failed to close staging file %q: %w", h.tmpPath, err)
	}
	l.staged = append(l.staged, localStaged{tmpPath: h.tmpPath, finalPath: h.finalPath})
	return nil
}

func (l *LocalFSSink) Commit(_ context.Context) error {
	for i, s := range l.staged {
		if err := os.Rename(s.tmpPath, s.finalPath); err != nil {
			// Keep the unrenamed entries (including the failed one) so a follow-up
			// Abort can remove their staged temp files instead of leaking them.
			l.staged = l.staged[i:]
			return fmt.Errorf("failed to publish %q: %w", s.finalPath, err)
		}
	}
	l.staged = nil
	return nil
}

func (l *LocalFSSink) Abort(_ context.Context) error {
	defer func() { l.staged = nil }()
	for _, s := range l.staged {
		_ = os.Remove(s.tmpPath)
	}
	return nil
}
