package indexdb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sync"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
)

var unsafeDirChars = regexp.MustCompile(`[^a-zA-Z0-9_.-]+`)

type pebbleStore struct {
	opts Options

	mu      sync.Mutex
	indexes map[string]*pebbleIndex
	closed  bool
}

// NewPebbleStore returns a TableIndexStore that keeps each stream's row index in
// its own pebble database beneath opts.Dir. A stream owns its memory as well as
// its files: opts sizes one database, so a sync's footprint scales with the
// number of streams it writes.
func NewPebbleStore(opts Options) types.TableIndexStore {
	return &pebbleStore{opts: opts, indexes: make(map[string]*pebbleIndex)}
}

func (s *pebbleStore) Open(_ context.Context, streamID string) (types.TableIndex, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, fmt.Errorf("row index store is already closed")
	}
	if index, exists := s.indexes[streamID]; exists {
		return index, nil
	}

	index, err := openIndex(s.indexDir(streamID), s.opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open row index for stream[%s]: %s", streamID, err)
	}

	s.indexes[streamID] = index
	return index, nil
}

func (s *pebbleStore) Drop(_ context.Context, streamID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if index, exists := s.indexes[streamID]; exists {
		delete(s.indexes, streamID)
		if err := index.Close(); err != nil {
			return err
		}
	}

	if err := os.RemoveAll(s.indexDir(streamID)); err != nil {
		return fmt.Errorf("failed to remove row index for stream[%s]: %s", streamID, err)
	}
	return nil
}

func (s *pebbleStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	var errs []error
	for streamID, index := range s.indexes {
		if err := index.Close(); err != nil {
			errs = append(errs, err)
		}
		delete(s.indexes, streamID)
	}

	return errors.Join(errs...)
}

func (s *pebbleStore) indexDir(streamID string) string {
	return filepath.Join(s.opts.Dir, indexDirName(streamID))
}

// indexDirName maps a stream ID onto a filesystem-safe directory name. The hash
// suffix keeps two stream IDs that sanitize to the same string apart.
func indexDirName(streamID string) string {
	sum := sha256.Sum256([]byte(streamID))
	safe := unsafeDirChars.ReplaceAllString(streamID, "_")
	if len(safe) > 80 {
		safe = safe[:80]
	}
	return fmt.Sprintf("%s-%s", safe, hex.EncodeToString(sum[:6]))
}

// pebbleLogger routes pebble's internal logging into OLake's logger. Pebble's
// Infof output is verbose compaction bookkeeping, so it goes to debug.
type pebbleLogger struct{}

func (pebbleLogger) Infof(format string, args ...interface{}) {
	logger.Debugf("row index: "+format, args...)
}

func (pebbleLogger) Errorf(format string, args ...interface{}) {
	logger.Errorf("row index: "+format, args...)
}

func (pebbleLogger) Fatalf(format string, args ...interface{}) {
	logger.Fatalf("row index: "+format, args...)
}
