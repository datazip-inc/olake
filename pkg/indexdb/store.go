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
	"strconv"
	"sync"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
)

var unsafeDirChars = regexp.MustCompile(`[^a-zA-Z0-9_.-]+`)

type StoreOptions struct {
	Dir          string
	CacheSize    int64
	MemTableSize uint64
	MaxOpenFiles int
}

func DefaultOptions() StoreOptions {
	dir := os.Getenv(constants.IndexDBDir)
	// TODO: make env keys equal to pebble support instead of custom defined

	// if not set in env use current working directory
	if dir == "" {
		wd, _ := os.Getwd()
		dir = filepath.Join(wd, "olake-row-index")
	}

	cacheSize, err := strconv.Atoi(os.Getenv(constants.IndexDBCacheSizePerStream))
	if err != nil {
		logger.Errorf("failed to parse index db cache size (using default %d MB): %s", 128, err)
		cacheSize = 128 * 1024 * 1024 // 128 MB default block cache
	}

	return StoreOptions{
		Dir:          dir,
		CacheSize:    int64(cacheSize),
		MemTableSize: 64 * 1024 * 1024, // 64 MB default memtable
		MaxOpenFiles: 1000,
	}
}

// NewStore returns a TableIndexStore backed by PebbleDB.
func NewStore() types.TableIndexStore {
	opts := DefaultOptions()
	logger.Infof("keeping row indexes in %s", opts.Dir)
	return &pebbleStore{opts: opts, indexes: make(map[string]*pebbleIndex)}
}

type pebbleStore struct {
	opts StoreOptions

	mu      sync.Mutex
	indexes map[string]*pebbleIndex
	closed  bool
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

func indexDirName(streamID string) string {
	sum := sha256.Sum256([]byte(streamID))
	safe := unsafeDirChars.ReplaceAllString(streamID, "_")
	if len(safe) > 80 {
		safe = safe[:80]
	}
	return fmt.Sprintf("%s-%s", safe, hex.EncodeToString(sum[:6]))
}
