package indexdb

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/datazip-inc/olake/utils/logger"
)

// Environment variables that size the row index. They are read from the
// environment rather than the destination config because the destination config
// is shared across every sync of a pipeline, whereas index placement and memory
// budget belong to the machine running one particular sync.
const (
	// EnvDir is the parent directory holding one database per stream. It should
	// point at fast local disk that survives between syncs, otherwise every sync
	// pays for a full index rebuild.
	EnvDir = "OLAKE_INDEX_DB_DIR"
	// EnvCacheMB sizes the shared block cache.
	EnvCacheMB = "OLAKE_INDEX_DB_CACHE_MB"
	// EnvMemTableMB sizes each database's memtable.
	EnvMemTableMB = "OLAKE_INDEX_DB_MEMTABLE_MB"
	// EnvMaxOpenFiles caps open sstable file descriptors per database.
	EnvMaxOpenFiles = "OLAKE_INDEX_DB_MAX_OPEN_FILES"
)

const (
	defaultCacheMB      = 128
	defaultMemTableMB   = 64
	defaultMaxOpenFiles = 1000
	mib                 = 1024 * 1024
)

// Options tunes the on-disk row index.
type Options struct {
	// Dir is the parent directory containing one database per stream.
	Dir string
	// CacheSize is the block cache size in bytes, shared across streams.
	CacheSize int64
	// MemTableSize is the per-database memtable size in bytes.
	MemTableSize uint64
	// MaxOpenFiles caps open sstable file descriptors per database.
	MaxOpenFiles int
}

// OptionsFromEnv builds Options from the OLAKE_INDEX_DB_* environment
// variables, falling back to defaults for anything unset.
func OptionsFromEnv() (Options, error) {
	opts := Options{Dir: os.Getenv(EnvDir)}
	if opts.Dir == "" {
		opts.Dir = filepath.Join(os.TempDir(), "olake-row-index")
		logger.Warnf("%s is not set, keeping row indexes in %s; set it to persistent local disk to avoid rebuilding the index every sync", EnvDir, opts.Dir)
	}

	cacheMB, err := envInt(EnvCacheMB, defaultCacheMB)
	if err != nil {
		return Options{}, err
	}
	memTableMB, err := envInt(EnvMemTableMB, defaultMemTableMB)
	if err != nil {
		return Options{}, err
	}
	maxOpenFiles, err := envInt(EnvMaxOpenFiles, defaultMaxOpenFiles)
	if err != nil {
		return Options{}, err
	}

	opts.CacheSize = int64(cacheMB) * mib
	opts.MemTableSize = uint64(memTableMB) * mib // #nosec G115 - envInt rejects non-positive values
	opts.MaxOpenFiles = maxOpenFiles

	return opts, nil
}

func envInt(name string, defaultValue int) (int, error) {
	raw, ok := os.LookupEnv(name)
	if !ok || raw == "" {
		return defaultValue, nil
	}

	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("failed to parse %s[%s] as an integer: %s", name, raw, err)
	}
	if value <= 0 {
		return 0, fmt.Errorf("%s must be positive, got %d", name, value)
	}

	return value, nil
}
