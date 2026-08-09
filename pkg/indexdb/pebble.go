package indexdb

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
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
	if dir == "" {
		wd, _ := os.Getwd()
		dir = filepath.Join(wd, constants.DefaultDirName)
	}

	cacheSize, err := strconv.Atoi(os.Getenv(constants.IndexDBCacheSizePerStream))
	if err != nil {
		logger.Debugf("failed to parse index db cache size (using default %d MB): %s", constants.DefaultCacheSize, err)
		cacheSize = constants.DefaultCacheSize
	}

	return StoreOptions{
		Dir:          dir,
		CacheSize:    int64(cacheSize),
		MemTableSize: constants.DefaultMemTableSize,
		MaxOpenFiles: constants.DefaultMaxOpenFiles,
	}
}

// Open opens or creates a TableIndex for the given streamID.
func Open(streamID string) (types.TableIndex, error) {
	opts := DefaultOptions()
	dir := indexDir(opts.Dir, streamID)
	return openIndex(dir, opts)
}

// Drop removes the on-disk index directory for streamID.
func Drop(streamID string) error {
	opts := DefaultOptions()
	dir := indexDir(opts.Dir, streamID)
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("failed to remove row index for stream[%s]: %w", streamID, err)
	}
	return nil
}

func indexDir(baseDir, streamID string) string {
	sum := sha256.Sum256([]byte(streamID))
	safe := unsafeDirChars.ReplaceAllString(streamID, "_")
	if len(safe) > 80 {
		safe = safe[:80]
	}
	return filepath.Join(baseDir, fmt.Sprintf("%s-%s", safe, hex.EncodeToString(sum[:6])))
}

const (
	prefixRow        byte = 0x01
	prefixFileByID   byte = 0x02
	prefixFileByPath byte = 0x03
	prefixMeta       byte = 0x05

	bloomBitsPerKey        = 10
	maxCompactions         = 4
	formatVersion   uint64 = 4
)

var (
	metaSnapshot      = []byte("snapshot")
	metaNextFileID    = []byte("next_file_id")
	metaFormatVersion = []byte("format_version")
)

type pebbleIndex struct {
	dir string
	db  *pebble.DB

	fileMu     sync.RWMutex
	pathToID   map[string]uint64
	idToPath   map[uint64]string
	nextFileID uint64
}

func openIndex(dir string, opts StoreOptions) (*pebbleIndex, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create row index directory %s: %s", dir, err)
	}

	pebbleOpts := &pebble.Options{
		MemTableSize:               opts.MemTableSize,
		MaxOpenFiles:               opts.MaxOpenFiles,
		Logger:                     pebbleLogger{},
		CompactionConcurrencyRange: func() (int, int) { return 1, maxCompactions },
	}

	for level := range pebbleOpts.Levels {
		pebbleOpts.Levels[level].FilterPolicy = bloom.FilterPolicy(bloomBitsPerKey)
	}

	if opts.CacheSize > 0 {
		cache := pebble.NewCache(opts.CacheSize)
		defer cache.Unref()
		pebbleOpts.Cache = cache
	}

	db, err := pebble.Open(dir, pebbleOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open pebble db at %s: %s", dir, err)
	}

	index := &pebbleIndex{
		dir:      dir,
		db:       db,
		pathToID: make(map[string]uint64),
		idToPath: make(map[uint64]string),
	}

	if err := index.load(); err != nil {
		_ = db.Close()
		return nil, err
	}

	return index, nil
}

func (i *pebbleIndex) load() error {
	if err := i.checkFormatVersion(); err != nil {
		return err
	}

	var err error
	if i.nextFileID, err = i.readMetaKey(metaNextFileID); err != nil {
		return err
	}

	return nil
}

func (i *pebbleIndex) checkFormatVersion() error {
	stored, err := i.readMetaKey(metaFormatVersion)
	if err != nil {
		return err
	}

	if stored == formatVersion {
		return nil
	}

	if stored != 0 {
		logger.Warnf("row index %s: on-disk format v%d is not v%d, discarding it for a rebuild",
			i.dir, stored, formatVersion)
	}

	return i.Truncate()
}

func (i *pebbleIndex) Lookup(key string) (types.RowLocation, bool, error) {
	value, closer, err := i.db.Get(rowKey(key))
	if errors.Is(err, pebble.ErrNotFound) {
		return types.RowLocation{}, false, nil
	}
	if err != nil {
		return types.RowLocation{}, false, fmt.Errorf("failed to read row index for key[%s]: %s", key, err)
	}

	fileID, position, decodeErr := decodeRow(value)
	if err := closer.Close(); err != nil {
		return types.RowLocation{}, false, fmt.Errorf("failed to release row index read for key[%s]: %s", key, err)
	}
	if decodeErr != nil {
		return types.RowLocation{}, false, fmt.Errorf("corrupt row index entry for key[%s]: %s", key, decodeErr)
	}

	path, err := i.filePath(fileID)
	if err != nil {
		return types.RowLocation{}, false, err
	}

	return types.RowLocation{FilePath: path, Position: position}, true, nil
}

func (i *pebbleIndex) Commit(batch *types.RowIndexBatch, snapshotID *int64) error {
	pending := i.db.NewBatch()
	defer func() {
		_ = pending.Close()
	}()

	if batch != nil {
		err := batch.Range(func(key string, loc types.RowLocation, deleted bool) error {
			return i.stageChange(pending, key, loc, deleted)
		})
		if err != nil {
			return err
		}
	}

	if snapshotID != nil {
		if err := pending.Set(metaKey(metaSnapshot), binary.AppendVarint(nil, *snapshotID), nil); err != nil {
			return fmt.Errorf("failed to stage row index checkpoint[%d] in %s: %s", *snapshotID, i.dir, err)
		}
	}

	if pending.Empty() {
		return nil
	}

	if err := pending.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("failed to commit row index changes in %s: %s", i.dir, err)
	}

	if err := i.db.Flush(); err != nil {
		logger.Warnf("row index %s: failed to flush memtable after commit: %s", i.dir, err)
	}

	return nil
}

func (i *pebbleIndex) stageChange(batch *pebble.Batch, key string, loc types.RowLocation, deleted bool) error {
	if deleted {
		if err := batch.Delete(rowKey(key), nil); err != nil {
			return fmt.Errorf("failed to stage row index delete for key[%s]: %s", key, err)
		}
		return nil
	}

	if loc.Position < 0 {
		return fmt.Errorf("row index position for key[%s] must not be negative, got %d", key, loc.Position)
	}

	fileID, err := i.fileID(loc.FilePath)
	if err != nil {
		return err
	}
	if err := batch.Set(rowKey(key), encodeRow(fileID, uint64(loc.Position)), nil); err != nil {
		return fmt.Errorf("failed to stage row index entry for key[%s]: %s", key, err)
	}

	return nil
}

func (i *pebbleIndex) LastCommittedSnapshot() (int64, bool, error) {
	value, closer, err := i.db.Get(metaKey(metaSnapshot))
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("failed to read indexed snapshot from row index %s: %s", i.dir, err)
	}
	defer closer.Close()

	snapshotID, read := binary.Varint(value)
	if read <= 0 {
		return 0, false, fmt.Errorf("corrupt indexed snapshot in row index %s", i.dir)
	}

	return snapshotID, true, nil
}

func (i *pebbleIndex) Truncate() error {
	batch := i.db.NewBatch()
	defer batch.Close()

	for _, prefix := range [][]byte{{prefixRow}, {prefixFileByID}, {prefixFileByPath}, {prefixMeta}} {
		if err := batch.DeleteRange(prefix, prefixEnd(prefix), nil); err != nil {
			return fmt.Errorf("failed to clear row index %s: %s", i.dir, err)
		}
	}

	if err := setCounter(batch, metaFormatVersion, formatVersion); err != nil {
		return err
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("failed to commit row index truncate for %s: %s", i.dir, err)
	}

	i.fileMu.Lock()
	defer i.fileMu.Unlock()

	i.pathToID = make(map[string]uint64)
	i.idToPath = make(map[uint64]string)
	i.nextFileID = 0

	return nil
}

func (i *pebbleIndex) Close() error {
	if err := i.db.Close(); err != nil {
		return fmt.Errorf("failed to close row index %s: %s", i.dir, err)
	}
	return nil
}

func (i *pebbleIndex) fileID(path string) (uint64, error) {
	i.fileMu.RLock()
	id, cached := i.pathToID[path]
	i.fileMu.RUnlock()
	if cached {
		return id, nil
	}

	i.fileMu.Lock()
	defer i.fileMu.Unlock()

	if id, cached := i.pathToID[path]; cached {
		return id, nil
	}

	id, found, err := i.readFileID(path)
	if err != nil {
		return 0, err
	}

	if !found {
		id = i.nextFileID

		batch := i.db.NewBatch()
		defer batch.Close()
		if err := batch.Set(fileByIDKey(id), []byte(path), nil); err != nil {
			return 0, fmt.Errorf("failed to stage file id[%d] for %s: %s", id, path, err)
		}
		if err := batch.Set(fileByPathKey(path), be64(id), nil); err != nil {
			return 0, fmt.Errorf("failed to stage file path %s: %s", path, err)
		}
		if err := setCounter(batch, metaNextFileID, id+1); err != nil {
			return 0, err
		}
		if err := batch.Commit(pebble.Sync); err != nil {
			return 0, fmt.Errorf("failed to intern file path %s: %s", path, err)
		}

		i.nextFileID = id + 1
	}

	i.pathToID[path] = id
	i.idToPath[id] = path
	return id, nil
}

func (i *pebbleIndex) readFileID(path string) (uint64, bool, error) {
	value, closer, err := i.db.Get(fileByPathKey(path))
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("failed to read file id for %s: %s", path, err)
	}
	defer closer.Close()

	if len(value) != 8 {
		return 0, false, fmt.Errorf("corrupt file id for %s: expected 8 bytes, got %d", path, len(value))
	}
	return binary.BigEndian.Uint64(value), true, nil
}

func (i *pebbleIndex) filePath(id uint64) (string, error) {
	i.fileMu.RLock()
	path, cached := i.idToPath[id]
	i.fileMu.RUnlock()
	if cached {
		return path, nil
	}

	path, err := i.readFilePath(id)
	if err != nil {
		return "", err
	}

	i.fileMu.Lock()
	i.idToPath[id] = path
	i.pathToID[path] = id
	i.fileMu.Unlock()

	return path, nil
}

func (i *pebbleIndex) readFilePath(id uint64) (string, error) {
	value, closer, err := i.db.Get(fileByIDKey(id))
	if err != nil {
		return "", fmt.Errorf("failed to resolve indexed file id[%d] in %s: %s", id, i.dir, err)
	}

	path := string(value)
	if err := closer.Close(); err != nil {
		return "", fmt.Errorf("failed to release file id[%d] read: %s", id, err)
	}

	return path, nil
}

func (i *pebbleIndex) readMetaKey(name []byte) (uint64, error) {
	value, closer, err := i.db.Get(metaKey(name))
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, nil
	}

	if err != nil {
		return 0, fmt.Errorf("failed to read row index counter %s in %s: %s", name, i.dir, err)
	}
	defer closer.Close()

	counter, read := binary.Uvarint(value)
	if read <= 0 {
		return 0, fmt.Errorf("corrupt row index counter %s in %s", name, i.dir)
	}

	return counter, nil
}

func rowKey(id string) []byte {
	return append(append(make([]byte, 0, 1+len(id)), prefixRow), id...)
}

func fileByIDKey(id uint64) []byte {
	return append([]byte{prefixFileByID}, be64(id)...)
}

func fileByPathKey(path string) []byte {
	return append(append(make([]byte, 0, 1+len(path)), prefixFileByPath), path...)
}

func metaKey(name []byte) []byte {
	return append(append(make([]byte, 0, 1+len(name)), prefixMeta), name...)
}

func be64(value uint64) []byte {
	encoded := make([]byte, 8)
	binary.BigEndian.PutUint64(encoded, value)
	return encoded
}

func encodeRow(fileID, position uint64) []byte {
	return binary.AppendUvarint(binary.AppendUvarint(make([]byte, 0, 2*binary.MaxVarintLen64), fileID), position)
}

func decodeRow(value []byte) (fileID uint64, position int64, err error) {
	fileID, read := binary.Uvarint(value)
	if read <= 0 {
		return 0, 0, fmt.Errorf("unreadable file id")
	}

	raw, readPosition := binary.Uvarint(value[read:])
	if readPosition <= 0 {
		return 0, 0, fmt.Errorf("unreadable position")
	}
	if raw > math.MaxInt64 {
		return 0, 0, fmt.Errorf("position %d out of range", raw)
	}

	return fileID, int64(raw), nil
}

func setCounter(batch *pebble.Batch, name []byte, value uint64) error {
	if err := batch.Set(metaKey(name), binary.AppendUvarint(nil, value), nil); err != nil {
		return fmt.Errorf("failed to stage row index counter %s: %s", name, err)
	}
	return nil
}

func prefixEnd(prefix []byte) []byte {
	end := make([]byte, len(prefix))
	copy(end, prefix)

	for i := len(end) - 1; i >= 0; i-- {
		end[i]++
		if end[i] != 0 {
			return end[:i+1]
		}
	}

	return nil
}

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
