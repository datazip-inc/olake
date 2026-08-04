// Package indexdb provides a pebble-backed implementation of
// types.TableIndexStore: a durable map from row identifier to the location of
// that row in a destination table, used to turn equality deletes into
// positional deletes.
//
// There is no undo log and no transaction. Writers buffer their changes and hand
// them over only once the destination has committed, so a failed sync leaves the
// index untouched; and because every entry is a fact a rescan can produce again,
// an interrupted Apply is repaired by leaving the checkpoint behind and letting
// the next sync rescan from it.
//
// The on-disk key layout lives in codec.go, and the per-stream database handout
// in store.go.
package indexdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
)

const (
	// batchEntries bounds how many mutations pebble is asked to hold at once.
	// A caller's buffer may be far larger than this, so Apply hands it over in
	// chunks rather than materializing one giant write batch.
	batchEntries = 4096

	// bloomBitsPerKey sizes the per-sstable bloom filters. Ten bits is pebble's
	// own suggested value and costs roughly 1.2 bytes per indexed row for a
	// false positive rate near 1%.
	bloomBitsPerKey = 10

	// maxCompactions caps how many compactions pebble may run at once. The
	// default of one falls behind a sustained ingest and eventually stalls
	// writes, so the ceiling is raised while leaving the steady-state
	// concurrency at one.
	maxCompactions = 4

	// formatVersion identifies the on-disk encoding. The index is derived from
	// the destination table and can always be rebuilt, so an index written by a
	// different version is discarded rather than interpreted. Bump this whenever
	// the meaning of any key or value family changes.
	formatVersion uint64 = 4
)

type pebbleIndex struct {
	dir string
	db  *pebble.DB

	// File path interning. The maps are a cache over the 0x02/0x03 key families.
	fileMu     sync.RWMutex
	pathToID   map[string]uint64
	idToPath   map[uint64]string
	nextFileID uint64

	// applyMu serializes Apply so that two threads committing at once cannot
	// interleave their chunks and land a checkpoint over a half-written batch.
	applyMu sync.Mutex
}

func openIndex(dir string, opts Options) (*pebbleIndex, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create row index directory %s: %s", dir, err)
	}

	pebbleOpts := &pebble.Options{
		MemTableSize:               opts.MemTableSize,
		MaxOpenFiles:               opts.MaxOpenFiles,
		Logger:                     pebbleLogger{},
		CompactionConcurrencyRange: func() (int, int) { return 1, maxCompactions },
	}

	// Pebble builds no filters unless asked for them. Lookups during a backfill
	// are overwhelmingly misses, and a miss with no filter has to read index
	// blocks from every level before it can be answered.
	for level := range pebbleOpts.Levels {
		pebbleOpts.Levels[level].FilterPolicy = bloom.FilterPolicy(bloomBitsPerKey)
	}

	// This stream's cache, not a shared one, so that a stream's memory can be
	// reasoned about on its own. Open takes its own reference and drops it when
	// the database closes, which leaves nothing here to hold.
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

// load prepares an index for use. There is deliberately no recovery step here:
// an index left behind by a process that died mid-sync is not inconsistent, only
// out of date, because nothing reaches it until the destination has committed.
// Its checkpoint tells the next sync how far to rescan.
func (i *pebbleIndex) load() error {
	// Nothing on disk may be interpreted before the encoding is known to match.
	if err := i.checkFormatVersion(); err != nil {
		return err
	}

	var err error
	if i.nextFileID, err = i.readCounter(metaNextFileID); err != nil {
		return err
	}

	return nil
}

// checkFormatVersion discards an index left behind by a different on-disk
// encoding. A missing stamp means either an empty directory or one written
// before versioning existed; clearing both is safe because the index only ever
// holds what a rescan of the destination table can produce again.
func (i *pebbleIndex) checkFormatVersion() error {
	stored, err := i.readCounter(metaFormatVersion)
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

// Apply writes batch to the index and, when snapshotID is non-nil, moves the
// checkpoint to it.
//
// A batch larger than one pebble write batch is handed over in chunks. That is
// safe without any rollback machinery because the checkpoint rides with the last
// chunk: a crash partway leaves the index holding some of the batch under the
// old checkpoint, and rescanning from that checkpoint re-derives exactly the
// entries that landed, so replaying converges on the same state.
func (i *pebbleIndex) Apply(batch *types.RowIndexBatch, snapshotID *int64) error {
	i.applyMu.Lock()
	defer i.applyMu.Unlock()

	pending := i.db.NewBatch()
	defer func() {
		_ = pending.Close()
	}()

	staged := 0
	if batch != nil {
		err := batch.Range(func(key string, loc types.RowLocation, deleted bool) error {
			if err := i.stageChange(pending, key, loc, deleted); err != nil {
				return err
			}

			staged++
			if staged < batchEntries {
				return nil
			}

			if err := pending.Commit(pebble.NoSync); err != nil {
				return fmt.Errorf("failed to write row index chunk in %s: %s", i.dir, err)
			}
			if err := pending.Close(); err != nil {
				return fmt.Errorf("failed to release row index chunk in %s: %s", i.dir, err)
			}
			pending, staged = i.db.NewBatch(), 0
			return nil
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

func (i *pebbleIndex) IndexedSnapshot() (int64, bool, error) {
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

// Truncate clears the whole index, checkpoint included. Dropping the checkpoint
// is the point: an index with none is rebuilt from the table rather than
// trusted, which is what lets a from-scratch rebuild run without a rollback path.
func (i *pebbleIndex) Truncate() error {
	batch := i.db.NewBatch()
	defer batch.Close()

	for _, prefix := range [][]byte{{prefixRow}, {prefixFileByID}, {prefixFileByPath}, {prefixMeta}} {
		if err := batch.DeleteRange(prefix, prefixEnd(prefix), nil); err != nil {
			return fmt.Errorf("failed to clear row index %s: %s", i.dir, err)
		}
	}
	// The wipe takes the meta family with it, so the encoding has to be stamped
	// again in the same batch to keep an emptied index from looking unversioned.
	if err := setCounter(batch, metaFormatVersion, formatVersion); err != nil {
		return err
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("failed to commit row index truncate for %s: %s", i.dir, err)
	}

	i.fileMu.Lock()
	i.pathToID = make(map[string]uint64)
	i.idToPath = make(map[uint64]string)
	i.nextFileID = 0
	i.fileMu.Unlock()

	return nil
}

func (i *pebbleIndex) Close() error {
	if err := i.db.Close(); err != nil {
		return fmt.Errorf("failed to close row index %s: %s", i.dir, err)
	}
	return nil
}

// fileID interns path into a compact integer. The dictionary entry is made
// durable immediately instead of joining the caller's batch: an id that no row
// ends up referencing wastes a few bytes, whereas a row value referencing an
// unknown id would be unreadable.
//
// A path already interned keeps its id, so interning is idempotent and every
// call after the first is served from the cache.
func (i *pebbleIndex) fileID(path string) (uint64, error) {
	i.fileMu.RLock()
	id, cached := i.pathToID[path]
	i.fileMu.RUnlock()
	if cached {
		return id, nil
	}

	i.fileMu.Lock()
	defer i.fileMu.Unlock()

	// The cache is only ever populated from durable state, so a hit under the
	// write lock means another caller interned this path while we waited.
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

// filePath resolves an interned id back to the path of the file it names.
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

	// The conversion copies, so the path outlives the pebble-owned buffer.
	path := string(value)
	if err := closer.Close(); err != nil {
		return "", fmt.Errorf("failed to release file id[%d] read: %s", id, err)
	}

	return path, nil
}

func (i *pebbleIndex) readCounter(name []byte) (uint64, error) {
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
