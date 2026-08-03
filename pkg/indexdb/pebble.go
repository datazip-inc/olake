// Package indexdb provides a pebble-backed implementation of
// types.TableIndexStore: a durable map from row identifier to the location of
// that row in a destination table, used to turn equality deletes into
// positional deletes.
//
// Key space, one database per stream. Each family gets a distinct single-byte
// prefix so it can be scanned and range-deleted without touching the others:
//
//	0x01 <row id>                 -> uvarint(file id) uvarint(position)
//	0x02 <be64 file id>           -> file path
//	0x03 <file path>              -> be64 file id
//	0x04 <be64 txn id> <row id>   -> undo record
//	0x05 <name>                   -> counter or snapshot metadata
//
// Row values reference an interned file id rather than the full object-store URI
// so that a table with hundreds of millions of rows spends a couple of bytes per
// row on file identity instead of a hundred.
package indexdb

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sync"

	"github.com/cockroachdb/pebble/v2"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
)

const (
	prefixRow        byte = 0x01
	prefixFileByID   byte = 0x02
	prefixFileByPath byte = 0x03
	prefixUndo       byte = 0x04
	prefixMeta       byte = 0x05

	// Undo record tags describing what a key looked like before a txn touched it.
	undoAbsent  byte = 0x00
	undoPresent byte = 0x01

	// batchEntries bounds how many mutations are held in memory before being
	// handed to pebble. Every flush is atomic and carries the undo records for
	// the rows it writes, so a txn can span an arbitrarily large backfill
	// without its memory footprint growing.
	batchEntries = 4096

	// formatVersion identifies the on-disk encoding. The index is derived from
	// the destination table and can always be rebuilt, so an index written by a
	// different version is discarded rather than interpreted. Bump this whenever
	// the meaning of any key or value family changes.
	formatVersion uint64 = 2
)

var (
	metaSnapshot      = []byte("snapshot")
	metaNextFileID    = []byte("next_file_id")
	metaNextTxnID     = []byte("next_txn_id")
	metaFormatVersion = []byte("format_version")

	errTxnDone = errors.New("row index txn is already committed or rolled back")

	unsafeDirChars = regexp.MustCompile(`[^a-zA-Z0-9_.-]+`)
)

// rowGetter is satisfied by both *pebble.DB and *pebble.Batch, which lets a
// lookup run either against the committed index or against a txn's own indexed
// batch so that it observes the txn's staged writes.
type rowGetter interface {
	Get(key []byte) ([]byte, io.Closer, error)
}

type pebbleStore struct {
	opts Options

	mu      sync.Mutex
	indexes map[string]*pebbleIndex
	closed  bool
}

// NewPebbleStore returns a TableIndexStore that keeps each stream's row index in
// its own pebble database beneath opts.Dir.
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

type pebbleIndex struct {
	dir string
	db  *pebble.DB

	// File path interning. The maps are a cache over the 0x02/0x03 key families.
	fileMu     sync.RWMutex
	pathToID   map[string]uint64
	idToPath   map[uint64]string
	idToSeqNum map[uint64]int64
	nextFileID uint64

	txnMu     sync.Mutex
	nextTxnID uint64
}

func openIndex(dir string, opts Options) (*pebbleIndex, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create row index directory %s: %s", dir, err)
	}

	pebbleOpts := &pebble.Options{
		MemTableSize: opts.MemTableSize,
		MaxOpenFiles: opts.MaxOpenFiles,
		Logger:       pebbleLogger{},
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
		dir:        dir,
		db:         db,
		pathToID:   make(map[string]uint64),
		idToPath:   make(map[uint64]string),
		idToSeqNum: make(map[uint64]int64),
	}

	if err := index.load(); err != nil {
		_ = db.Close()
		return nil, err
	}

	return index, nil
}

func (i *pebbleIndex) load() error {
	// Nothing on disk may be interpreted before the encoding is known to match.
	if err := i.checkFormatVersion(); err != nil {
		return err
	}

	var err error
	if i.nextFileID, err = i.readCounter(metaNextFileID); err != nil {
		return err
	}
	if i.nextTxnID, err = i.readCounter(metaNextTxnID); err != nil {
		return err
	}

	// An undo log on disk at open time belongs to a txn that never reached
	// Commit, meaning the previous process died mid-sync. Restoring it now is
	// what keeps the index consistent with the last successful destination
	// commit rather than with files that were never registered.
	prefix := []byte{prefixUndo}
	restored, err := i.undo(prefix, prefixEnd(prefix))
	if err != nil {
		return err
	}
	if restored > 0 {
		logger.Warnf("row index %s: rolled back %d entries left behind by an interrupted sync", i.dir, restored)
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
	return i.lookupFrom(i.db, key)
}

func (i *pebbleIndex) lookupFrom(src rowGetter, key string) (types.RowLocation, bool, error) {
	value, closer, err := src.Get(rowKey(key))
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

	path, seqNum, err := i.fileLocation(fileID)
	if err != nil {
		return types.RowLocation{}, false, err
	}

	return types.RowLocation{FilePath: path, Position: position, SeqNumber: seqNum}, true, nil
}

func (i *pebbleIndex) NewTxn() (types.IndexTxn, error) {
	i.txnMu.Lock()
	defer i.txnMu.Unlock()

	id := i.nextTxnID
	i.nextTxnID++

	// Persist the counter before the txn writes anything, so a crash can never
	// hand the same txn id (and therefore the same undo key range) to two txns.
	batch := i.db.NewBatch()
	defer batch.Close()
	if err := setCounter(batch, metaNextTxnID, i.nextTxnID); err != nil {
		return nil, err
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return nil, fmt.Errorf("failed to reserve row index txn id: %s", err)
	}

	return &pebbleTxn{index: i, id: id, batch: i.db.NewIndexedBatch()}, nil
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

func (i *pebbleIndex) SetIndexedSnapshot(snapshotID int64) error {
	if err := i.db.Set(metaKey(metaSnapshot), binary.AppendVarint(nil, snapshotID), pebble.Sync); err != nil {
		return fmt.Errorf("failed to record indexed snapshot[%d] in row index %s: %s", snapshotID, i.dir, err)
	}
	return nil
}

// Truncate clears the whole index. Callers must have committed or rolled back
// every txn first.
func (i *pebbleIndex) Truncate() error {
	batch := i.db.NewBatch()
	defer batch.Close()

	for _, prefix := range [][]byte{{prefixRow}, {prefixFileByID}, {prefixFileByPath}, {prefixUndo}, {prefixMeta}} {
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
	i.idToSeqNum = make(map[uint64]int64)
	i.nextFileID = 0
	i.fileMu.Unlock()

	i.txnMu.Lock()
	i.nextTxnID = 0
	i.txnMu.Unlock()

	return nil
}

func (i *pebbleIndex) Close() error {
	if err := i.db.Close(); err != nil {
		return fmt.Errorf("failed to close row index %s: %s", i.dir, err)
	}
	return nil
}

// A file dictionary entry is the row's sequence number followed by its path. The
// sequence number leads so that its width is fixed and the remainder is the path
// verbatim, whatever it contains.
func encodeFileValue(path string, seqNumber int64) []byte {
	value := make([]byte, 8+len(path))
	// #nosec G115 -- the cast is a reinterpretation, undone by decodeFileValue.
	binary.BigEndian.PutUint64(value[:8], uint64(seqNumber))
	copy(value[8:], path)
	return value
}

func decodeFileValue(value []byte) (path string, seqNumber int64, err error) {
	if len(value) < 8 {
		return "", 0, fmt.Errorf("expected at least 8 bytes, got %d", len(value))
	}
	// #nosec G115 -- round trip of the int64 written by encodeFileValue.
	return string(value[8:]), int64(binary.BigEndian.Uint64(value[:8])), nil
}

// fileID interns path into a compact integer, recording the sequence number of
// the file it names. The dictionary entry is made durable immediately instead of
// joining the caller's txn: an id that no row ends up referencing wastes a few
// bytes, whereas a row value referencing an unknown id would be unreadable.
//
// A path already interned keeps its id. Its recorded sequence number is upgraded
// if it was interned as types.UnknownSeqNumber and a real one is now known,
// which is how files first indexed while being written pick up the number
// Iceberg assigned them at commit.
func (i *pebbleIndex) fileID(path string, seqNumber int64) (uint64, error) {
	i.fileMu.RLock()
	id, cached := i.pathToID[path]
	knownSeq, seqCached := i.idToSeqNum[id]
	i.fileMu.RUnlock()
	if cached && seqCached && !upgradesSeqNumber(knownSeq, seqNumber) {
		return id, nil
	}

	i.fileMu.Lock()
	defer i.fileMu.Unlock()

	id, found, err := i.readFileID(path)
	if err != nil {
		return 0, err
	}

	storedSeq := seqNumber
	if found {
		_, storedSeq, err = i.readFileValue(id)
		if err != nil {
			return 0, err
		}
	} else {
		id = i.nextFileID
	}

	if !found || upgradesSeqNumber(storedSeq, seqNumber) {
		batch := i.db.NewBatch()
		defer batch.Close()
		if err := batch.Set(fileByIDKey(id), encodeFileValue(path, seqNumber), nil); err != nil {
			return 0, fmt.Errorf("failed to stage file id[%d] for %s: %s", id, path, err)
		}
		if err := batch.Set(fileByPathKey(path), be64(id), nil); err != nil {
			return 0, fmt.Errorf("failed to stage file path %s: %s", path, err)
		}
		if err := setCounter(batch, metaNextFileID, max(i.nextFileID, id+1)); err != nil {
			return 0, err
		}
		if err := batch.Commit(pebble.Sync); err != nil {
			return 0, fmt.Errorf("failed to intern file path %s: %s", path, err)
		}

		storedSeq = seqNumber
		i.nextFileID = max(i.nextFileID, id+1)
	}

	i.pathToID[path] = id
	i.idToPath[id] = path
	i.idToSeqNum[id] = storedSeq
	return id, nil
}

// upgradesSeqNumber reports whether replacing known with incoming teaches the
// dictionary something. Only filling in an unknown number counts: a data file's
// sequence number never changes once Iceberg has assigned one, so a second,
// different value would mean the path was reused and is not something to follow.
func upgradesSeqNumber(known, incoming int64) bool {
	return known == types.UnknownSeqNumber && incoming != types.UnknownSeqNumber
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

// fileLocation resolves an interned id back to the path and sequence number of
// the file it names.
func (i *pebbleIndex) fileLocation(id uint64) (string, int64, error) {
	i.fileMu.RLock()
	path, pathCached := i.idToPath[id]
	seqNumber, seqCached := i.idToSeqNum[id]
	i.fileMu.RUnlock()
	if pathCached && seqCached {
		return path, seqNumber, nil
	}

	path, seqNumber, err := i.readFileValue(id)
	if err != nil {
		return "", 0, err
	}

	i.fileMu.Lock()
	i.idToPath[id] = path
	i.pathToID[path] = id
	i.idToSeqNum[id] = seqNumber
	i.fileMu.Unlock()

	return path, seqNumber, nil
}

func (i *pebbleIndex) readFileValue(id uint64) (string, int64, error) {
	value, closer, err := i.db.Get(fileByIDKey(id))
	if err != nil {
		return "", 0, fmt.Errorf("failed to resolve indexed file id[%d] in %s: %s", id, i.dir, err)
	}

	path, seqNumber, decodeErr := decodeFileValue(value)
	if err := closer.Close(); err != nil {
		return "", 0, fmt.Errorf("failed to release file id[%d] read: %s", id, err)
	}
	if decodeErr != nil {
		return "", 0, fmt.Errorf("corrupt file dictionary entry for id[%d] in %s: %s", id, i.dir, decodeErr)
	}

	return path, seqNumber, nil
}

// undo replays the undo records in [start, end) newest txn first, so that when
// several txns overwrote one key the oldest record is applied last and wins. It
// is idempotent: re-running it after an interruption reaches the same state.
func (i *pebbleIndex) undo(start, end []byte) (int, error) {
	iter, err := i.db.NewIter(&pebble.IterOptions{LowerBound: start, UpperBound: end})
	if err != nil {
		return 0, fmt.Errorf("failed to scan row index undo log in %s: %s", i.dir, err)
	}
	defer iter.Close()

	batch := i.db.NewBatch()
	defer func() {
		_ = batch.Close()
	}()

	restored, pending := 0, 0
	for ok := iter.Last(); ok; ok = iter.Prev() {
		key := iter.Key()
		if len(key) < 9 {
			return restored, fmt.Errorf("corrupt row index undo key of length %d in %s", len(key), i.dir)
		}
		if err := applyUndo(batch, key[9:], iter.Value()); err != nil {
			return restored, err
		}

		restored++
		pending++
		if pending < batchEntries {
			continue
		}
		if err := batch.Commit(pebble.NoSync); err != nil {
			return restored, fmt.Errorf("failed to apply row index undo log in %s: %s", i.dir, err)
		}
		if err := batch.Close(); err != nil {
			return restored, fmt.Errorf("failed to release row index undo batch in %s: %s", i.dir, err)
		}
		batch, pending = i.db.NewBatch(), 0
	}

	if err := batch.DeleteRange(start, end, nil); err != nil {
		return restored, fmt.Errorf("failed to discard row index undo log in %s: %s", i.dir, err)
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return restored, fmt.Errorf("failed to commit row index undo log in %s: %s", i.dir, err)
	}

	return restored, nil
}

type pebbleTxn struct {
	index *pebbleIndex
	id    uint64

	mu      sync.Mutex
	batch   *pebble.Batch
	pending int
	done    bool
}

func (t *pebbleTxn) Lookup(key string) (types.RowLocation, bool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.done {
		return types.RowLocation{}, false, errTxnDone
	}
	return t.index.lookupFrom(t.batch, key)
}

func (t *pebbleTxn) Put(key string, loc types.RowLocation) error {
	if loc.Position < 0 {
		return fmt.Errorf("row index position for key[%s] must not be negative, got %d", key, loc.Position)
	}

	// Interning takes the index-level lock, so resolve the id before taking the
	// txn lock to keep the two orders from ever crossing.
	fileID, err := t.index.fileID(loc.FilePath, loc.SeqNumber)
	if err != nil {
		return err
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.done {
		return errTxnDone
	}
	if err := t.recordUndo(key); err != nil {
		return err
	}
	if err := t.batch.Set(rowKey(key), encodeRow(fileID, uint64(loc.Position)), nil); err != nil {
		return fmt.Errorf("failed to stage row index entry for key[%s]: %s", key, err)
	}

	return t.flushIfFull()
}

func (t *pebbleTxn) Delete(key string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.done {
		return errTxnDone
	}
	if err := t.recordUndo(key); err != nil {
		return err
	}
	if err := t.batch.Delete(rowKey(key), nil); err != nil {
		return fmt.Errorf("failed to stage row index delete for key[%s]: %s", key, err)
	}

	return t.flushIfFull()
}

func (t *pebbleTxn) Commit() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.done {
		return errTxnDone
	}
	t.done = true

	if err := t.flush(); err != nil {
		return err
	}
	defer func() {
		_ = t.batch.Close()
	}()

	start, end := undoRange(t.id)
	if err := t.batch.DeleteRange(start, end, nil); err != nil {
		return fmt.Errorf("failed to discard undo log for row index txn[%d]: %s", t.id, err)
	}
	if err := t.batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("failed to commit row index txn[%d]: %s", t.id, err)
	}

	return nil
}

func (t *pebbleTxn) Rollback() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.done {
		return errTxnDone
	}
	t.done = true

	// Anything still staged never reached pebble, so dropping the batch is
	// enough for it; everything already flushed is reverted from the undo log.
	if err := t.batch.Close(); err != nil {
		return fmt.Errorf("failed to discard staged row index txn[%d]: %s", t.id, err)
	}

	start, end := undoRange(t.id)
	if _, err := t.index.undo(start, end); err != nil {
		return err
	}

	return nil
}

// recordUndo logs the pre-txn value of key, but only on its first mutation
// within this txn, so that a rollback restores the value the txn started from
// rather than an intermediate one.
func (t *pebbleTxn) recordUndo(key string) error {
	logKey := undoKey(t.id, key)

	switch _, closer, err := t.batch.Get(logKey); {
	case err == nil:
		return closer.Close()
	case !errors.Is(err, pebble.ErrNotFound):
		return fmt.Errorf("failed to read row index undo log for key[%s]: %s", key, err)
	}

	undo := []byte{undoAbsent}
	value, closer, err := t.batch.Get(rowKey(key))
	switch {
	case err == nil:
		undo = append([]byte{undoPresent}, value...)
		if err := closer.Close(); err != nil {
			return fmt.Errorf("failed to release row index read for key[%s]: %s", key, err)
		}
	case !errors.Is(err, pebble.ErrNotFound):
		return fmt.Errorf("failed to read row index for key[%s]: %s", key, err)
	}

	if err := t.batch.Set(logKey, undo, nil); err != nil {
		return fmt.Errorf("failed to stage row index undo record for key[%s]: %s", key, err)
	}

	return nil
}

func (t *pebbleTxn) flushIfFull() error {
	t.pending++
	if t.pending < batchEntries {
		return nil
	}
	return t.flush()
}

// flush hands the staged mutations to pebble. Row writes and the undo records
// covering them land in one atomic batch, which is what makes an interrupted txn
// recoverable at the next open.
func (t *pebbleTxn) flush() error {
	if t.batch.Empty() {
		t.pending = 0
		return nil
	}

	if err := t.batch.Commit(pebble.NoSync); err != nil {
		return fmt.Errorf("failed to flush row index txn[%d]: %s", t.id, err)
	}
	if err := t.batch.Close(); err != nil {
		return fmt.Errorf("failed to release row index batch for txn[%d]: %s", t.id, err)
	}

	t.batch = t.index.db.NewIndexedBatch()
	t.pending = 0
	return nil
}

func applyUndo(batch *pebble.Batch, rowID, undo []byte) error {
	if len(undo) == 0 {
		return fmt.Errorf("empty row index undo record for key[%s]", rowID)
	}

	switch undo[0] {
	case undoAbsent:
		if err := batch.Delete(rowKeyBytes(rowID), nil); err != nil {
			return fmt.Errorf("failed to undo row index entry for key[%s]: %s", rowID, err)
		}
	case undoPresent:
		if err := batch.Set(rowKeyBytes(rowID), undo[1:], nil); err != nil {
			return fmt.Errorf("failed to restore row index entry for key[%s]: %s", rowID, err)
		}
	default:
		return fmt.Errorf("unknown row index undo tag[%#x] for key[%s]", undo[0], rowID)
	}

	return nil
}

func setCounter(batch *pebble.Batch, name []byte, value uint64) error {
	if err := batch.Set(metaKey(name), binary.AppendUvarint(nil, value), nil); err != nil {
		return fmt.Errorf("failed to stage row index counter %s: %s", name, err)
	}
	return nil
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

func rowKey(id string) []byte {
	return append(append(make([]byte, 0, 1+len(id)), prefixRow), id...)
}

func rowKeyBytes(id []byte) []byte {
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

func undoKey(txnID uint64, id string) []byte {
	key := append(append(make([]byte, 0, 9+len(id)), prefixUndo), be64(txnID)...)
	return append(key, id...)
}

func undoRange(txnID uint64) (start, end []byte) {
	start = undoKey(txnID, "")
	return start, prefixEnd(start)
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

// prefixEnd returns the exclusive upper bound covering every key that starts
// with prefix. A nil result means the range runs to the end of the key space.
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
