package iceberg

import (
	"cmp"
	"maps"
	"slices"
	"sync/atomic"

	"github.com/goccy/go-json"

	"github.com/datazip-inc/olake/constants"
)

const (
	// maxWriteCacheBytes is the memory all threads' write caches share, split equally; past its share a thread
	// drops its oldest rows, which are read back from the destination if needed.
	maxWriteCacheBytes = int64(128) * 1024 * 1024 // 128 MB
	// cachedRowOverhead and cachedValueOverhead approximate what a cached row costs besides its id and value
	// bytes: measured at about 400 bytes for a row of up to 8 values (Go 1.26).
	cachedRowOverhead   = int64(384)
	cachedValueOverhead = int64(24)
)

// writeCacheThreads counts the threads keeping a write cache; each keeps to maxWriteCacheBytes / this count. An
// idle thread keeps its old share until its next batch trims it to the new one.
var writeCacheThreads atomic.Int64

// writeCache holds, for each row, the values this thread last wrote for it, so the row's next change in this
// sync takes them from memory instead of reading the destination.
type writeCache struct {
	rows   map[string]*cachedRow // olake id -> row
	bytes  int64
	writes int64 // counts writes, to tell recently written rows from old ones
}

// cachedRow is what this thread last wrote for one row. deleted means the row is gone, so none of its old
// values may come back.
type cachedRow struct {
	deleted bool
	values  map[string]any
	written int64 // order the row was last written in
}

func newWriteCache() *writeCache {
	return &writeCache{rows: make(map[string]*cachedRow)}
}

// value is the row's cached value of column; a row deleted in this sync gives the placeholder.
func (c *writeCache) value(olakeID, column string) (any, bool) {
	row := c.rows[olakeID]
	if row == nil {
		return nil, false
	}
	if row.deleted {
		return constants.UnavailableValue, true
	}
	value, found := row.values[column]
	return value, found
}

// remember caches the given columns of what this thread wrote for a row.
func (c *writeCache) remember(olakeID string, data map[string]any, columns map[string]struct{}) {
	row := c.rows[olakeID]
	if row != nil {
		c.bytes -= row.size(olakeID)
	}
	if row == nil || row.deleted {
		row = &cachedRow{values: make(map[string]any, len(columns))}
		c.rows[olakeID] = row
	}

	for column := range columns {
		if value, present := data[column]; present {
			row.values[column] = value
		}
	}
	c.writes++
	row.written = c.writes
	c.bytes += row.size(olakeID)
}

// markDeleted records that the row was deleted, which drops the values cached for it.
func (c *writeCache) markDeleted(olakeID string) {
	if row := c.rows[olakeID]; row != nil {
		c.bytes -= row.size(olakeID)
	}
	c.writes++
	row := &cachedRow{deleted: true, written: c.writes}
	c.rows[olakeID] = row
	c.bytes += row.size(olakeID)
}

// trim drops the oldest rows down to half of share once the cache is over share, and returns how many it dropped.
// Old rows sit in closed files, cheap to read again, while a recent one may sit in a file still open.
func (c *writeCache) trim(share int64) int {
	if c.bytes <= share {
		return 0
	}

	olakeIDs := slices.SortedFunc(maps.Keys(c.rows), func(a, b string) int {
		return cmp.Compare(c.rows[a].written, c.rows[b].written)
	})
	dropped := 0
	for _, olakeID := range olakeIDs {
		if c.bytes <= share/2 { // halving keeps trims rare
			break
		}
		c.bytes -= c.rows[olakeID].size(olakeID)
		delete(c.rows, olakeID)
		dropped++
	}
	return dropped
}

// size is the memory the row counts for: its id, a fixed cost per row and per value, and its values' bytes.
func (row *cachedRow) size(olakeID string) int64 {
	return int64(len(olakeID)) + cachedRowOverhead + dataSize(row.values) + int64(len(row.values))*cachedValueOverhead
}

// readBuffer holds the values read from the destination until fill moves them into their records. Unlike the
// write cache it is never trimmed, so no row is read twice; reads keep it within Resolve's limit plus one row.
type readBuffer struct {
	rows  map[string]map[string]any // olake id -> column -> value
	bytes int64
}

func newReadBuffer() *readBuffer {
	return &readBuffer{rows: make(map[string]map[string]any)}
}

// value is the value read for the row's column.
func (b *readBuffer) value(olakeID, column string) (any, bool) {
	value, found := b.rows[olakeID][column]
	return value, found
}

// put keeps a value read for a row. A value can come twice when two source columns share one destination column
// (Foo and foo), so it replaces the old one.
func (b *readBuffer) put(olakeID, column string, value any) {
	values := b.rows[olakeID]
	if values == nil {
		values = make(map[string]any)
		b.rows[olakeID] = values
	}
	b.bytes += valueSize(value) - valueSize(values[column])
	values[column] = value
}

// hasAll reports whether a value has arrived for every one of the row's columns.
func (b *readBuffer) hasAll(olakeID string, columns []string) bool {
	for _, column := range columns {
		if _, found := b.rows[olakeID][column]; !found {
			return false
		}
	}
	return true
}

// release drops what was read for a row once fill has moved it into the row's record.
func (b *readBuffer) release(olakeID string) {
	b.bytes -= dataSize(b.rows[olakeID])
	delete(b.rows, olakeID)
}

func dataSize(data map[string]any) int64 {
	size := int64(0)
	for _, value := range data {
		size += valueSize(value)
	}
	return size
}

// valueSize is roughly the bytes a value holds: text by its length, numbers, booleans and times as 16. NULL and
// a missing value count as 0, which remember relies on.
func valueSize(value any) int64 {
	switch typed := value.(type) {
	case nil:
		return 0
	case string:
		return int64(len(typed))
	case json.RawMessage:
		return int64(len(typed))
	case []any: // Postgres arrays
		size := int64(0)
		for _, element := range typed {
			size += valueSize(element)
		}
		return size
	default:
		return 16
	}
}
