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
	// maxCachedBytes is the memory all threads' cached values share, split equally: all of it for a thread
	// alone, 128 MB / N with N, no minimum. Past its share a thread drops its oldest rows down to half the share,
	// read back from the destination if needed.
	maxCachedBytes = int64(128) * 1024 * 1024 // 128 MB
	// cachedRowOverhead and cachedValueOverhead approximate what a cached row costs besides its id and value
	// bytes: measured at about 400 bytes for a row of up to 8 values (Go 1.26).
	cachedRowOverhead   = int64(384)
	cachedValueOverhead = int64(24)
)

// cachingThreads counts the threads that cache values; each keeps to maxCachedBytes / this count, so N
// streams do not use N times the budget. An idle thread keeps its old share until its next batch trims it to the
// new one, so the total can pass maxCachedBytes for a while.
var cachingThreads atomic.Int64

// cachedValues holds, for each row, the values this thread last wrote for it, so the row's next change in this
// sync takes them from memory instead of reading the destination.
type cachedValues struct {
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

func newCachedValues() *cachedValues {
	return &cachedValues{rows: make(map[string]*cachedRow)}
}

// isEmpty reports whether no row is cached.
func (c *cachedValues) isEmpty() bool {
	return len(c.rows) == 0
}

// value is the row's cached value of column; a row deleted in this sync gives the placeholder.
func (c *cachedValues) value(olakeID, column string) (any, bool) {
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
func (c *cachedValues) remember(olakeID string, data map[string]any, columns map[string]struct{}) {
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
func (c *cachedValues) markDeleted(olakeID string) {
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
func (c *cachedValues) trim(share int64) int {
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

// tableValues holds the values read from the table (the placeholder for a row the table has no value for) until
// fillRecord moves them into their records. Unlike the cached values they are never trimmed, so no row is read twice;
// reads keep them within the maxBytes RecoverUnavailableValues is given, plus one row.
type tableValues struct {
	rows  map[string]map[string]any // olake id -> column -> value
	bytes int64
}

func newTableValues() *tableValues {
	return &tableValues{rows: make(map[string]map[string]any)}
}

// value is the value read for the row's column.
func (t *tableValues) value(olakeID, column string) (any, bool) {
	value, found := t.rows[olakeID][column]
	return value, found
}

// put keeps a value read for a row. A value can come twice when two source columns share one destination column
// (Foo and foo), so it replaces the old one.
func (t *tableValues) put(olakeID, column string, value any) {
	values := t.rows[olakeID]
	if values == nil {
		values = make(map[string]any)
		t.rows[olakeID] = values
	}
	t.bytes += valueSize(value) - valueSize(values[column])
	values[column] = value
}

// hasAll reports whether a value has arrived for every one of the row's columns.
func (t *tableValues) hasAll(olakeID string, columns []string) bool {
	for _, column := range columns {
		if _, found := t.rows[olakeID][column]; !found {
			return false
		}
	}
	return true
}

// release drops what was read for a row once fillRecord has moved it into the row's record.
func (t *tableValues) release(olakeID string) {
	t.bytes -= dataSize(t.rows[olakeID])
	delete(t.rows, olakeID)
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
