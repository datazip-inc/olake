package driver

import (
	"math"

	"github.com/datazip-inc/olake/constants"
)

// postgresMinVersionPG14 is the server_version_num for PostgreSQL 14.0, the first with TID range scans.
const postgresMinVersionPG14 = 140000

// ctidSizer returns the heap pages one ctid chunk spans, given the bytes behind one page offset across the relations
// the chunk reads (heapPageBytes for the heap alone, tablePageBytes with TOAST) and the heap bytes a full scan reads.
type ctidSizer interface {
	pagesPerChunk(heapPageBytes, tablePageBytes float64, scannedHeapBytes int64) float64
}

// newCTIDSizer picks the sizer for the server. PG 14+ reads only a chunk's own pages (TID range scan), while
// older versions read the whole heap for every chunk.
func newCTIDSizer(version int) ctidSizer {
	switch {
	case version >= postgresMinVersionPG14:
		return tidRangeSizer{}
	default:
		return seqScanSizer{}
	}
}

// tidRangeSizer gives every chunk EffectiveParquetSize of table data.
type tidRangeSizer struct{}

func (tidRangeSizer) pagesPerChunk(_, tablePageBytes float64, _ int64) float64 {
	return math.Ceil(float64(constants.EffectiveParquetSize) / tablePageBytes)
}

// seqScanSizer gives every chunk at least the heap it scans, so the repeated scans add at most one pass over
// the table, and never more pages than a heap-only chunk, so tables without TOAST keep their old chunks.
type seqScanSizer struct{}

func (seqScanSizer) pagesPerChunk(heapPageBytes, tablePageBytes float64, scannedHeapBytes int64) float64 {
	heapOnly := float64(constants.EffectiveParquetSize) / heapPageBytes
	capped := float64(max(constants.EffectiveParquetSize, scannedHeapBytes)) / tablePageBytes
	return math.Ceil(min(heapOnly, capped))
}
