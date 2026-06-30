package driver

import (
	"database/sql"
	"sync/atomic"

	"github.com/datazip-inc/olake/pkg/binlog"
)

// mysqlRowBytes returns the approximate InnoDB on-disk byte sum for a row
// scanned via database/sql. Uses the SQL type name for fixed-width types and
// the actual byte length of the Go value for variable-width types.
func mysqlRowBytes(vals []any, colTypes []*sql.ColumnType) int64 {
	var total int64
	for i, v := range vals {
		total += binlog.MysqlColumnBytes(v, colTypes[i].DatabaseTypeName())
	}
	return total
}

// makeLocalAddRowBytes returns a jdbc.MapScan / jdbc.MapScanConcurrent callback
// that accumulates row bytes into a caller-owned local int64. The local lives on
// the ChunkIterator / StreamIncrementalChanges call stack, so it resets to 0 on
// every retry (no double counting). The caller returns the final value, which
// the abstract driver commits to Stats.BytesCommitted via WriterThread.Close.
func makeLocalAddRowBytes(local *int64) func([]any, []*sql.ColumnType) {
	return func(vals []any, colTypes []*sql.ColumnType) {
		// atomic: MapScanConcurrent invokes this from the producer goroutine
		// while the consumer goroutine runs concurrently.
		atomic.AddInt64(local, mysqlRowBytes(vals, colTypes))
	}
}
