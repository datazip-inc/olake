package writers

import "runtime"

// DefaultPartitionConcurrency bounds how many partitions a PartitionedRollingWriter
// writes or flushes at once when Config.Concurrency is unset. GOMAXPROCS is a sensible
// ceiling for CPU- and memory-bound parquet encoding (each in-flight partition
// buffers roughly one row group).
var DefaultPartitionConcurrency = runtime.GOMAXPROCS(0)
