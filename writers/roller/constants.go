package roller

const (
	// DefaultMaxFileBytes is the encoded-size threshold that triggers a roll.
	// Matches Iceberg's target data-file size; delete-file rollers override it.
	DefaultMaxFileBytes int64 = 512 * 1024 * 1024 // 512 MB

	// DefaultSizeCheckInterval is how many rows are converted+encoded between size
	// checks. Mirrors Iceberg's parquet page row-count limit (WithBatchSize).
	DefaultSizeCheckInterval int = 10000
)
