package types

type DestinationType string

const (
	Parquet DestinationType = "PARQUET"
	Iceberg DestinationType = "ICEBERG"
)

// TODO: Add validations
type WriterConfig struct {
	Type         DestinationType `json:"type"`
	BatchSize    int             `json:"batch_size"`
	MaxThreads   int             `json:"max_threads"`
	WriterConfig any             `json:"writer"`
}
