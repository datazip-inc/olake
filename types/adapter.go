package types

type DestinationType string

const (
	Parquet DestinationType = "PARQUET"
	Iceberg DestinationType = "ICEBERG"
)

// UpsertSupported reports whether the destination can write upserts. An unknown (empty)
// destination is treated as supporting them, so callers keep checking the upsert fields.
func UpsertSupported(destinationType DestinationType) bool {
	return destinationType != Parquet
}

// TODO: Add validations
type WriterConfig struct {
	Type         DestinationType `json:"type"`
	WriterConfig any             `json:"writer"`
}
