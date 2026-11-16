package types

import "slices"

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

// WriteMode is how a stream's changes land in the destination: as new rows (append), or
// replacing the row they change (upsert).
type WriteMode string

const (
	WriteModeAppend WriteMode = "append"
	WriteModeUpsert WriteMode = "upsert"
)

// AvailableWriteModes returns the write modes both the destination and the source allow;
// sourceSupported is nil when the source has no restriction.
func AvailableWriteModes(destinationType DestinationType, sourceSupported []WriteMode) []WriteMode {
	available := []WriteMode{WriteModeAppend}
	if UpsertSupported(destinationType) {
		available = append(available, WriteModeUpsert)
	}
	if sourceSupported != nil {
		available = slices.DeleteFunc(available, func(m WriteMode) bool { return !slices.Contains(sourceSupported, m) })
	}

	return available
}

// TODO: Add validations
type WriterConfig struct {
	Type         DestinationType `json:"type"`
	WriterConfig any             `json:"writer"`
}
