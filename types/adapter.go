package types

type DestinationType string

const (
	Parquet DestinationType = "PARQUET"
	Iceberg DestinationType = "ICEBERG"
)

// TODO: Add validations
type WriterConfig struct {
	Type         DestinationType `json:"type"`
	WriterConfig any             `json:"writer"`
}
