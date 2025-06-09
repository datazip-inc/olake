package types

type AdapterType string

const (
	Parquet AdapterType = "PARQUET"
	Iceberg AdapterType = "ICEBERG"
)

// TODO: Add validations
type WriterConfig struct {
	Type         AdapterType `json:"type"`
	WriterConfig any         `json:"writer"`
}
