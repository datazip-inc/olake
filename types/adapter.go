package types

type AdapterType string

const (
	Parquet    AdapterType = "PARQUET"
	RawParquet AdapterType = "RAW_PARQUET"
	S3Iceberg  AdapterType = "S3_ICEBERG"
)

// TODO: Add validations
type WriterConfig struct {
	Type         AdapterType `json:"type"`
	WriterConfig any         `json:"writer"`
}
