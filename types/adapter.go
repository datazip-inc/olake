package types

type AdapterType string

const (
	Parquet   AdapterType = "PARQUET"
	S3Iceberg AdapterType = "S3_ICEBERG"
	Iceberg   AdapterType = "ICEBERG"
)

// TODO: Add validations
type WriterConfig struct {
	Type         AdapterType `json:"type"`
	WriterConfig any         `json:"writer"`
}
