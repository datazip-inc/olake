package types

type AdapterType string

const (
	Local     AdapterType = "LOCAL"
	S3        AdapterType = "S3"
	S3Iceberg AdapterType = "S3_ICEBERG"
)

// TODO: Add validations
type WriterConfig struct {
	Type          AdapterType `json:"type"`
	AdapterConfig any         `json:"adapter"`
}
