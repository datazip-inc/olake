package types

type AdapterType string

const (
	Local     MessageType = "LOCAL"
	S3        MessageType = "S3"
	S3Iceberg MessageType = "S3_ICEBERG"
)

// TODO: Add validations
type WriterConfig struct {
	Type          AdapterType `json:"type"`
	AdapterConfig any         `json:"adapter"`
}
