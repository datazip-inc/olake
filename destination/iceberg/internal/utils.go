package internal

import (
	"context"
)

type ServerClient interface {
	SendClientRequest(ctx context.Context, reqPayload interface{}) (interface{}, error)
	ServerID() string
}

// PartitionInfo represents an Iceberg partition column with its transform, preserving order.
// Field is the original source column name (used for record.Data lookups before pre-shaping).
// SchemaField is the reformatted destination column name (used for schema, Java partition spec,
// and record.Data lookups after pre-shaping). Computed once at parse time via utils.Reformat.
type PartitionInfo struct {
	Field       string // original case — matches source record.Data keys
	SchemaField string // reformatted — matches Iceberg schema field names
	Transform   string
}
