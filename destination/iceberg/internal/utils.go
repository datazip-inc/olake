package internal

import (
	"context"
)

type ServerClient interface {
	SendClientRequest(ctx context.Context, reqPayload interface{}) (interface{}, error)
	ServerID() string
}

// PartitionInfo represents a Iceberg partition column with its transform, preserving order
type PartitionInfo struct {
	Field     string
	Transform string
}
