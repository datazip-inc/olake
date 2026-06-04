package internal

import (
	"context"

	"github.com/datazip-inc/olake/destination/iceberg/proto"
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

// StreamMetaCtx carries the per-stream context that is shipped on every gRPC
// payload in the shared-JVM model. Previously these were JVM-global CLI args.
// Constructed once per Iceberg.Setup call and reused by Write / EvolveSchema /
// Close, so writers don't have to re-derive it per batch.
type StreamMetaCtx struct {
	ThreadID               string
	Namespace              string
	Upsert                 bool
	CreateIdentifierFields bool
	IdentifierField        string
	IcebergPartitionFields []*proto.IcebergPayload_PartitionField
	ArrowPartitionFields   []*proto.ArrowPayload_PartitionField // TODO: remove this
	DestTableName          string
}
