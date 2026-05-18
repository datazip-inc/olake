package arrowdst

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/datazip-inc/olake/types"
)

// OLakeSchema is the canonical destination-neutral schema representation:
// column name → OLake DataType.
type OLakeSchema map[string]types.DataType

// SchemaShape lets the adapter declare how the core should build the
// arrow.Schema from an OLakeSchema.
//
// Iceberg fills FieldIDs (PARQUET:field_id metadata) and IdentifierField
// (_olake_id non-null). Parquet-arrow leaves both empty.
type SchemaShape struct {
	FieldIDs        map[string]int32 // empty -> no PARQUET:field_id metadata
	IdentifierField string           // "" -> all fields nullable
}

// PartitionPreShape declares which source→destination key copies the core
// should perform for normalization=false batches BEFORE handing them to the
// adapter.
type PartitionPreShape struct {
	SourceField string // matches record.Data keys BEFORE pre-shape
	DestField   string // utils.Reformat(SourceField); matches arrow schema AFTER pre-shape
}

// Setup is returned by DestinationAdapter.Setup. The abstraction stores the
// schema + shape + state per stream so subsequent threads reuse them.
type Setup struct {
	Schema          OLakeSchema
	Shape           SchemaShape
	State           *types.MetadataState // Iceberg 2PC state; nil for parquet-arrow
	PartitionFields []PartitionPreShape  // empty when adapter needs no pre-shape
}

// DestinationAdapter is the 4-method interface every arrow destination
// implements.
type DestinationAdapter interface {
	// Setup is called once per writer thread. The adapter validates the
	// partition spec, starts servers/clients, fetches table metadata and
	// builds its initial schema, SchemaShape, and returns them.
	// upsertMode is true when the sync is neither a backfill nor append-only.
	Setup(ctx context.Context, stream types.StreamInterface, upsertMode bool,
		incoming OLakeSchema) (Setup, error)

	// WriteBatch receives already-flattened, filtered, normalization-shaped records.
	// The adapter handles partition routing, dedup, arrow.Record construction via
	// arrowdst.BuildArrowRecord, rolling parquet writes via arrowdst.RollingArrowWriter,
	// and accumulates commit metadata for Close.
	WriteBatch(ctx context.Context, records []types.RawRecord, arrowSchema *arrow.Schema) error

	// OnSchemaEvolved is invoked under the per-stream mutex when MergeSchemas reports
	// a real change. The adapter publishes the change and returns the SchemaShape for
	// the new schema so the core can rebuild the arrow.Schema for the next WriteBatch.
	OnSchemaEvolved(ctx context.Context, newSchema OLakeSchema) (SchemaShape, error)

	// Close is invoked unconditionally (even on ctx cancel). The core has already
	// flushed the pending buffer before Close runs.
	Close(ctx context.Context, finalMetadataState any) error
}

// AdapterInit is the factory function injected by the outer dispatcher into
// Pool so each call to NewWriter gets a fresh adapter instance.
type AdapterInit func() DestinationAdapter

// RegisteredAdapters is populated by each arrow-capable destination package's
// init() function. The key is the DestinationType string (e.g. "ICEBERG",
// "PARQUET"). destination/arrow_pool.go reads from this map.
var RegisteredAdapters = map[string]AdapterInit{}
