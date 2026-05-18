// Package arrowwriter implements arrowdst.DestinationAdapter for the
// arrow-native Iceberg write path.
//
// Layering:
//
//	destination/arrow                  — destination-neutral arrow library
//	                                     (BuildArrowRecord, RollingArrowWriter,
//	                                     schema mapping, FlattenAndDetect)
//	destination/arrow/iceberg          — THIS PACKAGE: arrow Iceberg adapter
//	                                     + its own Config (declared in config.go)
//	destination/legacy/iceberg         — legacy Iceberg destination + JAR build
package arrowwriter

import (
	"context"
	"fmt"

	arrowlib "github.com/apache/arrow-go/v18/arrow"
	arrowdst "github.com/datazip-inc/olake/destination/arrow"
	"github.com/datazip-inc/olake/types"
)

func init() {
	arrowdst.RegisteredAdapters[string(types.Iceberg)] = func() arrowdst.DestinationAdapter {
		return &Adapter{}
	}
}

// Adapter implements arrowdst.DestinationAdapter for Iceberg. It owns the
// per-flavor Config struct defined in config.go (same package).
type Adapter struct {
	cfg *Config
}

// GetConfigRef returns a pointer to the package-local Config so the arrow
// Pool can unmarshal the raw writer config into it.
func (a *Adapter) GetConfigRef() any {
	if a.cfg == nil {
		a.cfg = &Config{}
	}
	return a.cfg
}

// Setup validates config, fetches existing table metadata from the catalog,
// and returns the initial schema + shape + 2PC state.
//
// TODO: when the full arrow pipeline lands, this method will:
//  1. Validate non-normalized partition columns against the incoming schema.
//  2. Start the Java gRPC server via the shared iceberg.NewServer.
//  3. GET_OR_CREATE_TABLE → parse the returned schema into arrowdst.OLakeSchema.
//  4. Capture the olake_2pc_state metadata.
//  5. Fetch JSONSCHEMA for data + (if upsertMode) eq-delete file schemas.
//  6. Build the PartitionFields list from partitionInfo.
//
// All of the above already exists inside destination/legacy/iceberg; this adapter
// will call into it once the wiring is complete.
func (a *Adapter) Setup(_ context.Context, stream types.StreamInterface, _ bool,
	incoming arrowdst.OLakeSchema,
) (arrowdst.Setup, error) {
	if a.cfg == nil {
		return arrowdst.Setup{}, fmt.Errorf("iceberg arrow adapter: config not injected before Setup")
	}
	if err := a.cfg.Validate(); err != nil {
		return arrowdst.Setup{}, fmt.Errorf("iceberg arrow adapter: config validation: %s", err)
	}

	// Bootstrap from the stream schema until the full Java+gRPC wiring is in place.
	schema := incoming
	if schema == nil {
		schema = make(arrowdst.OLakeSchema)
		for _, col := range stream.Schema().ColumnNames() {
			if dt, err := stream.Schema().GetType(col); err == nil {
				schema[col] = dt
			}
		}
	}

	identifierField := "_olake_id"
	if a.cfg.NoIdentifierFields {
		identifierField = ""
	}

	return arrowdst.Setup{
		Schema: schema,
		Shape: arrowdst.SchemaShape{
			IdentifierField: identifierField,
		},
		State: nil,
	}, nil
}

// WriteBatch stages records as Iceberg parquet files via the Java gRPC server.
//
// TODO: full implementation — partition routing via the existing transforms,
// dedup n-1 logic, equality + positional delete writers, gRPC FILEPATH +
// UPLOAD_FILE, arrowdst.RollingArrowWriter per partition. The bulk of this
// already exists in writer.go; this adapter will delegate to it once the
// shared types are aligned.
func (a *Adapter) WriteBatch(_ context.Context, _ []types.RawRecord, _ *arrowlib.Schema) error {
	return fmt.Errorf("iceberg arrow adapter: WriteBatch not yet implemented — use the legacy Iceberg Writer with arrow_writes:true for now")
}

// OnSchemaEvolved publishes the schema change to the Iceberg catalog and
// returns the SchemaShape for the new schema.
//
// TODO: gRPC EVOLVE_SCHEMA + REFRESH_TABLE_SCHEMA + re-fetch field IDs.
func (a *Adapter) OnSchemaEvolved(_ context.Context, _ arrowdst.OLakeSchema) (arrowdst.SchemaShape, error) {
	identifierField := "_olake_id"
	if a.cfg != nil && a.cfg.NoIdentifierFields {
		identifierField = ""
	}
	return arrowdst.SchemaShape{IdentifierField: identifierField}, nil
}

// Close commits all staged Parquet files to the Iceberg catalog via gRPC
// REGISTER_AND_COMMIT and releases the Java server.
//
// TODO: build ordered file list (eq-delete → data → pos-delete per partition)
// and send gRPC REGISTER_AND_COMMIT with the finalMetadataState payload.
func (a *Adapter) Close(_ context.Context, _ any) error {
	return nil
}
