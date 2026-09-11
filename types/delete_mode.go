package types

import "fmt"

// UpdateType selects how a destination represents the removal of a row that a
// previous sync already committed.
type UpdateType string

const (
	// UpdateTypeEquality writes Iceberg equality delete files keyed on the table's
	// identifier field. Readers resolve them by matching key values, so no row
	// index is needed and this mode carries no bootstrap cost.
	UpdateTypeEquality UpdateType = "eq"
	// UpdateTypePosition writes Iceberg positional delete files, which address a
	// row as (data file, ordinal). Producing them requires a durable
	// identifier -> RowLocation index of every live row in the table.
	UpdateTypePosition UpdateType = "pos"
	// UpdateTypeDeletionVector writes Iceberg v3 deletion vectors: one Puffin
	// bitmap per data file instead of a positional delete file. Addresses rows
	// the same way UpdateTypePosition does, so it needs the same durable index,
	// and it requires the destination table to be format version 3.
	UpdateTypeDeletionVector UpdateType = "dv"
)

// NeedsTableIndex reports whether the mode can only be served by maintaining a
// TableIndex alongside the destination table.
func (m UpdateType) NeedsTableIndex(destinationType DestinationType) bool {
	return destinationType == Iceberg && (m == UpdateTypePosition || m == UpdateTypeDeletionVector)
}

func (m UpdateType) Validate() error {
	switch m {
	case UpdateTypeEquality, UpdateTypePosition, UpdateTypeDeletionVector:
		return nil
	default:
		return fmt.Errorf("invalid update mode: %s", m)
	}
}
