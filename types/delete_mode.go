package types

import "fmt"

// DeleteMode selects how a destination represents the removal of a row that a
// previous sync already committed.
type DeleteMode string

const (
	// DeleteModeEquality writes Iceberg equality delete files keyed on the table's
	// identifier field. Readers resolve them by matching key values, so no row
	// index is needed and this mode carries no bootstrap cost.
	DeleteModeEquality DeleteMode = "eq"
	// DeleteModePosition writes Iceberg positional delete files, which address a
	// row as (data file, ordinal). Producing them requires a durable
	// identifier -> RowLocation index of every live row in the table.
	DeleteModePosition DeleteMode = "pos"
	// DeleteModeDeletionVector writes Iceberg v3 deletion vectors: one Puffin
	// bitmap per data file instead of a positional delete file. Addresses rows
	// the same way DeleteModePosition does, so it needs the same durable index,
	// and it requires the destination table to be format version 3.
	DeleteModeDeletionVector DeleteMode = "dv"
)

// NeedsTableIndex reports whether the mode can only be served by maintaining a
// TableIndex alongside the destination table.
func (m DeleteMode) NeedsTableIndex(destinationType DestinationType) bool {
	return destinationType == Iceberg && (m == DeleteModePosition || m == DeleteModeDeletionVector)
}

func (m DeleteMode) Validate() error {
	switch m {
	case DeleteModeEquality, DeleteModePosition, DeleteModeDeletionVector:
		return nil
	default:
		return fmt.Errorf("invalid delete mode: %s", m)
	}
}
