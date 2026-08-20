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
	// DeleteModeDeletionVector writes Iceberg v3 deletion vectors.
	// TODO: implement dv writing in Olake (Difficulty: Medium)
	DeleteModeDeletionVector DeleteMode = "dv"
)

// NeedsTableIndex reports whether the mode can only be served by maintaining a
// TableIndex alongside the destination table.
func (m DeleteMode) NeedsTableIndex(destinationType DestinationType) bool {
	return destinationType == Iceberg && m == DeleteModePosition
}

func (m DeleteMode) Validate() error {
	if m == DeleteModeEquality || m == DeleteModePosition {
		return nil
	}

	return fmt.Errorf("invalid delete mode: %s", m)
}
