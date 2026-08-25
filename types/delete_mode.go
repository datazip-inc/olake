package types

import "fmt"

// UpdateMode iceberg update mode
type UpdateMode string

const (
	// UpdateModeEquality writes Iceberg equality delete files keyed on the table's
	// identifier field. Readers resolve them by matching key values, so no row
	// index is needed and this mode carries no bootstrap cost.
	UpdateModeEquality UpdateMode = "eq"
	// UpdateModePosition writes Iceberg positional delete files, which address a
	// row as (data file, ordinal). Producing them requires a durable
	// identifier -> RowLocation index of every live row in the table.
	UpdateModePosition UpdateMode = "pos"
	// UpdateModeDeletionVector writes Iceberg v3 deletion vectors.
	// TODO: implement dv writing in Olake (Difficulty: Medium)
	UpdateModeDeletionVector UpdateMode = "dv"
)

// NeedsTableIndex reports whether the mode can only be served by maintaining a
// TableIndex alongside the destination table.
func (m UpdateMode) NeedsTableIndex(destinationType DestinationType) bool {
	return destinationType == Iceberg && m == UpdateModePosition
}

func (m UpdateMode) Validate() error {
	if m == UpdateModeEquality || m == UpdateModePosition {
		return nil
	}

	return fmt.Errorf("invalid update mode: %s", m)
}
