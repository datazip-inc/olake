package types

import "fmt"

// UpdateMode iceberg update mode
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
	// UpdateTypeDeletionVector writes Iceberg v3 deletion vectors.
	// TODO: implement dv writing in Olake (Difficulty: Medium)
	UpdateTypeDeletionVector UpdateType = "dv"
)

// NeedsTableIndex reports whether the mode can only be served by maintaining a
// TableIndex alongside the destination table.
func (m UpdateType) NeedsTableIndex(destinationType DestinationType) bool {
	return destinationType == Iceberg && m == UpdateTypePosition
}

func (m UpdateType) Validate() error {
	if m == UpdateTypeEquality || m == UpdateTypePosition {
		return nil
	}

	return fmt.Errorf("invalid update mode: %s", m)
}
