package testutils

// DVUnpartTable and DVPartTable are the two source tables the deletion-vector suite drives.
// They are narrow and purpose-built (5 columns, not the ~50-column datatype-matrix table every
// other suite uses) because these tests need precise control over which DATA FILE a delete
// lands on, and over how many partitions exist - not datatype coverage.
//
// Both tables are always created, seeded and synced together: every scenario below runs one
// sync that advances both streams, then checks both. DVPartTable is not a special case with its
// own logic - it is exactly DVUnpartTable's schema, with a partition_regex set on its stream
// (see dvCatalogDoc). That is what makes it "the partitioned one."
const (
	DVUnpartTable = "dv_unpart"
	DVPartTable   = "dv_part"
)
