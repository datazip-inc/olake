package internal

import (
	"fmt"

	"github.com/datazip-inc/olake/types"
)

// RowIndexBaseSnapshotID returns the snapshot the row index is checkpointed at,
// for the Java writer to refuse a commit when the table tip has moved. Nil index
// yields a nil id (check skipped). A missing checkpoint is treated as snapshot 0.
func RowIndexBaseSnapshotID(index types.TableIndex) (*int64, error) {
	if index == nil {
		return nil, nil
	}
	snapshotID, _, err := index.LastCommittedSnapshot()
	if err != nil {
		return nil, fmt.Errorf("failed to read row index checkpoint before commit: %s", err)
	}
	return &snapshotID, nil
}
