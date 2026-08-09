package internal

import (
	"fmt"

	"github.com/datazip-inc/olake/types"
)

// RowIndexBaseSnapshotID returns the lastCommittedIndexSnapshot
func RowIndexBaseSnapshotID(index types.TableIndex) (*int64, error) {
	if index == nil {
		return nil, nil //nolint:nilnil
	}

	snapshotID, _, err := index.LastCommittedSnapshot()
	if err != nil {
		return nil, fmt.Errorf("failed to read row index checkpoint before commit: %s", err)
	}

	return &snapshotID, nil
}
