package iceberg

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/datazip-inc/olake/destination/iceberg/proto"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
)

// reconcileRowIndex makes the stream's index agree with the destination table before the first record is written
func (i *Iceberg) reconcileRowIndex(ctx context.Context, index types.StreamIndex, tableSnapshotID int64, hasEqualityDeletes bool) error {
	if index == nil {
		// no stream index to reconcile
		return nil
	}

	table := fmt.Sprintf("%s.%s", i.stream.GetDestinationDatabase(&i.config.IcebergDatabase), i.stream.GetDestinationTable())

	if hasEqualityDeletes {
		// first check for equality deletes and migrate them to positional deletes
		migrated, err := i.server.tableIndexClient.MigrateEqualityDeletes(ctx, &proto.MigrateEqualityDeletesRequest{
			ThreadId: i.options.ThreadID,
		})
		if err != nil {
			return fmt.Errorf("failed to migrate equality deletes of table[%s]: %s", table, err)
		}

		logger.Infof("Table[%s]: rewrote %d equality delete file(s) as %d positional delete(s)",
			table, migrated.GetRewrittenDeleteFiles(), migrated.GetPositionalDeletesWritten())
	}

	indexedSnapshotID, err := index.LastCommittedSnapshot()
	if err != nil {
		return fmt.Errorf("failed to read stream index checkpoint of table[%s]: %s", table, err)
	}

	if indexedSnapshotID == tableSnapshotID && !hasEqualityDeletes {
		logger.Infof("Table[%s]: stream index is already up to date at snapshot[%d]", table, tableSnapshotID)
		return nil
	}

	var fromSnapshotID *int64
	if indexedSnapshotID != 0 {
		fromSnapshotID = &indexedSnapshotID
	}

	// reindex iceberg table
	return i.fillRowIndex(ctx, index, table, fromSnapshotID)
}

// fillRowIndex streams row locations from the destination into index
// if fromSnapshotID is provided it try to update indexes in incremental manner
func (i *Iceberg) fillRowIndex(ctx context.Context, index types.StreamIndex, table string, fromSnapshotID *int64) error {
	posIterator, err := i.server.tableIndexClient.ScanTableForIndexing(ctx, &proto.TableIndexScanRequest{
		ThreadId:       i.options.ThreadID,
		FromSnapshotId: fromSnapshotID,
	})
	if err != nil {
		return fmt.Errorf("failed to start stream index scan: %s", err)
	}

	// if fromSnapshotID is not provided, we need to truncate the index before rebuilding it
	if fromSnapshotID == nil {
		logger.Infof("Table[%s]: not found starting index snapshot id, truncating stream index before rebuild", table)
		if err := index.Truncate(); err != nil {
			return fmt.Errorf("failed to empty stream index before rebuild: %s", err)
		}
	} else {
		logger.Infof("Table[%s]: starting stream index scan from snapshot[%d]", table, *fromSnapshotID)
	}

	snapshotID, entries, err := drainTableIndexScan(posIterator, index)
	if err != nil {
		return fmt.Errorf("failed to drain table index scan of table[%s]: %s", table, err)
	}

	logger.Infof("Indexed %d row(s) up to snapshot[%d]", entries, snapshotID)
	return nil
}

func drainTableIndexScan(posIterator proto.TableIndexService_ScanTableForIndexingClient, index types.StreamIndex) (snapshotID, entries int64, err error) {
	pending := types.NewStreamIndexThread(index)

	for {
		batch, err := posIterator.Recv()
		if errors.Is(err, io.EOF) {
			// The checkpoint rides with the final batch, applied in full only
			// after every entry the scan produced has been accumulated.
			if err := index.Commit(pending, &snapshotID); err != nil {
				return 0, 0, fmt.Errorf("failed to checkpoint stream index at snapshot[%d]: %s", snapshotID, err)
			}
			return snapshotID, entries, nil
		}

		if err != nil {
			return 0, 0, fmt.Errorf("stream index scan failed: %s", err)
		}

		snapshotID = batch.GetSnapshotId()
		for _, entry := range batch.GetEntries() {
			pending.Put(entry.GetOlakeId(), types.RowLocation{
				FilePath: entry.GetFilePath(),
				Position: entry.GetPosition(),
			})
		}

		entries += int64(len(batch.GetEntries()))
	}
}
