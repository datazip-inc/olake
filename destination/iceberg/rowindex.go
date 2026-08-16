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

var errFullScanRequired = errors.New("row index requires a full scan")

// reconcileRowIndex makes the stream's row index agree with the destination table before the first record is written
func (i *Iceberg) reconcileRowIndex(ctx context.Context, index types.TableIndex, tableSnapshotID int64, hasEqualityDeletes bool) error {
	if index == nil {
		// no row index to reconcile
		return nil
	}

	table := fmt.Sprintf("%s.%s", i.stream.GetDestinationDatabase(&i.config.IcebergDatabase), i.stream.GetDestinationTable())

	if hasEqualityDeletes {
		// first check for equality deletes and migrate them to positional deletes
		migrated, err := i.server.rowIndexClient.MigrateEqualityDeletes(ctx, &proto.MigrateEqualityDeletesRequest{
			ThreadId: i.options.ThreadID,
			// Rewrite straight into the mode this sync writes, so a table switching
			// from equality deletes lands on its target representation in one commit.
			TargetMode: string(i.options.DeleteMode),
		})
		if err != nil {
			return fmt.Errorf("failed to migrate equality deletes of table[%s]: %s", table, err)
		}

		logger.Infof("Table[%s]: rewrote %d equality delete file(s) as %d positional delete(s)",
			table, migrated.GetRewrittenDeleteFiles(), migrated.GetPositionalDeletesWritten())
		tableSnapshotID = migrated.GetSnapshotId()
	}

	// now check if the index is already up to date, if not then update incrementally
	indexedSnapshotID, indexed, err := index.LastCommittedSnapshot()
	if err != nil {
		return fmt.Errorf("failed to read row index checkpoint of table[%s]: %s", table, err)
	}

	switch {
	case indexed && indexedSnapshotID == tableSnapshotID:
		logger.Infof("Table[%s]: reusing row index at snapshot[%d]", table, tableSnapshotID)
		return nil
	case indexed:
		logger.Infof("Table[%s]: incrementally refreshing row index from snapshot[%d] to snapshot[%d]", table, indexedSnapshotID, tableSnapshotID)
		err := i.fillRowIndex(ctx, index, &indexedSnapshotID)
		if err == nil {
			return nil
		}

		// rebuild the index from scratch
		logger.Warnf("Table[%s]: row index cannot be refreshed incrementally, rebuilding it", table)
	default:
		logger.Infof("Table[%s]: building row index from snapshot[%d]", table, tableSnapshotID)
	}

	return i.fillRowIndex(ctx, index, nil)
}

// fillRowIndex streams row locations from the destination into index
// if fromSnapshotID is provided it try to update indexes in incremental manner
func (i *Iceberg) fillRowIndex(ctx context.Context, index types.TableIndex, fromSnapshotID *int64) error {
	stream, err := i.server.rowIndexClient.ScanRowIndex(ctx, &proto.RowIndexScanRequest{
		ThreadId:       i.options.ThreadID,
		FromSnapshotId: fromSnapshotID,
	})
	if err != nil {
		return fmt.Errorf("failed to start row index scan: %s", err)
	}

	// if fromSnapshotID is not provided, we need to truncate the index before rebuilding it
	if fromSnapshotID == nil {
		if err := index.Truncate(); err != nil {
			return fmt.Errorf("failed to empty row index before rebuild: %s", err)
		}
	}

	snapshotID, entries, err := drainRowIndexScan(stream, index)
	if err != nil {
		return err
	}

	logger.Infof("Indexed %d row(s) up to snapshot[%d]", entries, snapshotID)
	return nil
}

func drainRowIndexScan(stream proto.RowIndexService_ScanRowIndexClient, index types.TableIndex) (snapshotID, entries int64, err error) {
	pending := types.NewRowIndexBatch(index)

	for {
		batch, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			// The checkpoint rides with the final batch, applied in full only
			// after every entry the scan produced has been accumulated.
			if err := index.Commit(pending, &snapshotID); err != nil {
				return 0, 0, fmt.Errorf("failed to checkpoint row index at snapshot[%d]: %s", snapshotID, err)
			}
			return snapshotID, entries, nil
		}

		if err != nil {
			return 0, 0, fmt.Errorf("row index scan failed: %s", err)
		}

		if batch.GetRequiresFullScan() {
			return 0, 0, errFullScanRequired
		}

		snapshotID = batch.GetSnapshotId()
		for _, entry := range batch.GetEntries() {
			olakeID := entry.GetOlakeId()

			if entry.GetDeleted() {
				// if row come from deleted file we need to delete it from the index
				loc, found, err := pending.Lookup(olakeID)
				if err != nil {
					return 0, 0, fmt.Errorf("failed to lookup row[%s] for delete: %s", olakeID, err)
				}
				if found && loc.FilePath == entry.GetFilePath() {
					pending.Delete(olakeID)
				}
			} else {
				pending.Put(olakeID, types.RowLocation{
					FilePath: entry.GetFilePath(),
					Position: entry.GetPosition(),
				})
			}
		}

		entries += int64(len(batch.GetEntries()))
	}
}
