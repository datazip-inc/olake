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

// errFullScanRequired means the destination cannot describe the changes between
// the indexed snapshot and the current one, so the only safe repair is to
// discard the index and read the table from scratch.
var errFullScanRequired = errors.New("row index requires a full scan")

// reconcileRowIndex makes the stream's row index agree with the destination table
// before the first record is written.
//
// Positional deletes address rows by (file, ordinal), so a stale index would
// blank out the wrong rows. The index is therefore only trusted when its own
// checkpoint matches the table snapshot we are about to write on top of;
// otherwise it is brought forward, or rebuilt.
func (i *Iceberg) reconcileRowIndex(ctx context.Context, index types.TableIndex, tableSnapshotID int64, hasEqualityDeletes bool) error {
	table := fmt.Sprintf("%s.%s", i.stream.GetDestinationDatabase(&i.config.IcebergDatabase), i.stream.GetDestinationTable())

	// Equality and positional deletes cannot describe the same row without one
	// masking the other, so any inherited equality deletes are folded into
	// positional ones before this sync starts appending its own.
	if hasEqualityDeletes {
		migrated, err := i.server.rowIndexClient.MigrateEqualityDeletes(ctx, &proto.MigrateEqualityDeletesRequest{
			ThreadId: i.options.ThreadID,
		})
		if err != nil {
			return fmt.Errorf("failed to migrate equality deletes of table[%s]: %s", table, err)
		}

		logger.Infof("Table[%s]: rewrote %d equality delete file(s) as %d positional delete(s)",
			table, migrated.GetRewrittenDeleteFiles(), migrated.GetPositionalDeletesWritten())
		tableSnapshotID = migrated.GetSnapshotId()
	}

	indexedSnapshotID, indexed, err := index.IndexedSnapshot()
	if err != nil {
		return fmt.Errorf("failed to read row index checkpoint of table[%s]: %s", table, err)
	}

	switch {
	case indexed && indexedSnapshotID == tableSnapshotID:
		logger.Infof("Table[%s]: reusing row index at snapshot[%d]", table, tableSnapshotID)
		return nil
	case indexed:
		logger.Infof("Table[%s]: refreshing row index from snapshot[%d] to snapshot[%d]", table, indexedSnapshotID, tableSnapshotID)
		err := i.fillRowIndex(ctx, index, &indexedSnapshotID)
		if err == nil {
			return nil
		}
		if !errors.Is(err, errFullScanRequired) {
			return err
		}
		// Rows moved between files since the checkpoint, so every position the
		// index holds is suspect and the whole table has to be read again.
		logger.Warnf("Table[%s]: row index cannot be refreshed incrementally, rebuilding it", table)
		if err := index.Truncate(); err != nil {
			return fmt.Errorf("failed to clear row index of table[%s]: %s", table, err)
		}
	default:
		logger.Infof("Table[%s]: building row index from snapshot[%d]", table, tableSnapshotID)
	}

	return i.fillRowIndex(ctx, index, nil)
}

// fillRowIndex streams row locations from the destination into index. A nil
// fromSnapshotID reads every live row; otherwise only the rows added after that
// snapshot are read.
//
// The whole scan lands in one index transaction: a scan that dies halfway leaves
// no half-updated index behind, which is what lets the checkpoint be trusted.
func (i *Iceberg) fillRowIndex(ctx context.Context, index types.TableIndex, fromSnapshotID *int64) error {
	stream, err := i.server.rowIndexClient.ScanRowIndex(ctx, &proto.RowIndexScanRequest{
		ThreadId:       i.options.ThreadID,
		FromSnapshotId: fromSnapshotID,
	})
	if err != nil {
		return fmt.Errorf("failed to start row index scan: %s", err)
	}

	txn, err := index.NewTxn()
	if err != nil {
		return fmt.Errorf("failed to open row index transaction: %s", err)
	}

	snapshotID, entries, err := drainRowIndexScan(stream, txn)
	if err != nil {
		if rollbackErr := txn.Rollback(); rollbackErr != nil {
			return fmt.Errorf("%s (row index rollback also failed: %s)", err, rollbackErr)
		}
		return err
	}

	if err := txn.Commit(); err != nil {
		return fmt.Errorf("failed to commit row index scan: %s", err)
	}

	if err := index.SetIndexedSnapshot(snapshotID); err != nil {
		return fmt.Errorf("failed to checkpoint row index at snapshot[%d]: %s", snapshotID, err)
	}

	logger.Infof("Indexed %d row(s) up to snapshot[%d]", entries, snapshotID)
	return nil
}

func drainRowIndexScan(stream proto.RowIndexService_ScanRowIndexClient, txn types.IndexTxn) (snapshotID, entries int64, err error) {
	for {
		batch, err := stream.Recv()
		if errors.Is(err, io.EOF) {
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
				// Only remove from index if the row is still in the deleted file.
				// If it moved to another file (e.g. updated after this file was written),
				// a newer position was already put, or will be put later in this scan.
				loc, found, err := txn.Lookup(olakeID)
				if err != nil {
					return 0, 0, fmt.Errorf("failed to lookup row[%s] for delete: %s", olakeID, err)
				}
				if found && loc.FilePath == entry.GetFilePath() {
					if err := txn.Delete(olakeID); err != nil {
						return 0, 0, fmt.Errorf("failed to delete indexed row[%s]: %s", olakeID, err)
					}
				}
			} else {
				if err := txn.Put(olakeID, types.RowLocation{
					FilePath:  entry.GetFilePath(),
					Position:  entry.GetPosition(),
					SeqNumber: entry.GetSequenceNumber(),
				}); err != nil {
					return 0, 0, fmt.Errorf("failed to index row[%s]: %s", olakeID, err)
				}
			}
		}
		entries += int64(len(batch.GetEntries()))
	}
}
