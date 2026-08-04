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
		// index holds is suspect and the whole table has to be read again. The
		// rebuild scan below empties the index before it starts.
		logger.Warnf("Table[%s]: row index cannot be refreshed incrementally, rebuilding it", table)
	default:
		logger.Infof("Table[%s]: building row index from snapshot[%d]", table, tableSnapshotID)
	}

	return i.fillRowIndex(ctx, index, nil)
}

// rowIndexScanChunk bounds how many scanned entries are held in memory before
// being handed to the index. A scan is replayable from the checkpoint, so
// applying it in pieces is safe and keeps a table of any size from having to be
// buffered whole.
const rowIndexScanChunk = 50_000

// fillRowIndex streams row locations from the destination into index. A nil
// fromSnapshotID reads every live row; otherwise only the rows added after that
// snapshot are read.
//
// The checkpoint is written only once the scan has been consumed in full, so a
// scan that dies partway leaves the index behind rather than wrong: the next
// sync sees the older checkpoint and rescans from it, re-deriving exactly the
// entries that had already landed.
func (i *Iceberg) fillRowIndex(ctx context.Context, index types.TableIndex, fromSnapshotID *int64) error {
	stream, err := i.server.rowIndexClient.ScanRowIndex(ctx, &proto.RowIndexScanRequest{
		ThreadId:       i.options.ThreadID,
		FromSnapshotId: fromSnapshotID,
	})
	if err != nil {
		return fmt.Errorf("failed to start row index scan: %s", err)
	}

	// A scan from nothing replaces the index rather than amending it. Emptying it
	// up front also clears the checkpoint, which is what makes an interrupted
	// rebuild safe: an index with no checkpoint is rebuilt again, never trusted.
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
			// The checkpoint rides with the final chunk, so it is reached only
			// after every entry the scan produced has been applied.
			if err := index.Apply(pending, &snapshotID); err != nil {
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
				// Only remove from index if the row is still in the deleted file.
				// If it moved to another file (e.g. updated after this file was written),
				// a newer position was already put, or will be put later in this scan.
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

		if pending.Len() < rowIndexScanChunk {
			continue
		}
		if err := index.Apply(pending, nil); err != nil {
			return 0, 0, fmt.Errorf("failed to apply row index scan chunk: %s", err)
		}
		pending.Reset()
	}
}
