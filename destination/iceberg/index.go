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

// indexScanFlushRows is how many scanned rows are buffered in memory before being
// written to the index. Bounds the memory a full table rebuild can take.
const indexScanFlushRows = 200_000

// reconcileTableIndex makes the stream's index agree with the destination table before the first record is written
func (i *Iceberg) reconcileTableIndex(ctx context.Context, index types.StreamIndex, tableSnapshotID int64, hasEqualityDeletes bool) error {
	if index == nil {
		// no stream index to reconcile
		return nil
	}

	table := fmt.Sprintf("%s.%s", i.stream.GetDestinationDatabase(&i.config.IcebergDatabase), i.stream.GetDestinationTable())

	if hasEqualityDeletes {
		// Rewrite straight into whatever representation this stream is configured for,
		// so a table switching off equality deletes lands on its target encoding in one
		// commit instead of eq -> pos -> dv.
		migrated, err := i.server.tableIndexClient.MigrateEqualityDeletes(ctx, &proto.MigrateEqualityDeletesRequest{
			ThreadId:   i.options.ThreadID,
			TargetMode: protoDeleteMode(i.stream.GetDeleteMode()),
		})
		if err != nil {
			return fmt.Errorf("failed to migrate equality deletes of table[%s]: %s", table, err)
		}

		logger.Infof("Table[%s]: rewrote %d equality delete file(s) as %d %s delete(s)",
			table, migrated.GetRewrittenDeleteFiles(), migrated.GetPositionalDeletesWritten(), i.stream.GetDeleteMode())
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
	return i.fillTableIndex(ctx, index, table, fromSnapshotID)
}

// fillTableIndex streams table locations from the destination into index
// if fromSnapshotID is provided it try to update indexes in incremental manner
func (i *Iceberg) fillTableIndex(ctx context.Context, index types.StreamIndex, table string, fromSnapshotID *int64) error {
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
	buffered := 0

	// flush writes the rows accumulated so far without advancing the checkpoint, so a
	// full table scan never has to hold every row of the table in memory at once.
	// Rows are safe to publish early because they are only reachable once the
	// checkpoint moves: a crash mid-scan leaves the old checkpoint in place and the
	// next sync rebuilds (truncating) or resumes from it.
	flush := func() error {
		if buffered == 0 {
			return nil
		}
		if err := index.Commit(pending, nil); err != nil {
			return fmt.Errorf("failed to flush %d stream index row(s): %s", buffered, err)
		}
		pending = types.NewStreamIndexThread(index)
		buffered = 0
		return nil
	}

	for {
		batch, err := posIterator.Recv()
		if errors.Is(err, io.EOF) {
			// The checkpoint rides with the final flush, written only after every
			// entry the scan produced is durable in the index.
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
		buffered += len(batch.GetEntries())

		if buffered >= indexScanFlushRows {
			if err := flush(); err != nil {
				return 0, 0, fmt.Errorf("failed to flush stream index: %s", err)
			}
		}
	}
}
