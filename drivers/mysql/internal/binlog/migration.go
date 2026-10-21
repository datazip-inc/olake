package binlog

import (
	"fmt"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils/errs"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

type gtidRebuild struct {
	gtidSet     mysql.GTIDSet
	transaction gtidTransaction
}

func (p *gtidRebuild) consume(event *replication.BinlogEvent) error {
	if previous, ok := event.Event.(*replication.PreviousGTIDsEvent); ok && p.gtidSet == nil {
		var err error
		p.gtidSet, err = parseGTIDSet(previous.GTIDSets)
		return err
	}
	if event.Header.EventType != replication.GTID_EVENT && !p.transaction.active {
		return nil
	}
	if p.gtidSet == nil {
		return fmt.Errorf("missing PreviousGTIDs event while reconstructing the migration checkpoint")
	}
	completed, err := p.transaction.consume(event)
	if err != nil {
		return err
	}
	if completed != nil {
		if err := p.gtidSet.Update(completed.String()); err != nil {
			return fmt.Errorf("failed to reconstruct migration GTIDs: %w", err)
		}
	}
	return nil
}

func (c *Connection) finishMigration(progress gtidRebuild, recovering bool) error {
	if progress.gtidSet == nil || progress.transaction.active || !progress.gtidSet.Contain(c.targetGTIDSet) ||
		(recovering && !progress.gtidSet.Equal(c.targetGTIDSet)) {
		return errs.Precondition(errs.StateInvalid, "mysql.gtid_migration_invalid",
			fmt.Errorf("%w: binlog transactions do not match the GTID migration boundary", constants.ErrNonRetryable))
	}
	// SHOW status reads GTIDs and position separately; the consumed binlog proves their mapping.
	c.gtidSet = progress.gtidSet.Clone()
	return nil
}

func (c *Connection) prepareMigration(target Binlog) error {
	if target.ServerUUID == "" || target.Position.Name == "" || target.Position.Pos < 4 ||
		c.CurrentPos.Name == "" || c.CurrentPos.Pos < 4 || c.CurrentPos.Compare(target.Position) > 0 {
		return errs.Precondition(errs.StateInvalid, "mysql.gtid_migration_invalid",
			fmt.Errorf("%w: invalid file range for GTID migration", constants.ErrNonRetryable))
	}
	if err := c.Checkpoint().ValidateServerUUID(target.ServerUUID); err != nil {
		return err
	}
	set, err := parseGTIDSet(*target.GTIDSet)
	if err != nil {
		return err
	}
	if c.ServerUUID == "" {
		logger.Warnf("Migrating a MySQL checkpoint without recorded server identity; this requires the original source server and its binlogs")
	}
	c.ServerUUID = target.ServerUUID
	c.targetGTIDSet = set
	return nil
}
