package binlog

import (
	"fmt"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils/errs"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

type migrationProgress struct {
	gtidSet     mysql.GTIDSet
	transaction gtidTransaction
}

func (p *migrationProgress) consume(event *replication.BinlogEvent) error {
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

func (c *Connection) finishMigration(progress migrationProgress, recovering bool) error {
	if progress.gtidSet == nil || progress.transaction.active || !progress.gtidSet.Contain(c.targetGTIDSet) ||
		(recovering && !progress.gtidSet.Equal(c.targetGTIDSet)) {
		return errs.Precondition(errs.StateInvalid, "mysql.gtid_migration_invalid",
			fmt.Errorf("%w: binlog transactions do not match the GTID migration boundary", constants.ErrNonRetryable))
	}
	// SHOW status reads GTIDs and position separately; the consumed binlog proves their mapping.
	c.gtidSet = progress.gtidSet.Clone()
	c.migration.GTIDSet = c.gtidSet.String()
	return nil
}

func (b Binlog) migrationState() (Binlog, error) {
	if b.GTIDSet == nil || b.Migration == nil || b.Migration.ServerUUID == "" ||
		b.Migration.Position.Name == "" || b.Migration.Position.Pos < 4 {
		return Binlog{}, errs.Precondition(errs.StateInvalid, "mysql.gtid_migration_invalid",
			fmt.Errorf("%w: missing or invalid file/GTID migration boundary", constants.ErrNonRetryable))
	}
	state := Binlog{Position: b.Migration.Position, ServerUUID: b.Migration.ServerUUID, GTIDSet: &b.Migration.GTIDSet}
	comparison, err := b.Compare(state)
	if err != nil {
		return Binlog{}, err
	}
	if comparison < 0 {
		return Binlog{}, errs.Precondition(errs.StateInvalid, "mysql.gtid_migration_invalid",
			fmt.Errorf("%w: checkpoint precedes its GTID migration boundary", constants.ErrNonRetryable))
	}
	return state, nil
}

func (b Binlog) compareFileCheckpoint(file Binlog) (int, error) {
	boundary, err := b.migrationState()
	if err != nil {
		return 0, err
	}
	if err := file.ValidateServerUUID(boundary.ServerUUID); err != nil {
		return 0, err
	}
	comparison := file.Position.Compare(boundary.Position)
	if comparison > 0 {
		return 0, errs.Precondition(errs.StateInvalid, "mysql.checkpoint_mode_mismatch",
			fmt.Errorf("%w: file checkpoint is past the recorded GTID migration boundary", constants.ErrNonRetryable))
	}
	if comparison < 0 {
		return 1, nil
	}
	return b.Compare(boundary)
}

func (c *Connection) prepareMigration(target Binlog) error {
	if target.Migration != nil {
		boundary, err := target.migrationState()
		if err != nil {
			return err
		}
		comparison, err := target.Compare(boundary)
		if err != nil {
			return err
		}
		if comparison != 0 {
			return errs.Precondition(errs.StateInvalid, "mysql.gtid_migration_invalid",
				fmt.Errorf("%w: legacy source state predates an already completed GTID migration", constants.ErrNonRetryable))
		}
		target = boundary
	}
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
	c.migration = &GTIDMigration{Position: target.Position, ServerUUID: target.ServerUUID, GTIDSet: set.String()}
	return nil
}
