package binlog

import (
	"fmt"
	"strings"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils/errs"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

func parseGTIDSet(value string) (mysql.GTIDSet, error) {
	set, err := mysql.ParseGTIDSet("mysql", value)
	if err != nil {
		return nil, errs.Precondition(errs.StateInvalid, "mysql.gtid_invalid",
			fmt.Errorf("%w: unsupported or invalid MySQL GTID set: %w", constants.ErrNonRetryable, err))
	}
	return set, nil
}

// Compare orders checkpoints by processed transactions, or by position in file mode.
func (b Binlog) Compare(other Binlog) (int, error) {
	if b.GTIDSet == nil && other.GTIDSet == nil {
		if other.ServerUUID != "" {
			if err := b.ValidateServerUUID(other.ServerUUID); err != nil {
				return 0, err
			}
		}
		return b.Position.Compare(other.Position), nil
	}
	if b.GTIDSet == nil || other.GTIDSet == nil {
		if b.GTIDSet != nil {
			return b.compareFileCheckpoint(other)
		}
		comparison, err := other.compareFileCheckpoint(b)
		return -comparison, err
	}
	set, err := parseGTIDSet(*b.GTIDSet)
	if err != nil {
		return 0, err
	}
	otherSet, err := parseGTIDSet(*other.GTIDSet)
	if err != nil {
		return 0, err
	}
	switch {
	case set.Equal(otherSet):
		return 0, nil
	case set.Contain(otherSet):
		return 1, nil
	case otherSet.Contain(set):
		return -1, nil
	default:
		return 0, errs.Precondition(errs.StateInvalid, "mysql.gtid_incomparable",
			fmt.Errorf("%w: MySQL checkpoints contain different transaction histories", constants.ErrNonRetryable))
	}
}

func validateGTIDHistory(saved, target mysql.GTIDSet, mode, executed, purged string) error {
	if mode != "ON" {
		return errs.Precondition(errs.CDCPositionLost, "mysql.gtid_disabled",
			fmt.Errorf("%w: a GTID checkpoint requires gtid_mode=ON on the source", constants.ErrNonRetryable))
	}
	executedSet, err := parseGTIDSet(executed)
	if err != nil {
		return err
	}
	purgedSet, err := parseGTIDSet(purged)
	if err != nil {
		return err
	}
	if !executedSet.Contain(saved) || !executedSet.Contain(target) {
		return errs.Precondition(errs.CDCPositionLost, "mysql.gtid_history_missing",
			fmt.Errorf("%w: the source has not executed the saved checkpoint or sync target; reconnect to a source with the required transaction history", constants.ErrNonRetryable))
	}
	if !saved.Contain(purgedSet) {
		return errs.Precondition(errs.CDCPositionLost, "mysql.gtid_purged",
			fmt.Errorf("%w: transactions needed after the saved GTID checkpoint have been purged; restore the required binlogs or resync", constants.ErrNonRetryable))
	}
	return nil
}

// gtidTransaction tracks the consumer's boundary, independently of the syncer's read-ahead.
type gtidTransaction struct {
	active bool
	begun  bool
	gtid   mysql.GTIDSet
}

//nolint:nilnil // A nil set means the event has not completed a transaction.
func (t *gtidTransaction) consume(event *replication.BinlogEvent) (mysql.GTIDSet, error) {
	switch event.Header.EventType {
	case replication.ANONYMOUS_GTID_EVENT, replication.GTID_TAGGED_LOG_EVENT,
		replication.XA_PREPARE_LOG_EVENT, replication.TRANSACTION_PAYLOAD_EVENT:
		return nil, fmt.Errorf("%w: unsupported event %s in MySQL GTID sync", constants.ErrNonRetryable, event.Header.EventType)
	}
	switch e := event.Event.(type) {
	case *replication.GTIDEvent:
		if t.active {
			return nil, fmt.Errorf("received a GTID before the preceding transaction completed")
		}
		gtid, err := e.GTIDNext()
		if err != nil {
			return nil, fmt.Errorf("failed to read transaction GTID: %w", err)
		}
		t.gtid = gtid
		t.active = true
		return nil, nil
	case *replication.QueryEvent:
		query := strings.ToUpper(strings.TrimSpace(string(e.Query)))
		if strings.HasPrefix(query, "XA ") {
			return nil, fmt.Errorf("%w: XA transactions are not supported in MySQL GTID sync", constants.ErrNonRetryable)
		}
		if query == "BEGIN" {
			t.begun = true
			return nil, nil
		}
		if t.begun && query != "COMMIT" && query != "ROLLBACK" {
			return nil, nil
		}
	case *replication.XIDEvent:
	default:
		return nil, nil
	}
	if !t.active {
		return nil, fmt.Errorf("missing GTID at MySQL transaction boundary")
	}
	t.active, t.begun = false, false
	// GSet also includes transactions read outside our target on a reordered replica.
	return t.gtid, nil
}
