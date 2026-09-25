package driver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/binlog"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
)

func (m *MySQL) prerequisiteChecks() []abstract.Prerequisite {
	return []abstract.Prerequisite{
		{
			Name: "binlog_access", Required: true, Recommended: "REPLICATION CLIENT, REPLICATION SLAVE",
			Description: "OLake cannot read the binary log, so it cannot capture any changes.",
			Check:       m.checkBinlogAccess,
		},
		{
			Name: "log_bin", Required: true, Recommended: "ON",
			Description: "Binary logging is off, so the server records no changes for OLake to read.",
			Check:       m.checkVariable(jdbc.MySQLLogBinQuery(), "ON"),
		},
		{
			Name: "binlog_format", Required: true, Recommended: "ROW",
			Description: "With STATEMENT or MIXED, the log stores SQL statements instead of row data, so changed rows can't be rebuilt.",
			Check:       m.checkVariable(jdbc.MySQLBinlogFormatQuery(), "ROW"),
		},
		{
			// At MINIMAL or NOBLOB the binlog carries only some columns per row, which cannot
			// be mapped back to a complete record.
			Name: "binlog_row_image", Required: true, Recommended: "FULL",
			Description: "With MINIMAL or NOBLOB, updates log only some columns, so synced rows would be incomplete.",
			Check:       m.checkVariable(jdbc.MySQLBinlogRowImageQuery(), "FULL"),
		},
		{
			// FULL puts column names, ENUM/SET members, charsets and signedness in the binlog
			// itself. Without it, that metadata is rebuilt from information_schema: one query per
			// table, and blind to a rename the reader has not reached yet.
			Name: "binlog_row_metadata", Recommended: "FULL",
			Description: "Recommended to set to FULL so the binlog carries column metadata. Otherwise OLake reads it from " +
				"information_schema, which can mis-map columns if a table is altered while a sync is catching up.",
			Check: m.checkBinlogRowMetadata,
		},
		{
			Name: "binlog_retention", Recommended: ">= " + utils.HumanDuration(constants.RecommendedCDCLogRetention),
			Description: "If a sync is paused longer than the retention window, the logs it needs are purged and a full resync is required.",
			Check:       m.checkBinlogRetention,
		},
	}
}

// checkVariable compares a SHOW GLOBAL VARIABLES result against the expected value.
func (m *MySQL) checkVariable(query, want string) func(context.Context) (string, bool, error) {
	return func(ctx context.Context) (string, bool, error) {
		var name, value string
		if err := m.client.QueryRowxContext(ctx, query).Scan(&name, &value); err != nil {
			return "", false, err
		}
		return strings.ToUpper(value), strings.EqualFold(value, want), nil
	}
}

// checkBinlogRowMetadata compares binlog_row_metadata with FULL. The variable is absent before
// MySQL 8.0.1 and on MariaDB; nothing can be changed there, so the check passes.
func (m *MySQL) checkBinlogRowMetadata(ctx context.Context) (string, bool, error) {
	current, ok, err := m.checkVariable(jdbc.MySQLBinlogRowMetadataQuery(), "FULL")(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return "not available on this server", true, nil
	}
	return current, ok, err
}

// checkBinlogAccess reuses the position read CDC starts with: SHOW MASTER STATUS requires
// REPLICATION CLIENT, so success proves the permission.
func (m *MySQL) checkBinlogAccess(ctx context.Context) (string, bool, error) {
	if _, err := binlog.GetCurrentBinlogPosition(ctx, m.client); err != nil {
		return fmt.Sprintf("cannot read binlog position: %s", err), false, nil
	}
	return "granted", true, nil
}

// checkBinlogRetention follows the server's precedence: the RDS/Aurora setting, then
// binlog_expire_logs_seconds, then expire_logs_days. All unset or zero means no auto-purge.
func (m *MySQL) checkBinlogRetention(ctx context.Context) (string, bool, error) {
	var name string
	var rdsHours sql.NullFloat64
	if err := m.client.QueryRowxContext(ctx, jdbc.MySQLRDSBinlogRetentionQuery()).Scan(&name, &rdsHours); err == nil {
		// NULL on RDS means "purge as soon as possible", which reads as zero hours here
		return retentionResult(time.Duration(rdsHours.Float64 * float64(time.Hour)))
	}

	for _, v := range []struct {
		query string
		unit  time.Duration
	}{
		{jdbc.MySQLBinlogExpireSecondsQuery(), time.Second},
		{jdbc.MySQLExpireLogsDaysQuery(), 24 * time.Hour},
	} {
		var value float64
		if err := m.client.QueryRowxContext(ctx, v.query).Scan(&name, &value); err == nil && value > 0 {
			return retentionResult(time.Duration(value * float64(v.unit)))
		}
	}
	return "no auto-purge", true, nil
}

func retentionResult(retention time.Duration) (string, bool, error) {
	return utils.HumanDuration(retention), retention >= constants.RecommendedCDCLogRetention, nil
}

// Prerequisites returns the CDC setup checks evaluated in Setup.
func (m *MySQL) Prerequisites() types.Prerequisites {
	return m.prerequisites
}
