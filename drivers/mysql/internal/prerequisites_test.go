package driver

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newMockMySQL returns a driver backed by sqlmock; queries must match the jdbc strings exactly.
func newMockMySQL(t *testing.T) (*MySQL, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, mock.ExpectationsWereMet())
		_ = db.Close()
	})
	return &MySQL{client: sqlx.NewDb(db, "sqlmock")}, mock
}

func variableRow(name, value string) *sqlmock.Rows {
	return sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow(name, value)
}

func TestMySQLPrerequisiteChecks(t *testing.T) {
	checks := (&MySQL{}).prerequisiteChecks()

	required := map[string]bool{}
	for _, c := range checks {
		required[c.Name] = c.Required
		assert.NotEmpty(t, c.Description, c.Name)
		assert.NotEmpty(t, c.Recommended, c.Name)
		assert.NotNil(t, c.Check, c.Name)
	}
	assert.Equal(t, map[string]bool{
		"binlog_access":       true,
		"log_bin":             true,
		"binlog_format":       true,
		"binlog_row_image":    true,
		"binlog_row_metadata": false,
		"binlog_retention":    false,
	}, required)
}

func TestMySQLCheckVariable(t *testing.T) {
	tests := []struct {
		name        string
		query       string
		want        string
		value       string
		wantCurrent string
		wantOK      bool
	}{
		{"log_bin on", jdbc.MySQLLogBinQuery(), "ON", "ON", "ON", true},
		{"log_bin off", jdbc.MySQLLogBinQuery(), "ON", "OFF", "OFF", false},
		{"binlog_format row, case-insensitive", jdbc.MySQLBinlogFormatQuery(), "ROW", "row", "ROW", true},
		{"binlog_format mixed", jdbc.MySQLBinlogFormatQuery(), "ROW", "MIXED", "MIXED", false},
		{"binlog_row_image full", jdbc.MySQLBinlogRowImageQuery(), "FULL", "FULL", "FULL", true},
		{"binlog_row_image minimal", jdbc.MySQLBinlogRowImageQuery(), "FULL", "MINIMAL", "MINIMAL", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m, mock := newMockMySQL(t)
			mock.ExpectQuery(tc.query).WillReturnRows(variableRow("v", tc.value))

			current, ok, err := m.checkVariable(tc.query, tc.want)(context.Background())
			require.NoError(t, err)
			assert.Equal(t, tc.wantCurrent, current)
			assert.Equal(t, tc.wantOK, ok)
		})
	}

	t.Run("query error is returned", func(t *testing.T) {
		m, mock := newMockMySQL(t)
		mock.ExpectQuery(jdbc.MySQLLogBinQuery()).WillReturnError(errors.New("connection reset"))

		_, ok, err := m.checkVariable(jdbc.MySQLLogBinQuery(), "ON")(context.Background())
		require.Error(t, err)
		assert.False(t, ok)
	})
}

func TestMySQLCheckBinlogRowMetadata(t *testing.T) {
	tests := []struct {
		name        string
		expect      func(sqlmock.Sqlmock)
		wantCurrent string
		wantOK      bool
		wantErr     bool
	}{
		{
			name: "full",
			expect: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(jdbc.MySQLBinlogRowMetadataQuery()).WillReturnRows(variableRow("binlog_row_metadata", "FULL"))
			},
			wantCurrent: "FULL", wantOK: true,
		},
		{
			name: "minimal",
			expect: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(jdbc.MySQLBinlogRowMetadataQuery()).WillReturnRows(variableRow("binlog_row_metadata", "MINIMAL"))
			},
			wantCurrent: "MINIMAL", wantOK: false,
		},
		{
			// absent before MySQL 8.0.1 and on MariaDB: nothing to change, so it passes
			name: "variable missing",
			expect: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(jdbc.MySQLBinlogRowMetadataQuery()).WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}))
			},
			wantCurrent: "not available on this server", wantOK: true,
		},
		{
			name: "query error",
			expect: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(jdbc.MySQLBinlogRowMetadataQuery()).WillReturnError(errors.New("boom"))
			},
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m, mock := newMockMySQL(t)
			tc.expect(mock)

			current, ok, err := m.checkBinlogRowMetadata(context.Background())
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantCurrent, current)
			assert.Equal(t, tc.wantOK, ok)
		})
	}
}

func TestMySQLCheckBinlogAccess(t *testing.T) {
	statusColumns := []string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}

	tests := []struct {
		name            string
		version         string
		statusQuery     string
		statusErr       error
		wantOK          bool
		wantCurrentPart string
	}{
		{
			name: "mysql 8.0 granted", version: "8.0.36", statusQuery: jdbc.MySQLMasterStatusQuery(),
			wantOK: true, wantCurrentPart: "granted",
		},
		{
			// 8.4 renamed SHOW MASTER STATUS
			name: "mysql 8.4 granted", version: "8.4.2", statusQuery: jdbc.MySQLMasterStatusQueryNew(),
			wantOK: true, wantCurrentPart: "granted",
		},
		{
			name: "missing REPLICATION CLIENT", version: "8.0.36", statusQuery: jdbc.MySQLMasterStatusQuery(),
			statusErr: &mysql.MySQLError{Number: 1227, Message: "Access denied; you need (at least one of) the SUPER, REPLICATION CLIENT privilege(s)"},
			wantOK:    false, wantCurrentPart: "REPLICATION CLIENT",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m, mock := newMockMySQL(t)
			mock.ExpectQuery("SELECT @@version").WillReturnRows(sqlmock.NewRows([]string{"@@version"}).AddRow(tc.version))
			status := mock.ExpectQuery(tc.statusQuery)
			if tc.statusErr != nil {
				status.WillReturnError(tc.statusErr)
			} else {
				status.WillReturnRows(sqlmock.NewRows(statusColumns).AddRow("binlog.000003", 157, "", "", ""))
			}

			current, ok, err := m.checkBinlogAccess(context.Background())
			// a failed read is the check result, never an evaluation error
			require.NoError(t, err)
			assert.Equal(t, tc.wantOK, ok)
			assert.Contains(t, current, tc.wantCurrentPart)
		})
	}

	t.Run("binlog disabled returns no position row", func(t *testing.T) {
		m, mock := newMockMySQL(t)
		mock.ExpectQuery("SELECT @@version").WillReturnRows(sqlmock.NewRows([]string{"@@version"}).AddRow("8.0.36"))
		mock.ExpectQuery(jdbc.MySQLMasterStatusQuery()).WillReturnRows(sqlmock.NewRows(statusColumns))

		current, ok, err := m.checkBinlogAccess(context.Background())
		require.NoError(t, err)
		assert.False(t, ok)
		assert.Contains(t, current, "no binlog position available")
	})
}

func TestMySQLCheckBinlogRetention(t *testing.T) {
	rdsQuery := jdbc.MySQLRDSBinlogRetentionQuery()
	secondsQuery := jdbc.MySQLBinlogExpireSecondsQuery()
	daysQuery := jdbc.MySQLExpireLogsDaysQuery()
	notRDS := func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery(rdsQuery).WillReturnError(errors.New("Table 'mysql.rds_configuration' doesn't exist"))
	}
	noRows := sqlmock.NewRows([]string{"Variable_name", "Value"})

	tests := []struct {
		name        string
		expect      func(sqlmock.Sqlmock)
		wantCurrent string
		wantOK      bool
	}{
		{
			name: "rds 7 days",
			expect: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(rdsQuery).WillReturnRows(sqlmock.NewRows([]string{"name", "value"}).AddRow("binlog retention hours", "168"))
			},
			wantCurrent: "7 days", wantOK: true,
		},
		{
			name: "rds 24 hours",
			expect: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(rdsQuery).WillReturnRows(sqlmock.NewRows([]string{"name", "value"}).AddRow("binlog retention hours", "24"))
			},
			wantCurrent: "1 day", wantOK: false,
		},
		{
			// NULL on RDS means binlogs are purged as soon as possible
			name: "rds NULL",
			expect: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(rdsQuery).WillReturnRows(sqlmock.NewRows([]string{"name", "value"}).AddRow("binlog retention hours", nil))
			},
			wantCurrent: "0 hours", wantOK: false,
		},
		{
			name: "binlog_expire_logs_seconds takes precedence",
			expect: func(mock sqlmock.Sqlmock) {
				notRDS(mock)
				mock.ExpectQuery(secondsQuery).WillReturnRows(variableRow("binlog_expire_logs_seconds", "2592000"))
			},
			wantCurrent: "30 days", wantOK: true,
		},
		{
			name: "binlog_expire_logs_seconds too short",
			expect: func(mock sqlmock.Sqlmock) {
				notRDS(mock)
				mock.ExpectQuery(secondsQuery).WillReturnRows(variableRow("binlog_expire_logs_seconds", "86400"))
			},
			wantCurrent: "1 day", wantOK: false,
		},
		{
			name: "seconds zero falls back to expire_logs_days",
			expect: func(mock sqlmock.Sqlmock) {
				notRDS(mock)
				mock.ExpectQuery(secondsQuery).WillReturnRows(variableRow("binlog_expire_logs_seconds", "0"))
				mock.ExpectQuery(daysQuery).WillReturnRows(variableRow("expire_logs_days", "10"))
			},
			wantCurrent: "10 days", wantOK: true,
		},
		{
			// MariaDB and MySQL 5.7 have no binlog_expire_logs_seconds
			name: "seconds variable missing, expire_logs_days too short",
			expect: func(mock sqlmock.Sqlmock) {
				notRDS(mock)
				mock.ExpectQuery(secondsQuery).WillReturnRows(noRows)
				mock.ExpectQuery(daysQuery).WillReturnRows(variableRow("expire_logs_days", "3"))
			},
			wantCurrent: "3 days", wantOK: false,
		},
		{
			name: "both zero means no auto-purge",
			expect: func(mock sqlmock.Sqlmock) {
				notRDS(mock)
				mock.ExpectQuery(secondsQuery).WillReturnRows(variableRow("binlog_expire_logs_seconds", "0"))
				mock.ExpectQuery(daysQuery).WillReturnRows(variableRow("expire_logs_days", "0"))
			},
			wantCurrent: "no auto-purge", wantOK: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m, mock := newMockMySQL(t)
			tc.expect(mock)

			current, ok, err := m.checkBinlogRetention(context.Background())
			require.NoError(t, err)
			assert.Equal(t, tc.wantCurrent, current)
			assert.Equal(t, tc.wantOK, ok)
		})
	}
}
