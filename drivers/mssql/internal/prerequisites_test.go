package driver

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/jmoiron/sqlx"
	mssql "github.com/microsoft/go-mssqldb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newMockClient returns an sqlx client backed by sqlmock; queries must match exactly.
func newMockClient(t *testing.T) (*sqlx.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, mock.ExpectationsWereMet())
		_ = db.Close()
	})
	return sqlx.NewDb(db, "sqlmock"), mock
}

func newMockMSSQL(t *testing.T) (*MSSQL, sqlmock.Sqlmock) {
	t.Helper()
	client, mock := newMockClient(t)
	return &MSSQL{client: client, config: &Config{}}, mock
}

func bitRow(value bool) *sqlmock.Rows {
	return sqlmock.NewRows([]string{"value"}).AddRow(value)
}

func TestMSSQLPrerequisiteChecks(t *testing.T) {
	names := func(m *MSSQL) map[string]bool {
		required := map[string]bool{}
		for _, c := range m.prerequisiteChecks() {
			required[c.Name] = c.Required
			assert.NotEmpty(t, c.Description, c.Name)
			assert.NotEmpty(t, c.Recommended, c.Name)
			assert.NotNil(t, c.Check, c.Name)
		}
		return required
	}

	// no CDC intent in the config yet, so nothing may block setup
	t.Run("primary", func(t *testing.T) {
		assert.Equal(t, map[string]bool{
			"database_cdc":        false,
			"cdc_capture_job":     false,
			"view_database_state": false,
		}, names(&MSSQL{config: &Config{}}))
	})

	t.Run("manage capture instances adds db_owner check", func(t *testing.T) {
		got := names(&MSSQL{config: &Config{ManageCaptureInstances: true}})
		assert.Contains(t, got, "capture_instance_admin")
		assert.Len(t, got, 4)
	})

	// msdb jobs live on the primary and resolveInitialLSN skips the state check on replicas
	t.Run("read replica skips msdb and state checks", func(t *testing.T) {
		assert.Equal(t, map[string]bool{
			"database_cdc": false,
		}, names(&MSSQL{config: &Config{}, isReadReplica: true}))
	})
}

func TestMSSQLCheckDatabaseCDC(t *testing.T) {
	tests := []struct {
		name        string
		enabled     bool
		queryErr    error
		wantCurrent string
		wantOK      bool
		wantErr     bool
	}{
		{name: "enabled", enabled: true, wantCurrent: "enabled", wantOK: true},
		{name: "disabled", enabled: false, wantCurrent: "disabled", wantOK: false},
		{name: "query error", queryErr: errors.New("boom"), wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m, mock := newMockMSSQL(t)
			q := mock.ExpectQuery(jdbc.MSSQLCDCSupportQuery())
			if tc.queryErr != nil {
				q.WillReturnError(tc.queryErr)
			} else {
				q.WillReturnRows(bitRow(tc.enabled))
			}

			current, ok, err := m.checkDatabaseCDC(context.Background())
			if tc.wantErr {
				require.Error(t, err)
				assert.False(t, ok)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantCurrent, current)
			assert.Equal(t, tc.wantOK, ok)
		})
	}
}

func TestMSSQLCheckCaptureJob(t *testing.T) {
	jobColumns := []string{"maxtrans", "pollinginterval"}

	tests := []struct {
		name        string
		rows        *sqlmock.Rows
		queryErr    error
		wantCurrent string
		wantOK      bool
		wantErr     bool
	}{
		{
			name:        "capture job present",
			rows:        sqlmock.NewRows(jobColumns).AddRow(500, 5),
			wantCurrent: "present", wantOK: true,
		},
		{
			name:        "no capture job",
			rows:        sqlmock.NewRows(jobColumns),
			wantCurrent: "no capture job", wantOK: false,
		},
		{
			name:        "select denied on cdc_jobs",
			queryErr:    mssql.Error{Number: errSelectDenied, Message: "The SELECT permission was denied on the object 'cdc_jobs'"},
			wantCurrent: "no SELECT on msdb.dbo.cdc_jobs", wantOK: false,
		},
		{
			name:        "no access to msdb",
			queryErr:    mssql.Error{Number: errDatabaseDenied, Message: "The server principal is not able to access the database \"msdb\""},
			wantCurrent: "no SELECT on msdb.dbo.cdc_jobs", wantOK: false,
		},
		{
			name:     "other server error",
			queryErr: mssql.Error{Number: 1205, Message: "deadlock victim"},
			wantErr:  true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m, mock := newMockMSSQL(t)
			q := mock.ExpectQuery(jdbc.MSSQLCDCCaptureJobConfigQuery())
			if tc.queryErr != nil {
				q.WillReturnError(tc.queryErr)
			} else {
				q.WillReturnRows(tc.rows)
			}

			current, ok, err := m.checkCaptureJob(context.Background())
			if tc.wantErr {
				require.Error(t, err)
				assert.False(t, ok)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantCurrent, current)
			assert.Equal(t, tc.wantOK, ok)
		})
	}
}

func TestMSSQLCheckViewDatabaseState(t *testing.T) {
	tests := []struct {
		name        string
		granted     bool
		queryErr    error
		wantCurrent string
		wantOK      bool
	}{
		{name: "granted", granted: true, wantCurrent: "granted", wantOK: true},
		{name: "missing", granted: false, wantCurrent: "missing", wantOK: false},
		{name: "query error", queryErr: errors.New("boom"), wantCurrent: "missing", wantOK: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m, mock := newMockMSSQL(t)
			q := mock.ExpectQuery(jdbc.MSSQLViewDatabaseStatePermissionQuery())
			if tc.queryErr != nil {
				q.WillReturnError(tc.queryErr)
			} else {
				q.WillReturnRows(bitRow(tc.granted))
			}

			current, ok, err := m.checkViewDatabaseState(context.Background())
			assert.Equal(t, tc.queryErr, err)
			assert.Equal(t, tc.wantCurrent, current)
			assert.Equal(t, tc.wantOK, ok)
		})
	}
}

func TestMSSQLCheckDBOwner(t *testing.T) {
	t.Run("owner", func(t *testing.T) {
		m, mock := newMockMSSQL(t)
		mock.ExpectQuery(jdbc.MSSQLIsDBOwnerQuery()).WillReturnRows(bitRow(true))

		current, ok, err := m.checkDBOwner(context.Background())
		require.NoError(t, err)
		assert.Equal(t, "db_owner", current)
		assert.True(t, ok)
	})

	t.Run("not owner", func(t *testing.T) {
		m, mock := newMockMSSQL(t)
		mock.ExpectQuery(jdbc.MSSQLIsDBOwnerQuery()).WillReturnRows(bitRow(false))

		current, ok, err := m.checkDBOwner(context.Background())
		require.NoError(t, err)
		assert.Equal(t, "not db_owner", current)
		assert.False(t, ok)
	})

	// capture instances are created on the primary, so membership is checked there
	t.Run("uses the primary connection when configured", func(t *testing.T) {
		m, replicaMock := newMockMSSQL(t)
		primary, primaryMock := newMockClient(t)
		m.primaryClient = primary
		primaryMock.ExpectQuery(jdbc.MSSQLIsDBOwnerQuery()).WillReturnRows(bitRow(true))

		current, ok, err := m.checkDBOwner(context.Background())
		require.NoError(t, err)
		assert.Equal(t, "db_owner", current)
		assert.True(t, ok)
		// no query may reach the replica connection
		assert.NoError(t, replicaMock.ExpectationsWereMet())
	})

	t.Run("query error", func(t *testing.T) {
		m, mock := newMockMSSQL(t)
		mock.ExpectQuery(jdbc.MSSQLIsDBOwnerQuery()).WillReturnError(sql.ErrConnDone)

		_, ok, err := m.checkDBOwner(context.Background())
		require.ErrorIs(t, err, sql.ErrConnDone)
		assert.False(t, ok)
	})
}
