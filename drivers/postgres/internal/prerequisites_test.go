package driver

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/pkg/waljs"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	slotExistsQuery  = `SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1 AND database = current_database())`
	publicationQuery = "SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)"
)

// newMockPostgres returns a driver backed by sqlmock; queries must match exactly.
func newMockPostgres(t *testing.T) (*Postgres, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, mock.ExpectationsWereMet())
		_ = db.Close()
	})
	// Setup uses an Unsafe client; mirror it so unmapped slot columns are tolerated
	return &Postgres{client: sqlx.NewDb(db, "sqlmock").Unsafe(), config: &Config{Database: "app"}}, mock
}

func boolRow(column string, value bool) *sqlmock.Rows {
	return sqlmock.NewRows([]string{column}).AddRow(value)
}

func TestPostgresPrerequisiteChecks(t *testing.T) {
	names := func(cdc *CDC) map[string]bool {
		required := map[string]bool{}
		for _, c := range (&Postgres{}).prerequisiteChecks(cdc) {
			required[c.Name] = c.Required
			assert.NotEmpty(t, c.Description, c.Name)
			assert.NotEmpty(t, c.Recommended, c.Name)
			assert.NotNil(t, c.Check, c.Name)
		}
		return required
	}

	t.Run("pgoutput with publication", func(t *testing.T) {
		assert.Equal(t, map[string]bool{
			"wal_level":             true,
			"replication_privilege": true,
			"replication_slot":      true,
			"publication":           true,
		}, names(&CDC{ReplicationSlot: "olake_slot", Publication: "olake_pub"}))
	})

	t.Run("no publication configured skips the publication check", func(t *testing.T) {
		assert.Equal(t, map[string]bool{
			"wal_level":             true,
			"replication_privilege": true,
			"replication_slot":      true,
		}, names(&CDC{ReplicationSlot: "olake_slot"}))
	})
}

func TestPostgresCheckWalLevel(t *testing.T) {
	tests := []struct {
		level  string
		wantOK bool
	}{
		{"logical", true},
		{"replica", false},
		{"minimal", false},
	}
	for _, tc := range tests {
		t.Run(tc.level, func(t *testing.T) {
			p, mock := newMockPostgres(t)
			mock.ExpectQuery(jdbc.PostgresWalLevelQuery()).WillReturnRows(sqlmock.NewRows([]string{"wal_level"}).AddRow(tc.level))

			current, ok, err := p.checkWalLevel(context.Background())
			require.NoError(t, err)
			assert.Equal(t, tc.level, current)
			assert.Equal(t, tc.wantOK, ok)
		})
	}

	t.Run("query error", func(t *testing.T) {
		p, mock := newMockPostgres(t)
		mock.ExpectQuery(jdbc.PostgresWalLevelQuery()).WillReturnError(errors.New("boom"))

		_, ok, err := p.checkWalLevel(context.Background())
		require.Error(t, err)
		assert.False(t, ok)
	})
}

func TestPostgresCheckReplicationPrivilege(t *testing.T) {
	attrQuery := jdbc.PostgresReplicationAttrQuery()
	rdsQuery := jdbc.PostgresRDSReplicationRoleQuery()

	tests := []struct {
		name        string
		expect      func(sqlmock.Sqlmock)
		wantCurrent string
		wantOK      bool
		wantErr     bool
	}{
		{
			name: "REPLICATION attribute",
			expect: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(attrQuery).WillReturnRows(boolRow("has", true))
			},
			wantCurrent: "granted", wantOK: true,
		},
		{
			name: "rds_replication member",
			expect: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(attrQuery).WillReturnRows(boolRow("has", false))
				mock.ExpectQuery(rdsQuery).WillReturnRows(boolRow("pg_has_role", true))
			},
			wantCurrent: "granted", wantOK: true,
		},
		{
			// the role does not exist outside RDS/Aurora, so the query errors and is ignored
			name: "missing, rds role absent",
			expect: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(attrQuery).WillReturnRows(boolRow("has", false))
				mock.ExpectQuery(rdsQuery).WillReturnError(errors.New(`role "rds_replication" does not exist`))
			},
			wantCurrent: "missing", wantOK: false,
		},
		{
			name: "missing on rds",
			expect: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(attrQuery).WillReturnRows(boolRow("has", false))
				mock.ExpectQuery(rdsQuery).WillReturnRows(boolRow("pg_has_role", false))
			},
			wantCurrent: "missing", wantOK: false,
		},
		{
			name: "attribute query error",
			expect: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(attrQuery).WillReturnError(errors.New("boom"))
			},
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, mock := newMockPostgres(t)
			tc.expect(mock)

			current, ok, err := p.checkReplicationPrivilege(context.Background())
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

func TestPostgresCheckReplicationSlot(t *testing.T) {
	const slot = "olake_slot"
	slotInfoQuery := fmt.Sprintf(waljs.ReplicationSlotTempl, slot)
	slotRow := func(slotType, plugin string) *sqlmock.Rows {
		return sqlmock.NewRows([]string{"plugin", "slot_type", "confirmed_flush_lsn", "current_lsn"}).
			AddRow(plugin, slotType, "0/16B3748", "0/16B3780")
	}

	tests := []struct {
		name        string
		cdc         *CDC
		expect      func(sqlmock.Sqlmock)
		wantCurrent string
		wantOK      bool
		wantErr     bool
	}{
		{
			name: "logical pgoutput slot with publication",
			cdc:  &CDC{ReplicationSlot: slot, Publication: "olake_pub"},
			expect: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(slotExistsQuery).WithArgs(slot).WillReturnRows(boolRow("exists", true))
				mock.ExpectQuery(slotInfoQuery).WillReturnRows(slotRow("logical", "pgoutput"))
			},
			wantCurrent: "exists", wantOK: true,
		},
		{
			name: "logical wal2json slot needs no publication",
			cdc:  &CDC{ReplicationSlot: slot},
			expect: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(slotExistsQuery).WithArgs(slot).WillReturnRows(boolRow("exists", true))
				mock.ExpectQuery(slotInfoQuery).WillReturnRows(slotRow("logical", "wal2json"))
			},
			wantCurrent: "exists", wantOK: true,
		},
		{
			name: "slot not found",
			cdc:  &CDC{ReplicationSlot: slot, Publication: "olake_pub"},
			expect: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(slotExistsQuery).WithArgs(slot).WillReturnRows(boolRow("exists", false))
				mock.ExpectQuery(slotInfoQuery).WillReturnRows(sqlmock.NewRows([]string{"plugin", "slot_type", "confirmed_flush_lsn", "current_lsn"}))
			},
			wantCurrent: "not found", wantOK: false,
		},
		{
			name: "slot in another database",
			cdc:  &CDC{ReplicationSlot: slot, Publication: "olake_pub"},
			expect: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(slotExistsQuery).WithArgs(slot).WillReturnRows(boolRow("exists", false))
				mock.ExpectQuery(slotInfoQuery).WillReturnRows(slotRow("logical", "pgoutput"))
			},
			wantCurrent: "not in this database", wantOK: false,
		},
		{
			name: "physical slot",
			cdc:  &CDC{ReplicationSlot: slot, Publication: "olake_pub"},
			expect: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(slotExistsQuery).WithArgs(slot).WillReturnRows(boolRow("exists", true))
				mock.ExpectQuery(slotInfoQuery).WillReturnRows(slotRow("physical", ""))
			},
			wantCurrent: "only logical slots are supported: physical", wantOK: false,
		},
		{
			name: "pgoutput slot without publication",
			cdc:  &CDC{ReplicationSlot: slot},
			expect: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(slotExistsQuery).WithArgs(slot).WillReturnRows(boolRow("exists", true))
				mock.ExpectQuery(slotInfoQuery).WillReturnRows(slotRow("logical", "pgoutput"))
			},
			wantCurrent: "publication is required for pgoutput", wantOK: false,
		},
		{
			name: "query error",
			cdc:  &CDC{ReplicationSlot: slot},
			expect: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(slotExistsQuery).WithArgs(slot).WillReturnError(errors.New("boom"))
			},
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, mock := newMockPostgres(t)
			tc.expect(mock)

			current, ok, err := p.checkReplicationSlot(tc.cdc)(context.Background())
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

func TestPostgresCheckPublication(t *testing.T) {
	tests := []struct {
		name        string
		exists      bool
		queryErr    error
		wantCurrent string
		wantOK      bool
	}{
		{name: "exists", exists: true, wantCurrent: "exists", wantOK: true},
		{name: "missing", exists: false, wantCurrent: "not found", wantOK: false},
		{name: "query error", queryErr: errors.New("boom"), wantCurrent: "not found", wantOK: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, mock := newMockPostgres(t)
			q := mock.ExpectQuery(publicationQuery).WithArgs("olake_pub")
			if tc.queryErr != nil {
				q.WillReturnError(tc.queryErr)
			} else {
				q.WillReturnRows(boolRow("exists", tc.exists))
			}

			current, ok, err := p.checkPublication("olake_pub")(context.Background())
			assert.Equal(t, tc.queryErr, err)
			assert.Equal(t, tc.wantCurrent, current)
			assert.Equal(t, tc.wantOK, ok)
		})
	}
}
