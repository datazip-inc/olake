package driver

import (
	"context"
	"database/sql"
	"errors"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/types"
	mssql "github.com/microsoft/go-mssqldb"
)

// The MSSQL config has no CDC selection yet, so checks must not block full-refresh users.
// Flip once the config carries CDC intent.
const cdcIntentInConfig = false

const (
	errSelectDenied   = 229 // SELECT permission denied on object
	errDatabaseDenied = 916 // login cannot access msdb
)

func (m *MSSQL) prerequisiteChecks() []abstract.Prerequisite {
	checks := []abstract.Prerequisite{{
		Name: "database_cdc", Required: cdcIntentInConfig, Recommended: "enabled",
		Description: "CDC is not enabled on the database, so no change tables exist to read.",
		Check:       m.checkDatabaseCDC,
	}}

	if m.config.ManageCaptureInstances {
		checks = append(checks, abstract.Prerequisite{
			Name: "capture_instance_admin", Required: cdcIntentInConfig, Recommended: "db_owner",
			Description: "Manage Capture Instance is on, but the user cannot create capture instances when the schema changes.",
			Check:       m.checkDBOwner,
		})
	}

	// resolveInitialLSN skips both on replicas; msdb jobs live on the primary.
	if !m.isReadReplica {
		checks = append(checks,
			abstract.Prerequisite{
				Name: "cdc_capture_job", Required: cdcIntentInConfig,
				Recommended: "capture job present, SELECT on msdb.dbo.cdc_jobs",
				Description: "Without a readable capture job, changes are not captured or OLake cannot pick a safe start position.",
				Check:       m.checkCaptureJob,
			},
			abstract.Prerequisite{
				Name: "view_database_state", Recommended: "VIEW DATABASE STATE (2022+: VIEW DATABASE PERFORMANCE STATE)",
				Description: "OLake can't confirm that the capture agent has caught up before it starts, which can produce duplicates in append mode.",
				Check:       m.checkViewDatabaseState,
			},
		)
	}
	return checks
}

func (m *MSSQL) checkDatabaseCDC(ctx context.Context) (string, bool, error) {
	enabled, err := m.isDatabaseCDCEnabled(ctx)
	if err != nil || !enabled {
		return "disabled", false, err
	}
	return "enabled", true, nil
}

// checkCaptureJob runs the query waitForCDCAgentCatchUp uses at sync start: it fails without
// msdb access and returns no row when the database has no capture job.
func (m *MSSQL) checkCaptureJob(ctx context.Context) (string, bool, error) {
	var maxTrans, pollingInterval int
	err := m.client.QueryRowContext(ctx, jdbc.MSSQLCDCCaptureJobConfigQuery()).Scan(&maxTrans, &pollingInterval)

	var serverErr mssql.Error
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return "no capture job", false, nil
	case errors.As(err, &serverErr) && (serverErr.Number == errSelectDenied || serverErr.Number == errDatabaseDenied):
		return "no SELECT on msdb.dbo.cdc_jobs", false, nil
	case err != nil:
		return "", false, err
	}
	return "present", true, nil
}

// checkViewDatabaseState wraps the permission query resolveInitialLSN runs.
func (m *MSSQL) checkViewDatabaseState(ctx context.Context) (string, bool, error) {
	var granted bool
	if err := m.client.QueryRowContext(ctx, jdbc.MSSQLViewDatabaseStatePermissionQuery()).Scan(&granted); err != nil || !granted {
		return "missing", false, err
	}
	return "granted", true, nil
}

// checkDBOwner checks db_owner membership, on the primary when one is configured since that is
// where capture instances are created.
func (m *MSSQL) checkDBOwner(ctx context.Context) (string, bool, error) {
	client := m.client
	if m.primaryClient != nil {
		client = m.primaryClient
	}
	var isOwner bool
	if err := client.QueryRowContext(ctx, jdbc.MSSQLIsDBOwnerQuery()).Scan(&isOwner); err != nil || !isOwner {
		return "not db_owner", false, err
	}
	return "db_owner", true, nil
}

// Prerequisites returns the CDC setup checks evaluated in Setup.
func (m *MSSQL) Prerequisites() types.Prerequisites {
	return m.prerequisites
}
