package driver

import (
	"context"
	"database/sql"
	"errors"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/errs"
)

func (p *Postgres) prerequisiteChecks(cdc *CDC) []abstract.Prerequisite {
	checks := []abstract.Prerequisite{
		{
			Name: "wal_level", Required: true, Recommended: "logical",
			Description: "Without logical WAL, Postgres cannot stream row changes.",
			Check:       p.checkWalLevel,
		},
		{
			Name: "replication_privilege", Required: true, Recommended: "REPLICATION (RDS/Aurora: rds_replication)",
			Description: "The user cannot open a replication connection, so no changes can be read.",
			Check:       p.checkReplicationPrivilege,
		},
		{
			Name: "replication_slot", Required: true, Recommended: "logical slot in this database",
			Description: "The configured replication slot is missing or unusable, so there is no change stream to read.",
			Check:       p.checkReplicationSlot(cdc),
		},
	}
	if cdc.Publication != "" {
		checks = append(checks, abstract.Prerequisite{
			Name: "publication", Required: true, Recommended: "exists",
			Description: "The configured publication does not exist, so no changes are published to OLake.",
			Check:       p.checkPublication(cdc.Publication),
		})
	}
	return checks
}

func (p *Postgres) checkWalLevel(ctx context.Context) (string, bool, error) {
	var level string
	err := p.client.QueryRowContext(ctx, jdbc.PostgresWalLevelQuery()).Scan(&level)
	return level, level == "logical", err
}

// checkReplicationPrivilege checks the REPLICATION attribute, which covers most providers.
// RDS/Aurora grant it through rds_replication instead; that query errors elsewhere, so its error is ignored.
func (p *Postgres) checkReplicationPrivilege(ctx context.Context) (string, bool, error) {
	var granted bool
	if err := p.client.QueryRowContext(ctx, jdbc.PostgresReplicationAttrQuery()).Scan(&granted); err != nil {
		return "", false, err
	}
	if !granted {
		_ = p.client.QueryRowContext(ctx, jdbc.PostgresRDSReplicationRoleQuery()).Scan(&granted)
	}
	if !granted {
		return "missing", false, nil
	}
	return "granted", true, nil
}

// checkReplicationSlot reuses the slot validation; precondition errors become the current value.
func (p *Postgres) checkReplicationSlot(cdc *CDC) func(context.Context) (string, bool, error) {
	return func(ctx context.Context) (string, bool, error) {
		exists, err := doesReplicationSlotExists(ctx, p.client, cdc.ReplicationSlot, cdc.Publication, p.config.Database)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return "not found", false, nil
		case err != nil && errs.From(err).Category == errs.CDCPreconditionFailed:
			return err.Error(), false, nil // e.g. "only logical slots are supported: physical"
		case err != nil:
			return "", false, err
		case !exists:
			return "not in this database", false, nil
		}
		return "exists", true, nil
	}
}

// checkPublication reuses the existence check PreCDC runs.
func (p *Postgres) checkPublication(publication string) func(context.Context) (string, bool, error) {
	return func(ctx context.Context) (string, bool, error) {
		exists, err := checkPublicationExists(ctx, p.client, publication)
		if err != nil || !exists {
			return "not found", false, err
		}
		return "exists", true, nil
	}
}

// Prerequisites returns the CDC setup checks evaluated in Setup.
func (p *Postgres) Prerequisites() types.Prerequisites {
	return p.prerequisites
}
