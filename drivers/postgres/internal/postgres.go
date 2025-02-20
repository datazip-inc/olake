package driver

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/datazip-inc/olake/drivers/base"
	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/pkg/waljs"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/jmoiron/sqlx"
)

const (
	discoverTime = 5 * time.Minute
)

type Postgres struct {
	*base.Driver

	client      *sqlx.DB
	accessToken string
	config      *Config // postgres driver connection config
	cdcConfig   CDC
	cdcState    *types.Global[*waljs.WALState]
}

// ChangeStreamSupported implements protocol.Driver.
// Subtle: this method shadows the method (*Driver).ChangeStreamSupported of Postgres.Driver.
func (p *Postgres) ChangeStreamSupported() bool {
	return p.CDCSupport
}

// Setup implements protocol.Driver.
func (p *Postgres) Setup() error {
	err := p.config.Validate()
	if err != nil {
		return fmt.Errorf("failed to validate config: %s", err)
	}

	db, err := sqlx.Open("pgx", p.config.Connection.String())
	if err != nil {
		return fmt.Errorf("failed to connect database: %s", err)
	}

	db = db.Unsafe()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// force a connection and test that it worked
	err = db.PingContext(ctx)
	if err != nil {
		return fmt.Errorf("failed to ping database: %s", err)
	}
	// TODO: correct cdc setup
	found, _ := utils.IsOfType(p.config.UpdateMethod, "replication_slot")
	if found {
		logger.Info("Found CDC Configuration")
		cdc := &CDC{}
		if err := utils.Unmarshal(p.config.UpdateMethod, cdc); err != nil {
			return err
		}

		exists, err := doesReplicationSlotExists(db, cdc.ReplicationSlot)
		if err != nil {
			return fmt.Errorf("failed to check replication slot: %s", err)
		}

		if !exists {
			return fmt.Errorf("replication slot %s does not exist!", cdc.ReplicationSlot)
		}
		// no use of it if check not being called while sync run
		p.Driver.CDCSupport = true
		p.cdcConfig = *cdc
	} else {
		logger.Info("Standard Replication is selected")
	}
	p.client = db
	return nil
}

func (p *Postgres) GetConfigRef() protocol.Config {
	p.config = &Config{}

	return p.config
}

func (p *Postgres) Spec() any {
	return Config{}
}

func (p *Postgres) Check() error {
	err := p.config.Validate()
	if err != nil {
		return fmt.Errorf("failed to validate config: %s", err)
	}

	db, err := sqlx.Open("pgx", p.config.Connection.String())
	if err != nil {
		return fmt.Errorf("failed to connect database: %s", err)
	}
	db = db.Unsafe()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// force a connection and test that it worked
	err = db.PingContext(ctx)
	if err != nil {
		return fmt.Errorf("failed to ping database: %s", err)
	}
	p.client = db

	return nil
}

func (p *Postgres) CloseConnection() {
	if p.client != nil {
		err := p.client.Close()
		if err != nil {
			logger.Error("failed to close connection with postgres: %s", err)
		}
	}
}

func (p *Postgres) Discover(discoverSchema bool) ([]*types.Stream, error) {
	// if not cached already; discover
	streams := p.GetStreams()
	if len(streams) != 0 {
		return streams, nil
	}

	logger.Infof("Starting discover for Postgres database %s", p.config.Database)

	discoverCtx, cancel := context.WithTimeout(context.Background(), discoverTime)
	defer cancel()

	var tableNamesOutput []Table
	err := p.client.Select(&tableNamesOutput, getPrivilegedTablesTmpl)
	if err != nil {
		return streams, fmt.Errorf("failed to retrieve table names: %s", err)
	}

	if len(tableNamesOutput) == 0 {
		logger.Warnf("no tables found")
		return streams, nil
	}

	err = utils.Concurrent(discoverCtx, tableNamesOutput, len(tableNamesOutput), func(ctx context.Context, pgTable Table, _ int) error {
		stream, err := p.populateStream(pgTable)
		if err != nil && discoverCtx.Err() == nil {
			return err
		}
		stream.SyncMode = p.config.DefaultSyncMode
		// cache stream
		p.AddStream(stream)
		return err
	})
	if err != nil {
		return nil, err
	}

	return p.GetStreams(), nil
}

func (p *Postgres) Type() string {
	return "Postgres"
}

func (p *Postgres) Read(pool *protocol.WriterPool, stream protocol.Stream) error {
	switch stream.GetSyncMode() {
	case types.FULLREFRESH:
		return p.backfill(pool, stream)
	case types.INCREMENTAL:
		// read incrementally
		return p.incrementalSync(pool, stream)
	}

	return nil
}

func (p *Postgres) populateStream(table Table) (*types.Stream, error) {
	// create new stream
	stream := types.NewStream(table.Name, table.Schema)
	var columnSchemaOutput []ColumnDetails
	err := p.client.Select(&columnSchemaOutput, getTableSchemaTmpl, table.Schema, table.Name)
	if err != nil {
		return stream, fmt.Errorf("failed to retrieve column details for table %s[%s]: %s", table.Name, table.Schema, err)
	}

	if len(columnSchemaOutput) == 0 {
		logger.Warnf("no columns found in table %s[%s]", table.Name, table.Schema)
		return stream, nil
	}

	var primaryKeyOutput []ColumnDetails
	err = p.client.Select(&primaryKeyOutput, getTablePrimaryKey, table.Schema, table.Name)
	if err != nil {
		return stream, fmt.Errorf("failed to retrieve primary key columns for table %s[%s]: %s", table.Name, table.Schema, err)
	}

	for _, column := range columnSchemaOutput {
		datatype := types.Unknown
		if val, found := pgTypeToDataTypes[*column.DataType]; found {
			datatype = val
		} else {
			logger.Warnf("failed to get respective type in datatypes for column: %s[%s]", column.Name, *column.DataType)
		}

		stream.UpsertField(column.Name, datatype, strings.EqualFold("yes", *column.IsNullable))
	}

	// cdc additional fields
	if p.Driver.CDCSupport {
		for column, typ := range jdbc.CDCColumns {
			stream.UpsertField(column, typ, true)
		}
	}

	// TODO: Populate cursor fields
	if !p.Driver.CDCSupport {
		stream.WithSyncMode(types.FULLREFRESH)
		// source has cursor fields, hence incremental also supported
		if stream.SourceDefinedPrimaryKey.Len() > 0 {
			stream.WithSyncMode(types.INCREMENTAL)
		}
	} else {
		stream.WithSyncMode(types.CDC)
	}

	// add primary keys for stream
	for _, column := range primaryKeyOutput {
		stream.WithPrimaryKey(column.Name)
	}

	stream.SupportedSyncModes.Insert("cdc")
	stream.SupportedSyncModes.Insert("full_refresh")

	return stream, nil
}
