package driver

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/datazip-inc/olake/drivers/base"
	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/typeutils"
	"github.com/datazip-inc/olake/utils"
	"github.com/jmoiron/sqlx"
)

const (
	discoverTime = 5 * time.Minute
	// TODO: make these queries Postgres version specific
	// get all schemas and table
	getPrivilegedTablesTmpl = `SELECT nspname as table_schema,
		relname as table_name
		FROM pg_class c
		JOIN pg_namespace n ON c.relnamespace = n.oid
		WHERE has_table_privilege(c.oid, 'SELECT')
		AND has_schema_privilege(current_user, nspname, 'USAGE')
		AND relkind IN ('r', 'm', 't', 'f', 'p')
		AND nspname NOT LIKE 'pg_%'  -- Exclude default system schemas
		AND nspname != 'information_schema';  -- Exclude information_schema`
	// get table schema
	getTableSchemaTmpl = `SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position`
	// get primary key columns
	getTablePrimaryKey = `SELECT column_name FROM information_schema.key_column_usage WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position`
)

type Postgres struct {
	*base.Driver
	client    *sqlx.DB
	config    *Config // postgres driver connection config
	cdcConfig CDC
}

func (p *Postgres) ChangeStreamSupported() bool {
	return p.CDCSupport
}

func (p *Postgres) Setup() error {
	err := p.config.Validate()
	if err != nil {
		return fmt.Errorf("failed to validate config: %s", err)
	}

	sqlxDB, err := sqlx.Open("pgx", p.config.Connection.String())
	if err != nil {
		return fmt.Errorf("failed to connect database: %s", err)
	}

	pgClient := sqlxDB.Unsafe()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// force a connection and test that it worked
	err = pgClient.PingContext(ctx)
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

		exists, err := doesReplicationSlotExists(pgClient, cdc.ReplicationSlot)
		if err != nil {
			return fmt.Errorf("failed to check replication slot: %s", err)
		}

		if !exists {
			return fmt.Errorf("replication slot %s does not exist", cdc.ReplicationSlot)
		}
		if cdc.InitialWaitTime == 0 {
			// default set 10 sec
			cdc.InitialWaitTime = 10
		}
		// no use of it if check not being called while sync run
		p.CDCSupport = true
		p.cdcConfig = *cdc
	} else {
		logger.Info("Standard Replication is selected")
	}
	p.client = pgClient
	return nil
}

func (p *Postgres) StateType() types.StateType {
	return types.GlobalType
}

func (p *Postgres) SetupState(state *types.State) {
	state.Type = p.StateType()
	p.State = state
}

func (p *Postgres) GetConfigRef() protocol.Config {
	p.config = &Config{}

	return p.config
}

func (p *Postgres) Spec() any {
	return Config{}
}

func (p *Postgres) Check() error {
	return p.Setup()
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
	streams := p.GetStreams()
	if len(streams) != 0 {
		return streams, nil
	}

	logger.Infof("Starting discover for postgres database %s", p.config.Database)
	discoverCtx, cancel := context.WithTimeout(context.Background(), discoverTime)
	defer cancel()

	query := jdbc.PostgresDiscoverTablesQuery()

	rows, err := p.client.QueryContext(discoverCtx, query, p.config.Database)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	var tableNames []string
	for rows.Next() {
		var tableName, schemaName string
		if err := rows.Scan(&tableName, &schemaName); err != nil {
			return nil, fmt.Errorf("failed to scan table: %w", err)
		}
		tableNames = append(tableNames, fmt.Sprintf("%s.%s", schemaName, tableName))
	}

	err = utils.Concurrent(discoverCtx, tableNames, len(tableNames), func(ctx context.Context, streamName string, _ int) error {
		stream, err := p.produceTableSchema(ctx, streamName)
		if err != nil && discoverCtx.Err() == nil {
			return fmt.Errorf("failed to process table[%s]: %s", streamName, err)
		}
		// Set the sync mode from the configuration
		if p.config.SyncSettings != nil {
			stream.SyncMode = types.SyncMode(p.config.SyncSettings.Mode)
		} else if p.config.DefaultMode != "" {
			// Fallback to DefaultMode for backward compatibility
			stream.SyncMode = p.config.DefaultMode
		}
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

func (p *Postgres) dataTypeConverter(value interface{}, columnType string) (interface{}, error) {
	if value == nil {
		return nil, typeutils.ErrNullValue
	}
	// (e.g., varchar(50) -> varchar)
	baseType := strings.ToLower(strings.TrimSpace(strings.Split(columnType, "(")[0]))
	olakeType := pgTypeToDataTypes[baseType]
	return typeutils.ReformatValue(olakeType, value)
}

func (p *Postgres) Read(pool *protocol.WriterPool, stream protocol.Stream) error {
	switch stream.GetSyncMode() {
	case types.FULLREFRESH:
		return p.backfill(pool, stream)
	case types.CDC:
		return p.RunChangeStream(pool, stream)
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
			datatype = types.String
		}

		stream.UpsertField(typeutils.Reformat(column.Name), datatype, strings.EqualFold("yes", *column.IsNullable))
	}

	// cdc additional fields
	if p.CDCSupport {
		for column, typ := range base.DefaultColumns {
			stream.UpsertField(column, typ, true)
		}
	}

	// TODO: Populate cursor fields for incremental purpose
	if p.CDCSupport {
		stream.WithSyncMode(types.FULLREFRESH)
		stream.WithSyncMode(types.CDC)

	} else {
		stream.WithSyncMode(types.FULLREFRESH)
	}

	// add primary keys for stream
	for _, column := range primaryKeyOutput {
		stream.WithPrimaryKey(column.Name)
	}

	return stream, nil
}

// ApplyDefaultSyncMode applies the default sync mode from the config or catalog to streams that don't have a specific sync mode set
func (p *Postgres) ApplyDefaultSyncMode(catalog *types.Catalog) *types.Catalog {
	// Apply DefaultMode from config first (for backward compatibility)
	if p.config.DefaultMode != "" && p.config.SyncSettings == nil {
		// Warn about deprecated config
		logger.Warn("The 'default_mode' field in the source configuration is deprecated. Please use 'sync_settings.mode' instead.")
		
		for i := range catalog.Streams {
			if catalog.Streams[i].Stream.SyncMode == "" {
				catalog.Streams[i].Stream.SyncMode = p.config.DefaultMode
				logger.Infof("Applied default sync mode '%s' from source config to stream '%s'", p.config.DefaultMode, catalog.Streams[i].Name())
			}
		}
	}

	// Apply DefaultMode from catalog (new way)
	if catalog.DefaultMode != "" {
		for i := range catalog.Streams {
			if catalog.Streams[i].Stream.SyncMode == "" {
				catalog.Streams[i].Stream.SyncMode = catalog.DefaultMode
				logger.Infof("Applied default sync mode '%s' from catalog to stream '%s'", catalog.DefaultMode, catalog.Streams[i].Name())
			}
		}
	}

	return catalog
}
