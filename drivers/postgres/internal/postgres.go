package driver

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/waljs"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
)

const (
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
	client     *sqlx.DB
	config     *Config // postgres driver connection config
	CDCSupport bool    // indicates if the Postgres instance supports CDC
	cdcConfig  CDC
	Socket     *waljs.Socket
}

func (p *Postgres) CDCSupported() bool {
	return p.CDCSupport
}

func (p *Postgres) Setup(ctx context.Context) error {
	err := p.config.Validate()
	if err != nil {
		return fmt.Errorf("failed to validate config: %s", err)
	}

	sqlxDB, err := sqlx.Open("pgx", p.config.Connection.String())
	if err != nil {
		return fmt.Errorf("failed to connect database: %s", err)
	}
	sqlxDB.SetMaxOpenConns(p.config.MaxThreads)
	pgClient := sqlxDB.Unsafe()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
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
			return fmt.Errorf("failed to check existence of replication slot %s: %s", cdc.ReplicationSlot, err)
		}

		if !exists {
			return fmt.Errorf("provided replication slot %s does not exist", cdc.ReplicationSlot)
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
	p.config.RetryCount = utils.Ternary(p.config.RetryCount <= 0, 1, p.config.RetryCount+1).(int)
	return nil
}

func (p *Postgres) StateType() types.StateType {
	return types.GlobalType
}

func (p *Postgres) GetConfigRef() abstract.Config {
	p.config = &Config{}

	return p.config
}

func (p *Postgres) Spec() any {
	return Config{}
}

func (p *Postgres) CloseConnection() {
	if p.client != nil {
		err := p.client.Close()
		if err != nil {
			logger.Error("failed to close connection with postgres: %s", err)
		}
	}
}

func (p *Postgres) GetStreamNames(ctx context.Context) ([]string, error) {
	logger.Infof("Starting discover for Postgres database %s", p.config.Database)
	var tableNamesOutput []Table
	err := p.client.Select(&tableNamesOutput, getPrivilegedTablesTmpl)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve table names: %s", err)
	}
	tablesNames := []string{}
	for _, table := range tableNamesOutput {
		tablesNames = append(tablesNames, fmt.Sprintf("%s.%s", table.Schema, table.Name))
	}
	return tablesNames, nil
}

func (p *Postgres) ProduceSchema(ctx context.Context, streamName string) (*types.Stream, error) {
	populateStream := func(streamName string) (*types.Stream, error) {
		streamParts := strings.Split(streamName, ".")
		schemaName, streamName := streamParts[0], streamParts[1]
		stream := types.NewStream(streamName, schemaName)
		stream.SyncMode = p.config.DefaultSyncMode
		var columnSchemaOutput []ColumnDetails
		err := p.client.Select(&columnSchemaOutput, getTableSchemaTmpl, schemaName, streamName)
		if err != nil {
			return stream, fmt.Errorf("failed to retrieve column details for table %s: %s", streamName, err)
		}

		if len(columnSchemaOutput) == 0 {
			logger.Warnf("no columns found in table [%s.%s]", schemaName, streamName)
			return stream, nil
		}

		var primaryKeyOutput []ColumnDetails
		err = p.client.Select(&primaryKeyOutput, getTablePrimaryKey, schemaName, streamName)
		if err != nil {
			return stream, fmt.Errorf("failed to retrieve primary key columns for table %s: %s", streamName, err)
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

		stream.WithSyncMode(types.FULLREFRESH)
		// add primary keys for stream
		for _, column := range primaryKeyOutput {
			stream.WithPrimaryKey(column.Name)
		}

		return stream, nil
	}

	stream, err := populateStream(streamName)
	if err != nil && ctx.Err() == nil {
		return nil, err
	}
	return stream, nil
}

func (p *Postgres) Type() string {
	return string(constants.Postgres)
}

func (p *Postgres) MaxConnections() int {
	return p.config.MaxThreads
}

func (p *Postgres) MaxRetries() int {
	return p.config.RetryCount
}

func (p *Postgres) dataTypeConverter(value interface{}, columnType string) (interface{}, error) {
	if value == nil {
		return nil, typeutils.ErrNullValue
	}
	olakeType := typeutils.ExtractAndMapColumnType(columnType, pgTypeToDataTypes)
	return typeutils.ReformatValue(olakeType, value)
}
