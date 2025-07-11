package driver

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/jmoiron/sqlx"
	_ "github.com/sijms/go-ora/v2"
)

type Oracle struct {
	config     *Config
	client     *sqlx.DB
	state      *types.State
	CDCSupport bool
}

func (o *Oracle) Setup(ctx context.Context) error {
	err := o.config.Validate()
	if err != nil {
		return fmt.Errorf("failed to validate config: %s", err)
	}
	// TODO: Add support for more encryption options provided in OracleDB
	connectionString, err := o.config.connectionString()
	if err != nil {
		return fmt.Errorf("failed to get connection string: %s", err)
	}
	client, err := sqlx.Open("oracle",connectionString)
	if err != nil {
		return fmt.Errorf("failed to open database connection: %s", err)
	}

	// Set connection pool size
	client.SetMaxOpenConns(o.config.MaxThreads)

	// Test connection
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := client.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping database: %s", err)
	}

	o.client = client
	o.config.RetryCount = utils.Ternary(o.config.RetryCount <= 0, 1, o.config.RetryCount+1).(int)
	return nil
}

func (o *Oracle) GetConfigRef() abstract.Config {
	o.config = &Config{}
	return o.config
}

func (o *Oracle) Spec() any {
	return Config{}
}

// Close closes the database connection
func (o *Oracle) Close() error {
	if o.client != nil {
		return o.client.Close()
	}
	return nil
}

// Type returns the database type
func (o *Oracle) Type() string {
	return string(constants.Oracle)
}

// MaxConnections returns the maximum number of connections
func (o *Oracle) MaxConnections() int {
	return o.config.MaxThreads
}

// MaxRetries returns the maximum number of retries
func (o *Oracle) MaxRetries() int {
	return o.config.RetryCount
}

// GetStreamNames returns a list of available tables/streams
func (o *Oracle) GetStreamNames(ctx context.Context) ([]string, error) {
	logger.Infof("Starting discover for Oracle database")
	// TODO: Add support for custom schema names
	query := jdbc.OracleTableDiscoveryQuery()
	rows, err := o.client.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables: %s", err)
	}
	defer rows.Close()

	var streamNames []string
	for rows.Next() {
		var owner, table_name string
		if err := rows.Scan(&owner, &table_name); err != nil {
			return nil, fmt.Errorf("failed to scan table: %s", err)
		}
		streamNames = append(streamNames, fmt.Sprintf("%s.%s", owner, table_name))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating tables: %s", err)
	}

	return streamNames, nil
}

// ProduceSchema generates the schema for a given stream
func (o *Oracle) ProduceSchema(ctx context.Context, streamName string) (*types.Stream, error) {
	logger.Infof("producing type schema for stream [%s]", streamName)
	parts := strings.Split(streamName, ".")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid stream name format: %s", streamName)
	}
	schemaName, tableName := parts[0], parts[1]
	stream := types.NewStream(tableName, schemaName).WithSyncMode(types.FULLREFRESH, types.INCREMENTAL)

	// Get column information
	query := jdbc.OracleTableDetailsQuery(schemaName, tableName)
	rows, err := o.client.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query column information: %s", err)
	}
	defer rows.Close()

	for rows.Next() {
		var columnName, dataType, isNullable string
		var dataPrecision, dataScale sql.NullInt64
		if err := rows.Scan(&columnName, &dataType, &isNullable, &dataPrecision, &dataScale); err != nil {
			return nil, fmt.Errorf("failed to scan column: %s", err)
		}
		if strings.EqualFold("N", isNullable) {
			stream.WithCursorField(columnName)
		}
		datatype := types.Unknown
		if val, found := reformatOracleDatatype(dataType, dataPrecision, dataScale); found {
			datatype = val
		} else {
			logger.Warnf("Unsupported Oracle type '%s' for column '%s.%s', defaulting to String", dataType, streamName, columnName)
			datatype = types.String
		}

		stream.UpsertField(typeutils.Reformat(columnName), datatype, strings.EqualFold("Y", isNullable))
	}

	query = jdbc.OraclePrimaryKeyColummsQuery(schemaName, tableName)
	pkRows, err := o.client.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query primary key information: %s", err)
	}
	defer pkRows.Close()

	for pkRows.Next() {
		var columnName string
		if err := pkRows.Scan(&columnName); err != nil {
			return nil, fmt.Errorf("failed to scan primary key column: %s", err)
		}
		stream.WithPrimaryKey(columnName)
	}

	return stream, pkRows.Err()
}

func (o *Oracle) dataTypeConverter(value interface{}, columnType string) (interface{}, error) {
	if value == nil {
		return nil, typeutils.ErrNullValue
	}
	olakeType := typeutils.ExtractAndMapColumnType(columnType, oracleTypeToDataTypes)
	return typeutils.ReformatValue(olakeType, value)
}
