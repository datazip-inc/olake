package driver

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/jmoiron/sqlx"
	_ "github.com/sijms/go-ora/v2" // Pure Go Oracle driver
)

type Oracle struct {
	config *Config
	client *sqlx.DB
	state  *types.State
}

func (o *Oracle) Setup(ctx context.Context) error {
	err := o.config.Validate()
	if err != nil {
		return fmt.Errorf("failed to validate config: %s", err)
	}

	// Open database connection using go-ora
	client, err := sqlx.Open("oracle", o.config.connectionString())
	if err != nil {
		return fmt.Errorf("failed to open database connection: %s", err)
	}

	// Test connection
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Set connection pool size
	client.SetMaxOpenConns(o.config.MaxThreads)
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
	query := `SELECT USER AS owner, table_name FROM user_tables`

	rows, err := o.client.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	var streamNames []string
	for rows.Next() {
		var owner, table_name string
		if err := rows.Scan(&owner, &table_name); err != nil {
			return nil, fmt.Errorf("failed to scan table: %s", err)
		}
		streamId := fmt.Sprintf("%s.%s", owner, table_name)
		streamNames = append(streamNames, streamId)
		logger.Infof("Discovered table: %s", streamId)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating tables: %w", err)
	}

	logger.Infof("Discovery complete. Found %d tables", len(streamNames))
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

	stream := types.NewStream(tableName, schemaName).WithSyncMode(types.FULLREFRESH)
	stream.SyncMode = o.config.DefaultMode

	// Get column information
	query := `
		SELECT column_name, data_type, nullable
		FROM all_tab_columns
		WHERE owner = :1 AND table_name = :2`

	rows, err := o.client.QueryContext(ctx, query, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query column information: %s", err)
	}
	defer rows.Close()

	for rows.Next() {
		var columnName, dataType, isNullable string
		if err := rows.Scan(&columnName, &dataType, &isNullable); err != nil {
			return nil, fmt.Errorf("failed to scan column: %s", err)
		}

		datatype := types.Unknown
		if val, found := OracleDatatype(dataType); found {
			datatype = val
		} else {
			logger.Warnf("Unsupported Oracle type '%s' for column '%s.%s', defaulting to String", dataType, streamName, columnName)
			datatype = types.String
		}

		stream.UpsertField(typeutils.Reformat(columnName), datatype, strings.EqualFold("Y", isNullable))
	}

	// Get primary key information
	pkQuery := `
		SELECT cols.column_name
		FROM all_constraints cons, all_cons_columns cols
		WHERE cons.constraint_type = 'P'
		AND cons.constraint_name = cols.constraint_name
		AND cons.owner = cols.owner
		AND cons.owner = :1
		AND cols.table_name = :2`

	pkRows, err := o.client.QueryContext(ctx, pkQuery, schemaName, tableName)
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

	return stream, nil
}

// PostCDC is called after CDC operation completes
func (o *Oracle) PostCDC(ctx context.Context, stream types.StreamInterface, success bool) error {
	// Since CDC is not supported yet, this is a no-op
	return nil
}

// PreCDC is called before CDC operation starts
func (o *Oracle) PreCDC(ctx context.Context, streams []types.StreamInterface) error {
	// Since CDC is not supported yet, this is a no-op
	return nil
}

// StreamChanges streams CDC changes for a given stream
func (o *Oracle) StreamChanges(ctx context.Context, stream types.StreamInterface, processFn abstract.CDCMsgFn) error {
	// Since CDC is not supported yet, this is a no-op
	return nil
}

// CDCSupported returns whether CDC is supported
func (o *Oracle) CDCSupported() bool {
	return false // CDC is not supported yet
}

// SetupState sets the state for the driver
func (o *Oracle) SetupState(state *types.State) {
	o.state = state
}

func (o *Oracle) dataTypeConverter(value interface{}, columnType string) (interface{}, error) {
	if value == nil {
		return nil, typeutils.ErrNullValue
	}
	olakeType := typeutils.ExtractAndMapColumnType(columnType, oracleTypeToDataTypes)
	return typeutils.ReformatValue(olakeType, value)
}
