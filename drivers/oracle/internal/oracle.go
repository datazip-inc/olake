package driver

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/jmoiron/sqlx"
)

// Oracle represents the Oracle database driver
type Oracle struct {
	config     *Config
	client     *sqlx.DB
	CDCSupport bool // indicates if the Oracle instance supports CDC
	cdcConfig  CDC
	state      *types.State // reference to globally present state
}

func (o *Oracle) CDCSupported() bool {
	return o.CDCSupport
}

// GetConfigRef returns a reference to the configuration
func (o *Oracle) GetConfigRef() abstract.Config {
	o.config = &Config{}
	return o.config
}

// Spec returns the configuration specification
func (o *Oracle) Spec() any {
	return Config{}
}

// Setup establishes the database connection
func (o *Oracle) Setup(ctx context.Context) error {
	err := o.config.Validate()
	if err != nil {
		return fmt.Errorf("failed to validate config: %s", err)
	}

	// Open database connection
	client, err := sqlx.Open("oracle", o.config.URI())
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

	found, _ := utils.IsOfType(o.config.UpdateMethod, "intial_wait_time")
	if found {
		logger.Info("Found CDC Configuration")
		cdc := &CDC{}
		if err := utils.Unmarshal(o.config.UpdateMethod, cdc); err != nil {
			return err
		}
		if cdc.InitialWaitTime == 0 {
			// default set 10 sec
			cdc.InitialWaitTime = 10
		}
		o.cdcConfig = *cdc
	}

	o.client = client
	o.config.RetryCount = utils.Ternary(o.config.RetryCount <= 0, 1, o.config.RetryCount+1).(int)

	// Enable CDC support if Oracle is configured for it
	// TODO: check for Oracle CDC permissions and configuration
	o.CDCSupport = true
	return nil
}

// Type returns the database type
func (o *Oracle) Type() string {
	return string(constants.Oracle)
}

// SetupState sets the state to Oracle
func (o *Oracle) SetupState(state *types.State) {
	o.state = state
}

func (o *Oracle) MaxConnections() int {
	return o.config.MaxThreads
}

func (o *Oracle) MaxRetries() int {
	return o.config.RetryCount
}

func (o *Oracle) GetStreamNames(ctx context.Context) ([]string, error) {
	logger.Infof("Starting discover for Oracle database %s", o.config.Database)
	query := jdbc.OracleDiscoverTablesQuery()
	rows, err := o.client.QueryContext(ctx, query, o.config.Username)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	var tableNames []string
	for rows.Next() {
		var tableName, schemaName string
		if err := rows.Scan(&tableName, &schemaName); err != nil {
			return nil, fmt.Errorf("failed to scan table: %s", err)
		}
		tableNames = append(tableNames, fmt.Sprintf("%s.%s", schemaName, tableName))
	}
	return tableNames, nil
}

func (o *Oracle) ProduceSchema(ctx context.Context, streamName string) (*types.Stream, error) {
	produceTableSchema := func(ctx context.Context, streamName string) (*types.Stream, error) {
		logger.Infof("producing type schema for stream [%s]", streamName)
		parts := strings.Split(streamName, ".")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid stream name format: %s", streamName)
		}
		schemaName, tableName := parts[0], parts[1]
		stream := types.NewStream(tableName, schemaName).WithSyncMode(types.FULLREFRESH, types.CDC)
		query := jdbc.OracleTableSchemaQuery()

		rows, err := o.client.QueryContext(ctx, query, schemaName, tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to query column information: %s", err)
		}
		defer rows.Close()

		for rows.Next() {
			var columnName, columnType, dataType, isNullable, columnKey string
			if err := rows.Scan(&columnName, &columnType, &dataType, &isNullable, &columnKey); err != nil {
				return nil, fmt.Errorf("failed to scan column: %s", err)
			}
			datatype := types.Unknown

			if val, found := oracleTypeToDataTypes[dataType]; found {
				datatype = val
			} else {
				logger.Warnf("Unsupported Oracle type '%s' for column '%s.%s', defaulting to String", dataType, streamName, columnName)
				datatype = types.String
			}
			stream.UpsertField(typeutils.Reformat(columnName), datatype, strings.EqualFold("Y", isNullable))

			// Mark primary keys
			if columnKey == "P" {
				stream.WithPrimaryKey(columnName)
			}
		}
		return stream, rows.Err()
	}
	stream, err := produceTableSchema(ctx, streamName)
	if err != nil && ctx.Err() == nil {
		return nil, fmt.Errorf("failed to process table[%s]: %s", streamName, err)
	}
	return stream, nil
}

func (o *Oracle) dataTypeConverter(value interface{}, columnType string) (interface{}, error) {
	if value == nil {
		return nil, typeutils.ErrNullValue
	}
	olakeType := typeutils.ExtractAndMapColumnType(columnType, oracleTypeToDataTypes)
	return typeutils.ReformatValue(olakeType, value)
}

// Close ensures proper cleanup
func (o *Oracle) Close() error {
	if o.client != nil {
		return o.client.Close()
	}
	return nil
}

// Oracle type to Olake data type mapping
var oracleTypeToDataTypes = map[string]types.DataType{
	"VARCHAR2":                       types.String,
	"NVARCHAR2":                      types.String,
	"CHAR":                           types.String,
	"NCHAR":                          types.String,
	"CLOB":                           types.String,
	"NCLOB":                          types.String,
	"LONG":                           types.String,
	"NUMBER":                         types.Float,
	"FLOAT":                          types.Float,
	"BINARY_FLOAT":                   types.Float,
	"BINARY_DOUBLE":                  types.Float,
	"DATE":                           types.DateTime,
	"TIMESTAMP":                      types.DateTime,
	"TIMESTAMP WITH TIME ZONE":       types.DateTime,
	"TIMESTAMP WITH LOCAL TIME ZONE": types.DateTime,
	"INTERVAL YEAR TO MONTH":         types.String,
	"INTERVAL DAY TO SECOND":         types.String,
	"BLOB":                           types.Binary,
	"BFILE":                          types.Binary,
	"RAW":                            types.Binary,
	"LONG RAW":                       types.Binary,
	"BINARY":                         types.Binary,
	"VARBINARY":                      types.Binary,
	"XMLTYPE":                        types.String,
	"JSON":                           types.String,
	"BOOLEAN":                        types.Boolean,
}

// GetOrSplitChunks implements the backfill chunking logic for Oracle
func (o *Oracle) GetOrSplitChunks(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
	// For now, implement a simple single chunk approach
	// TODO: Implement proper chunking logic based on Oracle-specific requirements
	chunk := types.Chunk{
		Min: nil,
		Max: nil,
	}

	chunkSet := types.NewSet[types.Chunk]()
	chunkSet.Add(chunk)

	return chunkSet, nil
}

// ChunkIterator implements the chunk iteration logic for Oracle
func (o *Oracle) ChunkIterator(ctx context.Context, stream types.StreamInterface, chunk types.Chunk, processFn abstract.BackfillMsgFn) error {
	// Build query based on chunk
	var query string
	if chunk.Min == nil && chunk.Max == nil {
		query = jdbc.OracleWithoutState(stream)
	} else {
		query = jdbc.OracleChunkScanQuery(stream, stream.Cursor(), chunk)
	}

	rows, err := o.client.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get columns: %w", err)
	}

	// Create a slice of interface{} to hold the values
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		// Convert row to map
		rowMap := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			if val != nil {
				// Convert Oracle-specific types if needed
				convertedVal, err := o.dataTypeConverter(val, "")
				if err != nil {
					logger.Warnf("Failed to convert value for column %s: %v", col, err)
					convertedVal = val
				}
				rowMap[col] = convertedVal
			} else {
				rowMap[col] = nil
			}
		}

		// Process the row
		if err := processFn(rowMap); err != nil {
			return fmt.Errorf("failed to process row: %w", err)
		}
	}

	return rows.Err()
}

// PreCDC initializes CDC state for Oracle
func (o *Oracle) PreCDC(ctx context.Context, streams []types.StreamInterface) error {
	// TODO: Implement Oracle-specific CDC initialization
	// This might involve setting up Oracle Streams, GoldenGate, or other CDC mechanisms
	logger.Info("Initializing Oracle CDC for streams")
	return nil
}

// StreamChanges streams CDC changes for Oracle
func (o *Oracle) StreamChanges(ctx context.Context, stream types.StreamInterface, processFn abstract.CDCMsgFn) error {
	// TODO: Implement Oracle-specific CDC streaming
	// This would typically involve:
	// 1. Setting up Oracle Streams or GoldenGate
	// 2. Monitoring redo logs
	// 3. Converting Oracle-specific change records to CDCChange format

	logger.Infof("Streaming CDC changes for Oracle stream: %s", stream.Name())

	// Placeholder implementation - in a real implementation, this would:
	// 1. Connect to Oracle CDC mechanism
	// 2. Monitor for changes
	// 3. Convert and process changes

	return nil
}

// PostCDC saves CDC state for Oracle
func (o *Oracle) PostCDC(ctx context.Context, stream types.StreamInterface, success bool) error {
	// TODO: Implement Oracle-specific CDC state saving
	logger.Infof("Saving Oracle CDC state for stream: %s, success: %v", stream.Name(), success)
	return nil
}
