package driver

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/datazip-inc/olake/drivers/base"
	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"

	// MySQL driver
	_ "github.com/go-sql-driver/mysql"
)

const (
	discoverTime = 5 * time.Minute // maximum time allowed to discover all the streams
)

// MySQL represents the MySQL database driver
type MySQL struct {
	*base.Driver
	config    *Config
	client    *sql.DB
	cdcConfig CDC
}

func (m *MySQL) StateType() types.StateType {
	return types.GlobalType
}

func (m *MySQL) SetupState(state *types.State) {
	state.Type = m.StateType()
	m.State = state
}

// GetConfigRef returns a reference to the configuration
func (m *MySQL) GetConfigRef() protocol.Config {
	m.config = &Config{}
	return m.config
}

// Spec returns the configuration specification
func (m *MySQL) Spec() any {
	return Config{}
}

// Setup establishes the database connection
func (m *MySQL) Setup() error {
	err := m.config.Validate()
	if err != nil {
		return fmt.Errorf("failed to validate config: %s", err)
	}
	// Open database connection
	client, err := sql.Open("mysql", m.config.URI())
	if err != nil {
		return fmt.Errorf("failed to open database connection: %w", err)
	}
	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := client.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}
	found, _ := utils.IsOfType(m.config.UpdateMethod, "intial_wait_time")
	if found {
		logger.Info("Found CDC Configuration")
		cdc := &CDC{}
		if err := utils.Unmarshal(m.config.UpdateMethod, cdc); err != nil {
			return err
		}
		if cdc.InitialWaitTime == 0 {
			// default set 10 sec
			cdc.InitialWaitTime = 10
		}
		m.cdcConfig = *cdc
	}
	m.client = client

	binLogPermissions, err := checkBinlogPermissions(m)
	if err != nil {
		logger.Errorf("failed to check binlog permissions for CDC support: %v", err)
	}

	userPermissions, err := checkUserPermissions(m)
	if err != nil {
		logger.Errorf("failed to check user permissions for CDC support: %v", err)
	}

	// Enable CDC support if binlog is configured
	m.CDCSupport = binLogPermissions && userPermissions
	return nil
}

// QueryRow executes a query and scans the result into the destination
func QueryRow(client *sql.DB, query string, dest ...any) error {
	return client.QueryRow(query).Scan(dest...)
}

// checkBinLogPermissions verifies the binary log permissions required for CDC support
func checkBinlogPermissions(m *MySQL) (bool, error) {
	var variableName string
	var variableValue string

	err := QueryRow(m.client, "SHOW VARIABLES LIKE 'log_bin'", &variableName, &variableValue)
	if err != nil {
		return false, fmt.Errorf("failed to check log_bin: %w", err)
	}
	if variableValue != "ON" {
		logger.Warnf("log_bin is not set to 'ON'")
		return false, nil
	}

	err = QueryRow(m.client, "SHOW VARIABLES LIKE 'binlog_format'", &variableName, &variableValue)
	if err != nil {
		return false, fmt.Errorf("failed to check binlog_format: %w", err)
	}
	if variableValue != "ROW" {
		logger.Warnf("binlog_format is not set to ROW")
		return false, nil
	}

	err = QueryRow(m.client, "SHOW VARIABLES LIKE 'binlog_row_metadata'", &variableName, &variableValue)
	if err != nil {
		return false, fmt.Errorf("failed to check binlog_row_metadata: %w", err)
	}
	if variableValue != "FULL" {
		logger.Warnf("binlog_row_metadata is not set to FULL")
		return false, nil
	}

	return true, nil
}

// checkUserPermissions verifies user permissions required for CDC support
func checkUserPermissions(m *MySQL) (bool, error) {
	var userGrants string

	err := QueryRow(m.client, "SHOW GRANTS FOR CURRENT_USER()", &userGrants)
	if err != nil {
		return false, fmt.Errorf("failed to check user privileges: %w", err)
	}
	if !strings.Contains(userGrants, "REPLICATION CLIENT") {
		logger.Warnf("user does not have REPLICATION CLIENT privilege")
		return false, nil
	}

	if !strings.Contains(userGrants, "REPLICATION SLAVE") {
		logger.Warnf("user does not have REPLICATION SLAVE privilege")
		return false, nil
	}

	return true, nil
}

// Check verifies the database connection
func (m *MySQL) Check() error {
	return m.Setup()
}

// Type returns the database type
func (m *MySQL) Type() string {
	return "MySQL"
}

// Discover finds and catalogs database tables
func (m *MySQL) Discover(discoverSchema bool) ([]*types.Stream, error) {
	streams := m.GetStreams()
	if len(streams) != 0 {
		return streams, nil
	}

	logger.Infof("Starting discover for MySQL database %s", m.config.Database)
	discoverCtx, cancel := context.WithTimeout(context.Background(), discoverTime)
	defer cancel()

	query := jdbc.MySQLDiscoverTablesQuery()

	rows, err := m.client.QueryContext(discoverCtx, query, m.config.Database)
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
		stream, err := m.produceTableSchema(ctx, streamName)
		if err != nil && discoverCtx.Err() == nil {
			return fmt.Errorf("failed to process table[%s]: %s", streamName, err)
		}
		stream.SyncMode = m.config.DefaultMode
		m.AddStream(stream)
		return err
	})
	if err != nil {
		return nil, err
	}

	return m.GetStreams(), nil
}

// Read handles different sync modes for data retrieval
func (m *MySQL) Read(pool *protocol.WriterPool, stream protocol.Stream) error {
	switch stream.GetSyncMode() {
	case types.FULLREFRESH:
		return m.backfill(pool, stream)
	case types.CDC:
		return m.RunChangeStream(pool, stream)
	}

	return nil
}

// produceTableSchema extracts schema information for a given table
func (m *MySQL) produceTableSchema(ctx context.Context, streamName string) (*types.Stream, error) {
	logger.Infof("producing type schema for stream [%s]", streamName)

	parts := strings.Split(streamName, ".")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid stream name format: %s", streamName)
	}
	schemaName, tableName := parts[0], parts[1]
	stream := types.NewStream(tableName, schemaName).WithSyncMode(types.FULLREFRESH, types.CDC)

	query := jdbc.MySQLTableSchemaQuery()

	rows, err := m.client.QueryContext(ctx, query, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query column information: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var columnName, columnType, dataType, isNullable, columnKey string
		if err := rows.Scan(&columnName, &columnType, &dataType, &isNullable, &columnKey); err != nil {
			return nil, fmt.Errorf("failed to scan column: %w", err)
		}
		datatype := types.Unknown

		if val, found := mysqlTypeToDataTypes[dataType]; found {
			datatype = val
		} else {
			logger.Warnf("Unsupported MySQL type '%s'for column '%s.%s', defaulting to String", dataType, streamName, columnName)
			datatype = types.String
		}
		stream.UpsertField(columnName, datatype, strings.EqualFold("yes", isNullable))

		// Mark primary keys
		if columnKey == "PRI" {
			stream.WithPrimaryKey(columnName)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}
	// TODO: Populate cursor fields for incremental purpose

	// Add CDC columns if supported
	if m.CDCSupport {
		for column, typ := range base.DefaultColumns {
			stream.UpsertField(column, typ, true)
		}
		stream.WithSyncMode(types.CDC)

	}
	stream.WithSyncMode(types.FULLREFRESH)

	return stream, nil
}

// Close ensures proper cleanup
func (m *MySQL) Close() error {
	if m.client != nil {
		return m.client.Close()
	}
	return nil
}
