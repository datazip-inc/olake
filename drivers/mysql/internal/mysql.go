package driver

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/binlog"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/jmoiron/sqlx"

	// MySQL driver
	_ "github.com/go-sql-driver/mysql"
)

// MySQL represents the MySQL database driver
type MySQL struct {
	config     *Config
	client     *sqlx.DB
	CDCSupport bool // indicates if the MySQL instance supports CDC
	cdcConfig  CDC
	BinlogConn *binlog.Connection
	state      *types.State // reference to globally present state
}

// MySQLGlobalState tracks the binlog position and backfilled streams.
type MySQLGlobalState struct {
	ServerID uint32        `json:"server_id"`
	State    binlog.Binlog `json:"state"`
}

func (m *MySQL) CDCSupported() bool {
	return m.CDCSupport
}

// GetConfigRef returns a reference to the configuration
func (m *MySQL) GetConfigRef() abstract.Config {
	m.config = &Config{}
	return m.config
}

// Spec returns the configuration specification
func (m *MySQL) Spec() any {
	return Config{}
}

// Setup establishes the database connection
func (m *MySQL) Setup(ctx context.Context) error {
	err := m.config.Validate()
	if err != nil {
		return fmt.Errorf("failed to validate config: %s", err)
	}
	// Open database connection
	client, err := sqlx.Open("mysql", m.config.URI())
	if err != nil {
		return fmt.Errorf("failed to open database connection: %s", err)
	}
	// Test connection
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	// Set connection pool size
	client.SetMaxOpenConns(m.config.MaxThreads)
	if err := client.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping database: %s", err)
	}
	found, _ := utils.IsOfType(m.config.UpdateMethod, "initial_wait_time")
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
	m.config.RetryCount = utils.Ternary(m.config.RetryCount <= 0, 1, m.config.RetryCount+1).(int)
	// Enable CDC support if binlog is configured
	cdcSupported, err := m.IsCDCSupported(ctx)
	if err != nil {
		logger.Warnf("failed to check CDC support: %s", err)
	}
	if !cdcSupported {
		logger.Warnf("CDC is not supported")
	}
	m.CDCSupport = cdcSupported
	return nil
}

// Type returns the database type
func (m *MySQL) Type() string {
	return string(constants.MySQL)
}

// set state to mysql
func (m *MySQL) SetupState(state *types.State) {
	m.state = state
}

func (m *MySQL) MaxConnections() int {
	return m.config.MaxThreads
}

func (m *MySQL) MaxRetries() int {
	return m.config.RetryCount
}

func (m MySQL) GetStreamNames(ctx context.Context) ([]string, error) {
	logger.Infof("Starting discover for MySQL database %s", m.config.Database)
	query := jdbc.MySQLDiscoverTablesQuery()
	rows, err := m.client.QueryContext(ctx, query, m.config.Database)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables: %s", err)
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

func (m *MySQL) ProduceSchema(ctx context.Context, streamName string) (*types.Stream, error) {
	produceTableSchema := func(ctx context.Context, streamName string) (*types.Stream, error) {
		logger.Infof("producing type schema for stream [%s]", streamName)
		parts := strings.Split(streamName, ".")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid stream name format: %s", streamName)
		}
		schemaName, tableName := parts[0], parts[1]
		stream := types.NewStream(tableName, schemaName)
		query := jdbc.MySQLTableSchemaQuery()

		rows, err := m.client.QueryContext(ctx, query, schemaName, tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to query column information: %s", err)
		}
		defer rows.Close()

		for rows.Next() {
			var columnName, columnType, dataType, isNullable, columnKey string
			if err := rows.Scan(&columnName, &columnType, &dataType, &isNullable, &columnKey); err != nil {
				return nil, fmt.Errorf("failed to scan column: %s", err)
			}
			if strings.EqualFold("no", isNullable) {
				stream.WithCursorField(columnName)
			}
			datatype := types.Unknown

			if val, found := mysqlTypeToDataTypes[dataType]; found {
				datatype = val
			} else {
				logger.Warnf("Unsupported MySQL type '%s'for column '%s.%s', defaulting to String", dataType, streamName, columnName)
				datatype = types.String
			}
			stream.UpsertField(typeutils.Reformat(columnName), datatype, strings.EqualFold("yes", isNullable))

			// Mark primary keys
			if columnKey == "PRI" {
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

func (m *MySQL) dataTypeConverter(value interface{}, columnType string) (interface{}, error) {
	if value == nil {
		return nil, typeutils.ErrNullValue
	}
	olakeType := typeutils.ExtractAndMapColumnType(columnType, mysqlTypeToDataTypes)
	return typeutils.ReformatValue(olakeType, value)
}

// Close ensures proper cleanup
func (m *MySQL) Close() error {
	if m.client != nil {
		return m.client.Close()
	}
	return nil
}

func (m *MySQL) IsCDCSupported(ctx context.Context) (bool, error) {
	// Permission check via SHOW MASTER STATUS / SHOW BINARY LOG STATUS
	if _, err := m.getCurrentBinlogPosition(); err != nil {
		return false, fmt.Errorf("failed to get binlog position: %s", err)
	}
	// checkMySQLConfig checks a MySQL configuration value against an expected value
	checkMySQLConfig := func(ctx context.Context, query, expectedValue, warnMessage string) (bool, error) {
		var name, value string
		if err := m.client.QueryRowxContext(ctx, query).Scan(&name, &value); err != nil {
			return false, fmt.Errorf("failed to check %s: %s", name, err)
		}

		if strings.ToUpper(value) != expectedValue {
			logger.Warnf(warnMessage)
			return false, nil
		}

		return true, nil
	}

	// Check binlog configurations
	configChecks := []struct {
		query         string
		expectedValue string
		errMessage    string
	}{
		{jdbc.MySQLLogBinQuery(), "ON", "log_bin is not enabled"},
		{jdbc.MySQLBinlogFormatQuery(), "ROW", "binlog_format is not set to ROW"},
		{jdbc.MySQLBinlogRowMetadataQuery(), "FULL", "binlog_row_metadata is not set to FULL"},
	}

	for _, check := range configChecks {
		if ok, err := checkMySQLConfig(ctx, check.query, check.expectedValue, check.errMessage); err != nil || !ok {
			return ok, err
		}
	}

	return true, nil
}
