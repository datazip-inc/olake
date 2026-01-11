package driver

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"time"

	_ "github.com/microsoft/go-mssqldb"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/jmoiron/sqlx"
)

type MSSQL struct {
	client *sqlx.DB
	config *Config

	CDCSupport bool

	state            *types.State
	streams          map[string]types.StreamInterface
	lastProcessedLSN string
}

// GetConfigRef implements abstract.DriverInterface.
func (m *MSSQL) GetConfigRef() abstract.Config {
	m.config = &Config{}
	return m.config
}

// Spec implements abstract.DriverInterface.
func (m *MSSQL) Spec() any {
	return Config{}
}

// Type implements abstract.DriverInterface.
func (m *MSSQL) Type() string {
	return string(constants.MSSQL)
}

func (m *MSSQL) CDCSupported() bool {
	return m.CDCSupport
}

// Setup establishes the database connection and initialises CDC settings.
// TODO: Add support for SSH Connection (bastion/jump node)
func (m *MSSQL) Setup(ctx context.Context) error {
	if err := m.config.Validate(); err != nil {
		return fmt.Errorf("failed to validate config: %s", err)
	}

	var client *sqlx.DB
	connStr := m.buildConnectionString()
	db, err := sql.Open("sqlserver", connStr)
	if err != nil {
		return fmt.Errorf("failed to open MSSQL connection: %s", err)
	}

	client = sqlx.NewDb(db, "sqlserver").Unsafe()
	// Set connection pool size
	client.SetMaxOpenConns(m.config.MaxThreads)

	// Test connection
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := client.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping database: %s", err)
	}

	m.client = client
	m.config.RetryCount = utils.Ternary(m.config.RetryCount <= 0, 1, m.config.RetryCount+1).(int)
	// Enable CDC support if database-level CDC is enabled
	cdcSupported, err := m.isDatabaseCDCEnabled(ctx)
	if err != nil {
		logger.Warnf("failed to check CDC support: %s", err)
	}
	if !cdcSupported {
		logger.Warnf("CDC is not supported")
	}
	m.CDCSupport = cdcSupported
	return nil
}

func (m *MSSQL) buildConnectionString() string {
	host := m.config.Host
	if !strings.Contains(host, ":") {
		host = fmt.Sprintf("%s:%d", host, m.config.Port)
	}

	query := url.Values{}
	query.Add("database", m.config.Database)

	// Set encrypt parameter based on SSL configuration
	// SSL modes: "disable" -> encrypt=disable, "require"/"verify-*" -> encrypt=true
	if m.config.SSLConfiguration == nil {
		query.Add("encrypt", "disable")
	} else {
		sslmode := string(m.config.SSLConfiguration.Mode)
		switch sslmode {
		case utils.SSLModeDisable:
			query.Add("encrypt", "disable")
		case utils.SSLModeRequire, utils.SSLModeVerifyCA, utils.SSLModeVerifyFull:
			query.Add("encrypt", "true")
			// TODO: Add support for certificate-based validation (verify-ca, verify-full)
			if sslmode == utils.SSLModeRequire {
				query.Add("TrustServerCertificate", "true")
			}
		default:
			query.Add("encrypt", "disable")
		}
	}

	u := &url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(m.config.Username, m.config.Password),
		Host:     host,
		RawQuery: query.Encode(),
	}

	return u.String()
}

// Close ensures proper cleanup
func (m *MSSQL) Close() error {
	if m.client != nil {
		err := m.client.Close()
		if err != nil {
			logger.Errorf("failed to close connection with MSSQL: %s", err)
		}
	}
	return nil
}

// SetupState wires global state reference.
func (m *MSSQL) SetupState(state *types.State) {
	m.state = state
}

func (m *MSSQL) MaxConnections() int {
	return m.config.MaxThreads
}

func (m *MSSQL) MaxRetries() int {
	return m.config.RetryCount
}

func (m *MSSQL) GetStreamNames(ctx context.Context) ([]string, error) {
	logger.Infof("Starting discover for MSSQL database %s", m.config.Database)

	query := jdbc.MSSQLDiscoverTablesQuery()
	rows, err := m.client.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables: %s", err)
	}
	defer rows.Close()

	var tableNames []string
	for rows.Next() {
		var tableName, schemaName string
		if err := rows.Scan(&schemaName, &tableName); err != nil {
			return nil, fmt.Errorf("failed to scan table: %s", err)
		}
		tableNames = append(tableNames, fmt.Sprintf("%s.%s", schemaName, tableName))
	}

	// Check for any errors that occurred while iterating over the rows
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tableNames, nil
}

func (m *MSSQL) ProduceSchema(ctx context.Context, streamName string) (*types.Stream, error) {
	produceTableSchema := func(ctx context.Context, streamName string) (*types.Stream, error) {
		logger.Infof("producing type schema for stream [%s]", streamName)
		parts := strings.Split(streamName, ".")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid stream name format: %s", streamName)
		}
		schemaName, tableName := parts[0], parts[1]
		stream := types.NewStream(tableName, schemaName, &m.config.Database)

		columnQuery := jdbc.MSSQLTableSchemaQuery()
		rows, err := m.client.QueryContext(ctx, columnQuery, schemaName, tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to query column information: %s", err)
		}
		defer rows.Close()

		type columnInfo struct {
			name         string
			dataType     string
			isNullable   string
			isPrimaryKey bool
		}

		var columns []columnInfo
		for rows.Next() {
			var colInfo columnInfo
			if err := rows.Scan(&colInfo.name, &colInfo.dataType, &colInfo.isNullable, &colInfo.isPrimaryKey); err != nil {
				return nil, fmt.Errorf("failed to scan column: %s", err)
			}
			columns = append(columns, colInfo)
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}

		for _, column := range columns {
			stream.WithCursorField(column.name)

			datatype := types.Unknown
			if val, found := mssqlTypeToDataTypes[strings.ToLower(column.dataType)]; found {
				datatype = val
			} else {
				logger.Warnf("Unsupported MSSQL type '%s' for column '%s.%s', defaulting to String", column.dataType, streamName, column.name)
				datatype = types.String
			}
			stream.UpsertField(column.name, datatype, strings.EqualFold(column.isNullable, "YES"))

			if column.isPrimaryKey {
				stream.WithPrimaryKey(column.name)
			}
		}

		return stream, nil
	}
	stream, err := produceTableSchema(ctx, streamName)
	if err != nil && ctx.Err() == nil {
		return nil, fmt.Errorf("failed to process table[%s]: %s", streamName, err)
	}
	return stream, nil
}

func (m *MSSQL) dataTypeConverter(value interface{}, columnType string) (interface{}, error) {
	if value == nil {
		return nil, typeutils.ErrNullValue
	}

	columnType = strings.ToLower(columnType)
	switch columnType {
	// SQL Server stores UNIQUEIDENTIFIER values in a mixed-endian binary format:
	// the first three fields are little-endian, while the remaining bytes are big-endian.
	// When the driver returns this value as []byte, we must reorder the bytes to
	// reconstruct a proper RFC4122 UUID string (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx).
	case "uniqueidentifier":
		if v, ok := value.([]byte); ok {
			return fmt.Sprintf("%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
				v[3], v[2], v[1], v[0], // first 4 bytes (little-endian)
				v[5], v[4], // next 2 bytes
				v[7], v[6], // next 2 bytes
				v[8], v[9], // next 2 bytes
				v[10], v[11], v[12], v[13], v[14], v[15]), nil // last 6 bytes
		}
		return fmt.Sprintf("%s", value), nil
	// TODO: check how to handle hierarchyid datatype
	case "hierarchyid":
		if val, ok := value.(string); ok {
			return val, nil
		}
		// Note: This returns a hex representation, not the hierarchical path format
		// For proper "/1/2/3/" format, cast in SQL using col.ToString()
		return fmt.Sprintf("%x", value), nil
	}

	olakeType := typeutils.ExtractAndMapColumnType(columnType, mssqlTypeToDataTypes)
	return typeutils.ReformatValue(olakeType, value)
}

func (m *MSSQL) isDatabaseCDCEnabled(ctx context.Context) (bool, error) {
	// sys.databases.is_cdc_enabled is a BIT; go-mssqldb returns it as bool.
	var isEnabled bool
	err := m.client.QueryRowContext(ctx, jdbc.MSSQLCDCSupportQuery()).Scan(&isEnabled)
	if err != nil {
		return false, fmt.Errorf("failed to check MSSQL CDC enablement: %s", err)
	}

	return isEnabled, nil
}

// IsTableCDCEnabled checks if CDC is enabled for a specific table.
// This is used during discover to determine if CDC sync modes should be available for a stream.
func (m *MSSQL) IsTableCDCEnabled(ctx context.Context, namespace, name string) (bool, error) {
	if !m.CDCSupport {
		return false, nil
	}
	var captureInstance string
	err := m.client.QueryRowContext(ctx, jdbc.MSSQLCDCTableEnabledQuery(), namespace, name).Scan(&captureInstance)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, fmt.Errorf("failed to check table CDC enablement for %s.%s: %s", namespace, name, err)
	}
	return true, nil
}
