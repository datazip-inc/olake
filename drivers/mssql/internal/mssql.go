package driver

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	mssql "github.com/microsoft/go-mssqldb"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/jmoiron/sqlx"
	"golang.org/x/crypto/ssh"
)

type MSSQL struct {
	client       *sqlx.DB
	config       *Config
	state        *types.State
	capturesMap  map[string][]captureInstance
	lsnMap       sync.Map
	streams      []types.StreamInterface
	cdcSupported bool
	sshClient    *ssh.Client
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
	return m.cdcSupported
}

// Setup establishes the database connection and initialises CDC settings.
func (m *MSSQL) Setup(ctx context.Context) error {
	if err := m.config.Validate(); err != nil {
		return fmt.Errorf("failed to validate config: %s", err)
	}

	if m.config.SSHConfig != nil && m.config.SSHConfig.Host != "" {
		logger.Info("Found SSH Configuration")
		var err error
		m.sshClient, err = m.config.SSHConfig.SetupSSHConnection()
		if err != nil {
			return fmt.Errorf("failed to setup SSH connection: %s", err)
		}
	}

	var client *sqlx.DB
	connStr := m.buildConnectionString()

	if m.sshClient != nil {
		logger.Info("Connecting to MSSQL via SSH tunnel")

		connector, err := mssql.NewConnector(connStr)
		if err != nil {
			return fmt.Errorf("failed to create MSSQL connector: %s", err)
		}

		connector.Dialer = &mssqlSSHDialer{sshClient: m.sshClient, host: m.config.Host}

		db := sql.OpenDB(connector)
		client = sqlx.NewDb(db, "sqlserver")
	} else {
		db, err := sql.Open("sqlserver", connStr)
		if err != nil {
			return fmt.Errorf("failed to open MSSQL connection: %s", err)
		}
		client = sqlx.NewDb(db, "sqlserver")
	}

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
	m.cdcSupported = cdcSupported
	return nil
}

func (m *MSSQL) buildConnectionString() string {
	host := m.config.Host
	if !strings.Contains(host, ":") {
		host = fmt.Sprintf("%s:%d", host, m.config.Port)
	}

	query := url.Values{}
	query.Add("database", m.config.Database)

	// Set encrypt parameter based on SSL configuration.
	if m.config.SSLConfiguration == nil {
		query.Add("encrypt", "disable")
	} else {
		sslmode := string(m.config.SSLConfiguration.Mode)
		switch sslmode {
		case utils.SSLModeDisable:
			query.Add("encrypt", "disable")
		case utils.SSLModeRequire:
			query.Add("encrypt", "true")
			// For "require" we trust the server certificate by default.
			query.Add("TrustServerCertificate", "true")
		default:
			// Fallback to disable for unsupported modes (e.g., verify-ca, verify-full).
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

	if m.sshClient != nil {
		if err := m.sshClient.Close(); err != nil {
			logger.Errorf("failed to close SSH client: %s", err)
		}
	}

	return nil
}

type mssqlSSHDialer struct {
	sshClient *ssh.Client
	host      string
}

func (d *mssqlSSHDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	return d.sshClient.DialContext(ctx, network, addr)
}

// HostName implements go-mssqldb's HostDialer interface, signalling that DNS
// resolution should happen on the remote (SSH) side rather than locally.
func (d *mssqlSSHDialer) HostName() string {
	return d.host
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
			stream.UpsertField(column.name, datatype, strings.EqualFold(column.isNullable, "YES"), false)

			if column.isPrimaryKey {
				stream.WithPrimaryKey(column.name)
			}
		}

		return stream, nil
	}
	stream, err := produceTableSchema(ctx, streamName)
	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("failed to produce schema context deadline exceeded: %s", ctx.Err())
		}
		return nil, fmt.Errorf("failed to process table[%s]: %s", streamName, err)
	}

	stream.WithSyncMode(types.FULLREFRESH, types.INCREMENTAL)
	if m.CDCSupported() {
		stream.UpsertField(CDCStartLSN, types.String, true, true)
		stream.UpsertField(CDCSeqVal, types.String, true, true)
		stream.WithSyncMode(types.CDC, types.STRICTCDC)
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
			if uuid, converted := formatUniqueIdentifierBytes(v); converted {
				return uuid, nil
			}
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
	case "time":
		return typeutils.ReformatTimeValue(value)
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
