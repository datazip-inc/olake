package driver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/binlog"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/errs"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/jmoiron/sqlx"
	"golang.org/x/crypto/ssh"

	// MySQL driver
	"github.com/go-sql-driver/mysql"
)

const (
	// MEDIUMINT's 3 bytes are the one MySQL integer width Go has no constant for.
	maxUint24 = 1<<24 - 1

	// minCDCInitialWaitTime is the minimum wait time in seconds for CDC sync.
	minCDCInitialWaitTime = 120
)

// MySQL represents the MySQL database driver
type MySQL struct {
	config     *Config
	client     *sqlx.DB
	sshClient  *ssh.Client
	CDCSupport bool // indicates if the MySQL instance supports CDC
	cdcConfig  CDC
	BinlogConn *binlog.Connection
	streams    []types.StreamInterface
	state      *types.State // reference to globally present state
	// effectiveTZ is the resolved timezone (e.g. for CDC binlog TimestampStringLocation).
	// Derived from config (jdbc_url_params.time_zone) or detected from the DB session.
	effectiveTZ *time.Location
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
		return fmt.Errorf("failed to validate config: %w", err)
	}

	if m.config.SSHConfig != nil && m.config.SSHConfig.Host != "" {
		logger.Info("Found SSH Configuration")
		m.sshClient, err = m.config.SSHConfig.SetupSSHConnection()
		if err != nil {
			return fmt.Errorf("failed to setup SSH connection: %w", err)
		}
	}

	var client *sqlx.DB
	if m.sshClient != nil {
		logger.Info("Connecting to MySQL via SSH tunnel")

		uri, err := m.config.URI()
		if err != nil {
			return fmt.Errorf("failed to setup config uri: %w", err)
		}

		cfg, err := mysql.ParseDSN(uri)
		if err != nil {
			return fmt.Errorf("failed to parse mysql DSN: %w", err)
		}

		// Allows mysql driver to use the SSH client to connect to the database
		cfg.Net = "mysqlTcp"
		mysql.RegisterDialContext(cfg.Net, func(_ context.Context, addr string) (net.Conn, error) {
			return m.sshClient.Dial("tcp", addr)
		})

		client, err = sqlx.Open("mysql", cfg.FormatDSN())
		if err != nil {
			return fmt.Errorf("failed to open tunneled database connection: %w", err)
		}
	} else {
		uri, err := m.config.URI()
		if err != nil {
			return fmt.Errorf("failed to setup config uri: %w", err)
		}

		client, err = sqlx.Open("mysql", uri)
		if err != nil {
			return fmt.Errorf("failed to open database connection: %w", err)
		}
	}
	// Test connection
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	// Set connection pool size
	client.SetMaxOpenConns(m.config.MaxThreads)
	if err := client.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	var resolved *time.Location
	if tzOverride := strings.TrimSpace(m.config.JDBCURLParams["time_zone"]); tzOverride != "" {
		resolved = resolveMySQLTimeZone(tzOverride, tzOverride, tzOverride)
	}
	if resolved == nil {
		query := jdbc.MySQLTimeZoneQuery()
		var sessionTimezone, globalTimezone, systemTimezone string
		if err := client.QueryRowxContext(ctx, query).Scan(&sessionTimezone, &globalTimezone, &systemTimezone); err != nil {
			logger.Warnf("mysql timezone detection failed; defaulting to UTC: %s", err)
			resolved = time.UTC
		} else {
			resolved = resolveMySQLTimeZone(sessionTimezone, globalTimezone, systemTimezone)
		}
	}
	m.effectiveTZ = resolved

	m.client = client

	found, _ := utils.IsOfType(m.config.UpdateMethod, "initial_wait_time")
	if found {
		logger.Info("Found CDC Configuration")
		cdc := &CDC{}
		if err := utils.Unmarshal(m.config.UpdateMethod, cdc); err != nil {
			return err
		}
		if cdc.InitialWaitTime < minCDCInitialWaitTime {
			logger.Warnf("initial_wait_time %d is below the minimum of %d seconds; using %d", cdc.InitialWaitTime, minCDCInitialWaitTime, minCDCInitialWaitTime)
			cdc.InitialWaitTime = minCDCInitialWaitTime
		}

		// Enable CDC support if binlog is configured
		cdcSupported, err := m.IsCDCSupported(ctx)
		if err != nil {
			return err
		}
		if !cdcSupported {
			return errs.Precondition(errs.CDCPreconditionFailed, codeCDCUnsupported, fmt.Errorf("failed to setup CDC: binlog is not configured correctly"))
		}

		m.CDCSupport = cdcSupported
		m.cdcConfig = *cdc
	}
	m.config.RetryCount = utils.Ternary(m.config.RetryCount <= 0, 1, m.config.RetryCount+1).(int)
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

func (m MySQL) GetStreamNames(ctx context.Context) ([]types.StreamID, error) {
	logger.Infof("Starting discover for MySQL database %s", m.config.Database)
	query := jdbc.MySQLDiscoverTablesQuery()
	rows, err := m.client.QueryContext(ctx, query, m.config.Database)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	var tableNames []types.StreamID
	for rows.Next() {
		var tableName, schemaName string
		if err := rows.Scan(&tableName, &schemaName); err != nil {
			return nil, fmt.Errorf("failed to scan table: %w", err)
		}
		tableNames = append(tableNames, types.StreamID{Namespace: schemaName, Name: tableName})
	}
	return tableNames, nil
}

func (m *MySQL) ProduceSchema(ctx context.Context, streamName types.StreamID) (*types.Stream, error) {
	produceTableSchema := func(ctx context.Context, streamName types.StreamID) (*types.Stream, error) {
		logger.Infof("producing type schema for stream [%s]", streamName)
		schemaName, tableName := streamName.Namespace, streamName.Name
		stream := types.NewStream(tableName, schemaName, nil)
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
			stream.WithCursorField(columnName)
			var datatype types.DataType
			if val, found := mysqlTypeToDataTypes[dataType]; found {
				datatype = val
			} else {
				logger.Warnf("Unsupported MySQL type '%s'for column '%s.%s', defaulting to String", dataType, streamName, columnName)
				datatype = types.String
			}
			stream.UpsertField(columnName, datatype, strings.EqualFold("yes", isNullable), false)

			// Mark primary keys
			if columnKey == "PRI" {
				stream.WithPrimaryKey(columnName)
			}
		}
		return stream, rows.Err()
	}
	stream, err := produceTableSchema(ctx, streamName)
	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("failed to produce schema context deadline exceeded: %w", ctx.Err())
		}
		return nil, fmt.Errorf("failed to process table[%s]: %w", streamName, err)
	}

	stream.WithSyncMode(types.FULLREFRESH, types.INCREMENTAL)
	if m.CDCSupported() {
		stream.UpsertField(binlog.CDCBinlogFileName, types.String, true, true)
		stream.UpsertField(binlog.CDCBinlogFilePos, types.Int64, true, true)
		stream.WithSyncMode(types.CDC, types.STRICTCDC)
	}

	return stream, nil
}

func (m *MySQL) dataTypeConverter(value interface{}, columnType string) (interface{}, error) {
	if value == nil {
		return nil, typeutils.ErrNullValue
	}

	// for special geospatial type, mysql returns non-utf8 binary data
	for _, geoType := range typeutils.GeospatialTypes {
		if strings.Contains(strings.ToLower(columnType), geoType) {
			// conversion to wkt from non-utf8 binary wkb
			return typeutils.ReformatGeoType(value)
		}
	}

	// The go-mysql binlog parser always returns integer values as their signed Go equivalents
	// (int8, int16, int32, int64) regardless of the MySQL UNSIGNED flag, sign-extending anything
	// with the high bit set. Masking the value back to the column's storage width undoes that:
	// it recovers the stored value without a signed-to-unsigned conversion that could overflow.
	if constants.LoadedStateVersion > 3 {
		value = stripSignExtension(value, strings.ToLower(columnType))
	} else {
		if strings.Contains(strings.ToLower(columnType), "unsigned") {
			columnType = strings.TrimSpace(strings.TrimPrefix(strings.ToLower(columnType), "unsigned "))
		}
	}

	olakeType := typeutils.ExtractAndMapColumnType(columnType, mysqlTypeToDataTypes)
	return typeutils.ReformatValue(olakeType, value)
}

// Close ensures proper cleanup
func (m *MySQL) Close() error {
	if m.client != nil {
		err := m.client.Close()
		if err != nil {
			logger.Errorf("failed to close connection with MySQL: %s", err)
		}
	}

	if m.sshClient != nil {
		err := m.sshClient.Close()
		if err != nil {
			logger.Errorf("failed to close SSH client: %s", err)
		}
	}
	return nil
}

// binlogRowMetadataFull reports whether the server emits full optional metadata in
// TableMapEvent. The variable does not exist before MySQL 8.0.1 or on MariaDB, in which
// case the answer is simply false — the decoder falls back to information_schema.
func (m *MySQL) binlogRowMetadataFull(ctx context.Context) bool {
	var name, value string
	if err := m.client.QueryRowxContext(ctx, jdbc.MySQLBinlogRowMetadataQuery()).Scan(&name, &value); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			logger.Warnf("failed to read binlog_row_metadata, assuming MINIMAL: %s", err)
		}
		return false
	}
	return strings.EqualFold(value, "FULL")
}

func (m *MySQL) IsCDCSupported(ctx context.Context) (bool, error) {
	// Permission check via SHOW MASTER STATUS / SHOW BINARY LOG STATUS
	if _, err := binlog.GetCurrentBinlogPosition(ctx, m.client); err != nil {
		return false, fmt.Errorf("failed to get binlog position: %w", err)
	}

	// go-mysql parses 5.7 binlogs correctly, so 5.7 is the supported floor. Below that,
	// fail here rather than with a decode error partway through a sync.
	flavor, major, minor, err := jdbc.MySQLVersion(ctx, m.client)
	if err != nil {
		return false, fmt.Errorf("failed to get MySQL version: %w", err)
	}
	if flavor == "MySQL" && (major < 5 || (major == 5 && minor < 7)) {
		logger.Warnf("MySQL %d.%d is below the supported CDC floor of 5.7", major, minor)
		return false, nil
	}

	// checkMySQLConfig checks a MySQL configuration value against an expected value
	checkMySQLConfig := func(ctx context.Context, query, expectedValue, warnMessage string) (bool, error) {
		var name, value string
		if err := m.client.QueryRowxContext(ctx, query).Scan(&name, &value); err != nil {
			return false, fmt.Errorf("failed to check %s: %w", name, err)
		}

		if strings.ToUpper(value) != expectedValue {
			logger.Warn(warnMessage)
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
		// At MINIMAL or NOBLOB the binlog carries only some columns per row, which the
		// decoder cannot map back to a complete record.
		{jdbc.MySQLBinlogRowImageQuery(), "FULL", "binlog_row_image is not set to FULL"},
	}

	for _, check := range configChecks {
		if ok, err := checkMySQLConfig(ctx, check.query, check.expectedValue, check.errMessage); err != nil || !ok {
			return ok, err
		}
	}

	// binlog_row_metadata=FULL lets the decoder read column names, ENUM/SET members,
	// charsets and signedness straight from the binlog. Without it — MySQL < 8.0.1, any
	// MariaDB, or the MINIMAL default — that metadata is rebuilt from information_schema,
	// which costs one query per table and cannot see a column rename that has not yet
	// reached the reader in the binlog stream.
	if !m.binlogRowMetadataFull(ctx) {
		logger.Warn("binlog_row_metadata is not FULL; falling back to information_schema for " +
			"column metadata. Set binlog_row_metadata=FULL (MySQL 8.0.1+) for best fidelity.")
	}

	return true, nil
}

// TODO: Add consistent timezone detection for CDC of other drivers as well.
// resolveMySQLTimeZone returns a *time.Location for interpreting TIMESTAMP values (e.g. CDC binlog).
// Precedence: session > global > system; "SYSTEM" is skipped so the next level is used.
// Invalid or missing IANA names fall back to UTC.
func resolveMySQLTimeZone(sessionTimezone, globalTimezone, systemTimezone string) *time.Location {
	// Strip surrounding quotes so "'Asia/Tokyo'" or "\"UTC\"" from jdbc params become valid IANA names.
	normalize := func(s string) string {
		return strings.Trim(strings.TrimSpace(s), `'"`)
	}

	session := normalize(sessionTimezone)
	global := normalize(globalTimezone)
	system := normalize(systemTimezone)

	var name string
	switch {
	case session != "" && !strings.EqualFold(session, "SYSTEM"):
		name = session
	case global != "" && !strings.EqualFold(global, "SYSTEM"):
		name = global
	default:
		name = system
	}

	offsetSeconds, ok := parseMySQLTimeZoneOffset(name)
	if constants.LoadedStateVersion > 2 && ok {
		return time.FixedZone(name, offsetSeconds)
	}

	loc, err := time.LoadLocation(name)
	if err != nil {
		logger.Warnf("failed to load mysql timezone location %s, falling back to UTC. Set jdbc_url_params.time_zone to override: %s", name, err)
		return time.UTC
	}
	return loc
}

// parseMySQTimeZoneOffset parses MySQL-style timezone offset. Returns offset in seconds and true, or 0, false.
func parseMySQLTimeZoneOffset(s string) (int, bool) {
	// MySql supports offsets ranging from -13:59 to +14:00
	mysqlOffsetRegex := regexp.MustCompile(`^([+-])(0?\d|1[0-4]):([0-5]\d)$`)

	s = strings.TrimSpace(s)
	matches := mysqlOffsetRegex.FindStringSubmatch(s)
	if matches == nil {
		return 0, false
	}
	signStr, hourStr, minuteStr := matches[1], matches[2], matches[3]

	hours, err1 := strconv.Atoi(hourStr)
	minutes, err2 := strconv.Atoi(minuteStr)
	if err1 != nil || err2 != nil || (hours == 14 && minutes > 0) || (signStr == "-" && hours == 14) {
		return 0, false
	}
	offsetSeconds := hours*3600 + minutes*60
	return utils.Ternary(signStr == "-", -offsetSeconds, offsetSeconds).(int), true
}

// stripSignExtension masks an UNSIGNED column's value back to its storage width, undoing the sign
// extension the binlog parser applies. The result comes back in the narrowest signed Go type that
// holds the column's whole range, so no case widens further than its own values need.
// UNSIGNED BIGINT is absent on purpose: it has no spare width, so its bits are already final.
// TODO: olake has no uint64 data type, so UNSIGNED BIGINT above MaxInt64 stays wrapped negative.
func stripSignExtension(value any, columnType string) any {
	switch columnType {
	case "unsigned tinyint":
		if v, ok := value.(int8); ok {
			return int16(v) & math.MaxUint8
		}
	case "unsigned smallint":
		if v, ok := value.(int16); ok {
			return int32(v) & math.MaxUint16
		}
	case "unsigned mediumint":
		if v, ok := value.(int32); ok {
			return v & maxUint24
		}
	case "unsigned int", "unsigned integer":
		if v, ok := value.(int32); ok {
			return int64(v) & math.MaxUint32
		}
	}
	return value
}
