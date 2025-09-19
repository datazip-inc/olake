package jdbc

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/jmoiron/sqlx"
)

// QuoteIdentifier returns the properly quoted identifier based on database driver
func QuoteIdentifier(identifier string, driver constants.DriverType) string {
	switch driver {
	case constants.MySQL:
		return fmt.Sprintf("`%s`", identifier)
	case constants.Postgres:
		return fmt.Sprintf(`"%s"`, identifier)
	case constants.Oracle:
		return fmt.Sprintf("%q", identifier)
	default:
		return identifier
	}
}

// QuoteTable returns the properly quoted schema.table combination
func QuoteTable(schema, table string, driver constants.DriverType) string {
	return fmt.Sprintf("%s.%s",
		QuoteIdentifier(schema, driver),
		QuoteIdentifier(table, driver))
}

// QuoteColumns returns a slice of quoted column names
func QuoteColumns(columns []string, driver constants.DriverType) []string {
	quoted := make([]string, len(columns))
	for i, col := range columns {
		quoted[i] = QuoteIdentifier(col, driver)
	}
	return quoted
}

// MinMaxQuery returns the query to fetch MIN and MAX values of a column in a Postgres table
func MinMaxQuery(stream types.StreamInterface, column string) string {
	quotedColumn := QuoteIdentifier(column, constants.Postgres)
	quotedTable := QuoteTable(stream.Namespace(), stream.Name(), constants.Postgres)
	return fmt.Sprintf(
		`SELECT MIN(%[1]s) AS min_value, MAX(%[1]s) AS max_value FROM %[2]s`,
		quotedColumn, quotedTable,
	)
}

// NextChunkEndQuery returns the query to calculate the next chunk boundary
// Example:
// Input:
//
//	stream.Namespace() = "mydb"
//	stream.Name() = "users"
//	columns = []string{"id", "created_at"}
//	chunkSize = 1000
//
// Output:
//
//	SELECT CONCAT_WS(',', id, created_at) AS key_str FROM (
//	  SELECT (',', id, created_at)
//	  FROM `mydb`.`users`
//	  WHERE (`id` > ?) OR (`id` = ? AND `created_at` > ?)
//	  ORDER BY id, created_at
//	  LIMIT 1 OFFSET 1000
//	) AS subquery
func NextChunkEndQuery(stream types.StreamInterface, columns []string, chunkSize int64) string {
	quotedCols := QuoteColumns(columns, constants.MySQL)
	quotedTable := QuoteTable(stream.Namespace(), stream.Name(), constants.MySQL)

	var query strings.Builder
	// SELECT with quoted and concatenated values
	fmt.Fprintf(&query, "SELECT CONCAT_WS(',', %s) AS key_str FROM (SELECT %s FROM %s",
		strings.Join(quotedCols, ", "),
		strings.Join(quotedCols, ", "),
		quotedTable,
	)
	// WHERE clause for lexicographic "greater than"
	query.WriteString(" WHERE ")
	// TODO: Embed primary key columns here directly
	for currentColIndex := 0; currentColIndex < len(columns); currentColIndex++ {
		if currentColIndex > 0 {
			query.WriteString(" OR ")
		}
		query.WriteString("(")
		for equalityColIndex := 0; equalityColIndex < currentColIndex; equalityColIndex++ {
			fmt.Fprintf(&query, "%s = ? AND ", quotedCols[equalityColIndex])
		}
		fmt.Fprintf(&query, "%s > ?", quotedCols[currentColIndex])
		query.WriteString(")")
	}
	// ORDER + LIMIT
	fmt.Fprintf(&query, " ORDER BY %s", strings.Join(quotedCols, ", "))
	fmt.Fprintf(&query, " LIMIT 1 OFFSET %d) AS subquery", chunkSize)
	return query.String()
}

// PostgreSQL-Specific Queries
// TODO: Rewrite queries for taking vars as arguments while execution.
// PostgresRowCountQuery returns the query to fetch the estimated row count in PostgreSQL
func PostgresRowCountQuery(stream types.StreamInterface) string {
	return fmt.Sprintf(`SELECT reltuples::bigint AS approx_row_count FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = '%s' AND n.nspname = '%s';`, stream.Name(), stream.Namespace())
}

// PostgresBlockSizeQuery returns the query to fetch the block size in PostgreSQL
func PostgresBlockSizeQuery() string {
	return `SHOW block_size`
}

// PostgresRelPageCount returns the query to fetch relation page count in PostgreSQL
func PostgresRelPageCount(stream types.StreamInterface) string {
	return fmt.Sprintf(`SELECT relpages FROM pg_class WHERE relname = '%s' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '%s')`, stream.Name(), stream.Namespace())
}

// PostgresWalLSNQuery returns the query to fetch the current WAL LSN in PostgreSQL
func PostgresWalLSNQuery() string {
	return `SELECT pg_current_wal_lsn()::text::pg_lsn`
}

// PostgresNextChunkEndQuery generates a SQL query to fetch the maximum value of a specified column
func PostgresNextChunkEndQuery(stream types.StreamInterface, filterColumn string, filterValue interface{}, batchSize int, dataType types.DataType) string {
	quotedColumn := QuoteIdentifier(filterColumn, constants.Postgres)
	quotedTable := QuoteTable(stream.Namespace(), stream.Name(), constants.Postgres)
	var baseCond string
	if dataType == types.String {
		baseCond = fmt.Sprintf(`%s > '%s'`, quotedColumn, strings.ReplaceAll(fmt.Sprintf("%v", filterValue), "'", "''"))
	} else {
		baseCond = fmt.Sprintf(`%s > %v`, quotedColumn, filterValue)
	}
	return fmt.Sprintf(`SELECT MAX(%s) FROM (SELECT %s FROM %s WHERE %s ORDER BY %s ASC LIMIT %d) AS T`,
		quotedColumn, quotedColumn, quotedTable, baseCond, quotedColumn, batchSize)
}

// PostgresBuildSplitScanQuery builds a chunk scan query for PostgreSQL
func PostgresChunkScanQuery(stream types.StreamInterface, filterColumn string, chunk types.Chunk, filter string, dataType types.DataType) string {
	quotedFilterColumn := QuoteIdentifier(filterColumn, constants.Postgres)
	quotedTable := QuoteTable(stream.Namespace(), stream.Name(), constants.Postgres)

	formatValue := func(val interface{}) string {
		if dataType == types.String && val != nil {
			return fmt.Sprintf("'%s'", strings.ReplaceAll(fmt.Sprintf("%v", val), "'", "''"))
		}
		return fmt.Sprintf("%v", val)
	}

	chunkCond := ""
	if chunk.Min != nil && chunk.Max != nil {
		chunkCond = fmt.Sprintf("%s >= %s AND %s < %s", quotedFilterColumn, formatValue(chunk.Min), quotedFilterColumn, formatValue(chunk.Max))
	} else if chunk.Min != nil {
		chunkCond = fmt.Sprintf("%s >= %s", quotedFilterColumn, formatValue(chunk.Min))
	} else if chunk.Max != nil {
		chunkCond = fmt.Sprintf("%s < %s", quotedFilterColumn, formatValue(chunk.Max))
	}

	chunkCond = utils.Ternary(filter != "" && chunkCond != "", fmt.Sprintf("(%s) AND (%s)", chunkCond, filter), chunkCond).(string)
	return fmt.Sprintf(`SELECT * FROM %s WHERE %s`, quotedTable, chunkCond)
}

// MySQL-Specific Queries
// buildChunkConditionMySQL builds the condition for a chunk in MySQL
func buildChunkConditionMySQL(filterColumns []string, chunk types.Chunk, extraFilter string) string {
	quotedCols := QuoteColumns(filterColumns, constants.MySQL)
	colTuple := "(" + strings.Join(quotedCols, ", ") + ")"

	buildSQLTuple := func(val any) string {
		parts := strings.Split(val.(string), ",")
		for i, part := range parts {
			parts[i] = fmt.Sprintf("'%s'", strings.TrimSpace(part))
		}
		return strings.Join(parts, ", ")
	}
	chunkCond := ""
	switch {
	case chunk.Min != nil && chunk.Max != nil:
		chunkCond = fmt.Sprintf("%s >= (%s) AND %s < (%s)", colTuple, buildSQLTuple(chunk.Min), colTuple, buildSQLTuple(chunk.Max))
	case chunk.Min != nil:
		chunkCond = fmt.Sprintf("%s >= (%s)", colTuple, buildSQLTuple(chunk.Min))
	case chunk.Max != nil:
		chunkCond = fmt.Sprintf("%s < (%s)", colTuple, buildSQLTuple(chunk.Max))
	}
	// Both filter and chunk cond both should exist
	if extraFilter != "" && chunkCond != "" {
		return fmt.Sprintf("(%s) AND (%s)", chunkCond, extraFilter)
	}
	return chunkCond
}

// MysqlLimitOffsetScanQuery is used to get the rows
func MysqlLimitOffsetScanQuery(stream types.StreamInterface, chunk types.Chunk, filter string) string {
	quotedTable := QuoteTable(stream.Namespace(), stream.Name(), constants.MySQL)
	query := fmt.Sprintf("SELECT * FROM %s", quotedTable)
	query = utils.Ternary(filter == "", query, fmt.Sprintf("%s WHERE %s", query, filter)).(string)
	if chunk.Min == nil {
		maxVal, _ := strconv.ParseUint(chunk.Max.(string), 10, 64)
		query = fmt.Sprintf("%s LIMIT %d", query, maxVal)
	} else if chunk.Min != nil && chunk.Max != nil {
		minVal, _ := strconv.ParseUint(chunk.Min.(string), 10, 64)
		maxVal, _ := strconv.ParseUint(chunk.Max.(string), 10, 64)
		query = fmt.Sprintf("%s LIMIT %d OFFSET %d", query, maxVal-minVal, minVal)
	} else {
		minVal, _ := strconv.ParseUint(chunk.Min.(string), 10, 64)
		maxNum := ^uint64(0)
		query = fmt.Sprintf("%s LIMIT %d OFFSET %d", query, maxNum, minVal)
	}
	return query
}

// MySQLWithoutState builds a chunk scan query for MySql
func MysqlChunkScanQuery(stream types.StreamInterface, filterColumns []string, chunk types.Chunk, extraFilter string) string {
	condition := buildChunkConditionMySQL(filterColumns, chunk, extraFilter)
	quotedTable := QuoteTable(stream.Namespace(), stream.Name(), constants.MySQL)
	return fmt.Sprintf("SELECT * FROM %s WHERE %s", quotedTable, condition)
}

// MinMaxQueryMySQL returns the query to fetch MIN and MAX values of a column in a MySQL table
func MinMaxQueryMySQL(stream types.StreamInterface, columns []string) string {
	quotedCols := QuoteColumns(columns, constants.MySQL)
	quotedTable := QuoteTable(stream.Namespace(), stream.Name(), constants.MySQL)
	concatCols := fmt.Sprintf("CONCAT_WS(',', %s)", strings.Join(quotedCols, ", "))

	orderAsc := strings.Join(quotedCols, ", ")
	descCols := make([]string, len(quotedCols))
	for i, col := range quotedCols {
		descCols[i] = col + " DESC"
	}
	orderDesc := strings.Join(descCols, ", ")
	return fmt.Sprintf(`
    SELECT
        (SELECT %s FROM %s ORDER BY %s LIMIT 1) AS min_value,
        (SELECT %s FROM %s ORDER BY %s LIMIT 1) AS max_value
    `,
		concatCols, quotedTable, orderAsc,
		concatCols, quotedTable, orderDesc,
	)
}

// MySQLDiscoverTablesQuery returns the query to discover tables in a MySQL database
func MySQLDiscoverTablesQuery() string {
	return `
		SELECT 
			TABLE_NAME, 
			TABLE_SCHEMA 
		FROM 
			INFORMATION_SCHEMA.TABLES 
		WHERE 
			TABLE_SCHEMA = ? 
			AND TABLE_TYPE = 'BASE TABLE'
	`
}

// MySQLTableSchemaQuery returns the query to fetch schema information for a table in MySQL
func MySQLTableSchemaQuery() string {
	return `
		SELECT 
			COLUMN_NAME, 
			COLUMN_TYPE,
			DATA_TYPE, 
			IS_NULLABLE,
			COLUMN_KEY
		FROM 
			INFORMATION_SCHEMA.COLUMNS 
		WHERE 
			TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY 
			ORDINAL_POSITION
	`
}

// MySQLPrimaryKeyQuery returns the query to fetch the primary key column of a table in MySQL
func MySQLPrimaryKeyQuery() string {
	return `
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
        WHERE TABLE_SCHEMA = DATABASE() 
        AND TABLE_NAME = ? 
        AND CONSTRAINT_NAME = 'PRIMARY' 
        LIMIT 1
	`
}

// MySQLTableRowStatsQuery returns the query to fetch the estimated row count and average row size of a table in MySQL
func MySQLTableRowStatsQuery() string {
	return `
		SELECT TABLE_ROWS,
		CEIL(data_length / NULLIF(table_rows, 0)) AS avg_row_bytes
		FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_SCHEMA = DATABASE()
		AND TABLE_NAME = ?
	`
}

// MySQLMasterStatusQuery returns the query to fetch the current binlog position in MySQL: mysql v8.3 and below
func MySQLMasterStatusQuery() string {
	return "SHOW MASTER STATUS"
}

// MySQLMasterStatusQuery returns the query to fetch the current binlog position in MySQL: mysql v8.4 and above
func MySQLMasterStatusQueryNew() string {
	return "SHOW BINARY LOG STATUS"
}

// MySQLLogBinQuery returns the query to fetch the log_bin variable in MySQL
func MySQLLogBinQuery() string {
	return "SHOW VARIABLES LIKE 'log_bin'"
}

// MySQLBinlogFormatQuery returns the query to fetch the binlog_format variable in MySQL
func MySQLBinlogFormatQuery() string {
	return "SHOW VARIABLES LIKE 'binlog_format'"
}

// MySQLBinlogRowMetadataQuery returns the query to fetch the binlog_row_metadata variable in MySQL
func MySQLBinlogRowMetadataQuery() string {
	return "SHOW VARIABLES LIKE 'binlog_row_metadata'"
}

// MySQLTableColumnsQuery returns the query to fetch column names of a table in MySQL
func MySQLTableColumnsQuery() string {
	return `
		SELECT COLUMN_NAME 
		FROM INFORMATION_SCHEMA.COLUMNS 
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? 
		ORDER BY ORDINAL_POSITION
	`
}

// MySQLVersion returns the version of the MySQL server
// It returns the major and minor version of the MySQL server
func MySQLVersion(client *sqlx.DB) (int, int, error) {
	var version string
	err := client.QueryRow("SELECT @@version").Scan(&version)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get MySQL version: %s", err)
	}

	parts := strings.Split(version, ".")
	if len(parts) < 2 {
		return 0, 0, fmt.Errorf("invalid version format")
	}
	majorVersion, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid major version: %s", err)
	}

	minorVersion, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid minor version: %s", err)
	}

	return majorVersion, minorVersion, nil
}

func WithIsolation(ctx context.Context, client *sqlx.DB, fn func(tx *sql.Tx) error) error {
	tx, err := client.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %s", err)
	}
	defer func() {
		if rerr := tx.Rollback(); rerr != nil && rerr != sql.ErrTxDone {
			fmt.Printf("transaction rollback failed: %s", rerr)
		}
	}()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// OracleDB Specific Queries

// OracleTableDiscoveryQuery returns the query to fetch the username and table name of all the tables which the current user has access to in OracleDB
func OracleTableDiscoveryQuery() string {
	return `SELECT owner, table_name FROM all_tables WHERE owner NOT IN (SELECT username FROM all_users WHERE oracle_maintained = 'Y')`
}

// OracleTableDetailsQuery returns the query to fetch the details of a table in OracleDB
func OracleTableDetailsQuery(schemaName, tableName string) string {
	return fmt.Sprintf("SELECT column_name, data_type, nullable, data_precision, data_scale FROM all_tab_columns WHERE owner = '%s' AND table_name = '%s'", schemaName, tableName)
}

// OraclePrimaryKeyQuery returns the query to fetch all the primary key columns of a table in OracleDB
func OraclePrimaryKeyColummsQuery(schemaName, tableName string) string {
	return fmt.Sprintf(`SELECT cols.column_name FROM all_constraints cons, all_cons_columns cols WHERE cons.constraint_type = 'P' AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner AND cons.owner = '%s' AND cols.table_name = '%s'`, schemaName, tableName)
}

// OracleChunkScanQuery returns the query to fetch the rows of a table in OracleDB
func OracleChunkScanQuery(stream types.StreamInterface, chunk types.Chunk, filter string) string {
	quotedTable := QuoteTable(stream.Namespace(), stream.Name(), constants.Oracle)
	currentSCN := strings.Split(chunk.Min.(string), ",")[0]
	chunkMin := strings.Split(chunk.Min.(string), ",")[1]

	filterClause := utils.Ternary(filter == "", "", " AND ("+filter+")").(string)

	if chunk.Max != nil {
		chunkMax := chunk.Max.(string)
		return fmt.Sprintf("SELECT * FROM %s AS OF SCN %s WHERE ROWID >= '%v' AND ROWID < '%v' %s",
			quotedTable, currentSCN, chunkMin, chunkMax, filterClause)
	}
	return fmt.Sprintf("SELECT * FROM %s AS OF SCN %s WHERE ROWID >= '%v' %s",
		quotedTable, currentSCN, chunkMin, filterClause)
}

// OracleTableSizeQuery returns the query to fetch the size of a table in bytes in OracleDB
func OracleBlockSizeQuery() string {
	return `SELECT CEIL(BYTES / NULLIF(BLOCKS, 0)) FROM user_segments WHERE BLOCKS IS NOT NULL AND ROWNUM =1`
}

// OracleCurrentSCNQuery returns the query to fetch the current SCN in OracleDB
func OracleCurrentSCNQuery() string {
	return `SELECT TO_CHAR(DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER) AS SCN_STR FROM DUAL`
}

// OracleEmptyCheckQuery returns the query to check if a table is empty in OracleDB
func OracleEmptyCheckQuery(stream types.StreamInterface) string {
	quotedTable := QuoteTable(stream.Namespace(), stream.Name(), constants.Oracle)
	return fmt.Sprintf("SELECT 1 FROM %s WHERE ROWNUM = 1", quotedTable)
}

// OracleTaskCreationQuery returns the query to create a task in OracleDB
func OracleTaskCreationQuery(taskName string) string {
	return fmt.Sprintf(`BEGIN DBMS_PARALLEL_EXECUTE.create_task('%s'); END;`, taskName)
}

// OracleChunkCreationQuery returns the query to make chunks in OracleDB using DBMS_PARALLEL_EXECUTE
func OracleChunkCreationQuery(stream types.StreamInterface, blocksPerChunk int64, taskName string) string {
	return fmt.Sprintf(`BEGIN
  						DBMS_PARALLEL_EXECUTE.create_chunks_by_rowid(
    					task_name   => '%s',
    					table_owner => '%s',
    					table_name  => '%s',	
    					by_row      => FALSE,
    					chunk_size  => %d
  						);
						END;`,
		taskName, stream.Namespace(), stream.Name(), blocksPerChunk,
	)
}

// OracleChunkTaskCleanerQuery returns the query to clean up a chunk task in OracleDB
func OracleChunkTaskCleanerQuery(taskName string) string {
	return fmt.Sprintf(`BEGIN DBMS_PARALLEL_EXECUTE.drop_task('%s'); END;`, taskName)
}

// OracleChunkRetrievalQuery returns the query to retrieve chunks from DBMS_PARALLEL_EXECUTE in OracleDB
func OracleChunkRetrievalQuery(taskName string) string {
	return fmt.Sprintf(`SELECT chunk_id, start_rowid, end_rowid FROM user_parallel_execute_chunks WHERE task_name = '%s' ORDER BY chunk_id`, taskName)
}

// OracleIncrementalValueFormatter is used to format the value of the cursor field for Oracle incremental sync, mainly because of the various timestamp formats
func OracleIncrementalValueFormatter(cursorField, argumentPlaceholder string, lastCursorValue any, opts IncrementalConditionOptions) (string, any, error) {
	// Get the datatype of the cursor field from streams
	stream := opts.Stream
	// remove cursorField conversion to lower case once column normalization is based on writer side
	datatype, err := stream.Self().Stream.Schema.GetType(cursorField)
	if err != nil {
		return "", nil, fmt.Errorf("cursor field %s not found in schema: %s", cursorField, err)
	}

	isTimestamp := strings.Contains(string(datatype), "timestamp")
	formattedValue, err := typeutils.ReformatValue(datatype, lastCursorValue)
	if err != nil {
		return "", nil, fmt.Errorf("failed to reformat value %v of type %T: %s", lastCursorValue, lastCursorValue, err)
	}

	query := fmt.Sprintf("SELECT DATA_TYPE FROM ALL_TAB_COLUMNS WHERE OWNER = '%s' AND TABLE_NAME = '%s' AND COLUMN_NAME = '%s'", stream.Namespace(), stream.Name(), cursorField)
	err = opts.Client.QueryRow(query).Scan(&datatype)
	if err != nil {
		return "", nil, fmt.Errorf("failed to get column datatype: %s", err)
	}
	// if the cursor field is a timestamp and not timezone aware, we need to cast the value as timestamp
	quotedCol := QuoteIdentifier(cursorField, constants.Oracle)
	if isTimestamp && !strings.Contains(string(datatype), "TIME ZONE") {
		return fmt.Sprintf("%s > CAST(%s AS TIMESTAMP)", quotedCol, argumentPlaceholder), formattedValue, nil
	}
	return fmt.Sprintf("%s > %s", quotedCol, argumentPlaceholder), formattedValue, nil
}

// ParseFilter converts a filter string to a valid SQL WHERE condition
func SQLFilter(stream types.StreamInterface, driver string) (string, error) {
	buildCondition := func(cond types.Condition, driver string) (string, error) {
		var driverType constants.DriverType
		switch driver {
		case "mysql":
			driverType = constants.MySQL
		case "postgres":
			driverType = constants.Postgres
		case "oracle":
			driverType = constants.Oracle
		default:
			driverType = constants.Postgres // default fallback
		}

		quotedColumn := QuoteIdentifier(cond.Column, driverType)

		// Handle unquoted null value
		if cond.Value == "null" {
			switch cond.Operator {
			case "=":
				return fmt.Sprintf("%s IS NULL", quotedColumn), nil
			case "!=":
				return fmt.Sprintf("%s IS NOT NULL", quotedColumn), nil
			default:
				return fmt.Sprintf("%s %s NULL", quotedColumn, cond.Operator), nil
			}
		}

		// Parse and format value
		value := cond.Value
		if strings.HasPrefix(value, "\"") && strings.HasSuffix(value, "\"") {
			// Handle quoted strings
			unquoted := value[1 : len(value)-1]
			escaped := strings.ReplaceAll(unquoted, "'", "''")
			value = fmt.Sprintf("'%s'", escaped)
		} else {
			_, err := strconv.ParseFloat(value, 64)
			booleanValue := strings.EqualFold(value, "true") || strings.EqualFold(value, "false")
			if err != nil && !booleanValue {
				escaped := strings.ReplaceAll(value, "'", "''")
				value = fmt.Sprintf("'%s'", escaped)
			}
		}

		return fmt.Sprintf("%s %s %s", quotedColumn, cond.Operator, value), nil
	}

	filter, err := stream.GetFilter()
	if err != nil {
		return "", fmt.Errorf("failed to parse stream filter: %s", err)
	}

	switch {
	case len(filter.Conditions) == 0:
		return "", nil // No conditions, return empty string
	case len(filter.Conditions) == 1:
		return buildCondition(filter.Conditions[0], driver)
	default:
		// for size 2
		conditions := make([]string, 0, len(filter.Conditions))
		err := utils.ForEach(filter.Conditions, func(cond types.Condition) error {
			formatted, err := buildCondition(cond, driver)
			if err != nil {
				return err
			}
			conditions = append(conditions, formatted)
			return nil
		})
		return strings.Join(conditions, fmt.Sprintf(" %s ", filter.LogicalOperator)), err
	}
}

// IncrementalConditionOptions contains options for building incremental conditions
type IncrementalConditionOptions struct {
	Driver constants.DriverType
	Stream types.StreamInterface
	State  *types.State
	Client *sqlx.DB
	Filter string
}

// BuildIncrementalQuery generates the incremental query SQL based on driver type
func BuildIncrementalQuery(opts IncrementalConditionOptions) (string, []any, error) {
	primaryCursor, secondaryCursor := opts.Stream.Cursor()
	lastPrimaryCursorValue := opts.State.GetCursor(opts.Stream.Self(), primaryCursor)
	lastSecondaryCursorValue := opts.State.GetCursor(opts.Stream.Self(), secondaryCursor)
	// cursor values cannot contain only nil values
	if lastPrimaryCursorValue == nil {
		logger.Warnf("last primary cursor value is nil for stream[%s]", opts.Stream.ID())
	}
	if secondaryCursor != "" && lastSecondaryCursorValue == nil {
		logger.Warnf("last secondary cursor value is nil for stream[%s]", opts.Stream.ID())
	}

	// Get placeholder based on driver
	var placeholder func(int) string
	switch opts.Driver {
	case constants.MySQL:
		placeholder = func(_ int) string { return "?" }
	case constants.Postgres:
		placeholder = func(i int) string { return fmt.Sprintf("$%d", i) }
	case constants.Oracle:
		placeholder = func(i int) string { return fmt.Sprintf(":%d", i) }
	default:
		return "", nil, fmt.Errorf("unsupported driver: %s", string(opts.Driver))
	}

	// buildCursorCondition creates the SQL condition for incremental queries based on cursor fields.
	buildCursorCondition := func(cursorField string, lastCursorValue any, argumentPosition int) (string, any, error) {
		if opts.Driver == constants.Oracle {
			return OracleIncrementalValueFormatter(cursorField, placeholder(argumentPosition), lastCursorValue, opts)
		}
		quotedColumn := QuoteIdentifier(cursorField, opts.Driver)
		return fmt.Sprintf("%s > %s", quotedColumn, placeholder(argumentPosition)), lastCursorValue, nil
	}

	// Build primary cursor condition
	incrementalCondition, primaryArg, err := buildCursorCondition(primaryCursor, lastPrimaryCursorValue, 1)
	if err != nil {
		return "", nil, fmt.Errorf("failed to format primary cursor value: %s", err)
	}
	queryArgs := []any{primaryArg}

	// Add secondary cursor condition if present
	if secondaryCursor != "" && lastSecondaryCursorValue != nil {
		secondaryCondition, secondaryArg, err := buildCursorCondition(secondaryCursor, lastSecondaryCursorValue, 2)
		if err != nil {
			return "", nil, fmt.Errorf("failed to format secondary cursor value: %s", err)
		}
		quotedPrimaryCursor := QuoteIdentifier(primaryCursor, opts.Driver)
		incrementalCondition = fmt.Sprintf("%s OR (%s IS NULL AND %s)",
			incrementalCondition, quotedPrimaryCursor, secondaryCondition)
		queryArgs = append(queryArgs, secondaryArg)
	}

	finalFilter := utils.Ternary(opts.Filter != "", fmt.Sprintf("(%s) AND (%s)", opts.Filter, incrementalCondition), incrementalCondition).(string)
	logger.Infof("Starting incremental sync for stream[%s] with filter: %s and args: %v", opts.Stream.ID(), finalFilter, queryArgs)

	// Use QuoteTable helper function for consistent table quoting
	quotedTable := QuoteTable(opts.Stream.Namespace(), opts.Stream.Name(), opts.Driver)
	incrementalQuery := fmt.Sprintf("SELECT * FROM %s WHERE %s", quotedTable, finalFilter)

	return incrementalQuery, queryArgs, nil
}
