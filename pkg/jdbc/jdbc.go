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
		return fmt.Sprintf("`%s`", identifier) // MySQL uses backticks for quoting identifiers
	case constants.Postgres:
		return fmt.Sprintf("%q", identifier)
	case constants.Oracle:
		return fmt.Sprintf("%q", identifier)
	case constants.MSSQL:
		return fmt.Sprintf("[%s]", identifier)
	default:
		return identifier
	}
}

// GetPlaceholder returns the appropriate placeholder for the given driver
func GetPlaceholder(driver constants.DriverType) func(int) string {
	switch driver {
	case constants.MySQL:
		return func(_ int) string { return "?" }
	case constants.Postgres:
		return func(i int) string { return fmt.Sprintf("$%d", i) }
	case constants.Oracle:
		return func(i int) string { return fmt.Sprintf(":%d", i) }
	case constants.MSSQL:
		return func(i int) string { return fmt.Sprintf("@p%d", i) }
	default:
		return func(_ int) string { return "?" }
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

// PostgresPartitionPages returns total relpages for each partition and the parent table.
// This can be used to dynamically adjust chunk sizes based on partition distribution.
func PostgresPartitionPages(stream types.StreamInterface) string {
	return fmt.Sprintf(`
        WITH parent AS (
            SELECT c.oid AS parent_oid
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = '%s'
                AND c.relname = '%s'
        ),
        partitions AS (
            SELECT
                child.relname AS name,
                CEIL(1.05 * (pg_relation_size(child.oid) / current_setting('block_size')::int)) AS pages
            FROM pg_inherits i
            JOIN pg_class child ON child.oid = i.inhrelid
            JOIN parent p ON p.parent_oid = i.inhparent
            
            UNION ALL
            
            SELECT
                c.relname AS name,
                CEIL(1.05 * (pg_relation_size(c.oid) / current_setting('block_size')::int)) AS pages
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = '%s'
                AND c.relname = '%s'
        )
        SELECT 
            name, 
            pages 
        FROM partitions 
        ORDER BY pages DESC;
    `,
		stream.Namespace(),
		stream.Name(),
		stream.Namespace(),
		stream.Name(),
	)
}

// PostgresIsPartitionedQuery returns a SQL query that checks whether a table is partitioned.
// It counts how many partitions exist under the given parent table in the specified schema.
func PostgresIsPartitionedQuery(stream types.StreamInterface) string {
	return fmt.Sprintf(`
        SELECT 
            COUNT(i.inhrelid)
        FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhparent
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = '%s'
            AND c.relname = '%s';
    `,
		stream.Namespace(),
		stream.Name(),
	)
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
func PostgresNextChunkEndQuery(stream types.StreamInterface, filterColumn string, filterValue interface{}) string {
	quotedColumn := QuoteIdentifier(filterColumn, constants.Postgres)
	quotedTable := QuoteTable(stream.Namespace(), stream.Name(), constants.Postgres)
	baseCond := fmt.Sprintf(`%s > %v`, quotedColumn, filterValue)
	return fmt.Sprintf(`SELECT MAX(%s) FROM (SELECT %s FROM %s WHERE %s ORDER BY %s ASC LIMIT %d) AS T`,
		quotedColumn, quotedColumn, quotedTable, baseCond, quotedColumn, 10000)
}

// PostgresBuildSplitScanQuery builds a chunk scan query for PostgreSQL
func PostgresChunkScanQuery(stream types.StreamInterface, filterColumn string, chunk types.Chunk, filter string) string {
	quotedFilterColumn := QuoteIdentifier(filterColumn, constants.Postgres)
	quotedTable := QuoteTable(stream.Namespace(), stream.Name(), constants.Postgres)

	chunkCond := ""
	if chunk.Min != nil && chunk.Max != nil {
		chunkCond = fmt.Sprintf("%s >= %v AND %s < %v", quotedFilterColumn, chunk.Min, quotedFilterColumn, chunk.Max)
	} else if chunk.Min != nil {
		chunkCond = fmt.Sprintf("%s >= %v", quotedFilterColumn, chunk.Min)
	} else if chunk.Max != nil {
		chunkCond = fmt.Sprintf("%s < %v", quotedFilterColumn, chunk.Max)
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

// buildChunkConditionMSSQL builds a WHERE condition for scanning a chunk in MSSQL.
// It emulates lexicographic ranges for one or more ordering columns because SQL Server
// does not support tuple comparisons like (col1, col2) >= (...).
func buildChunkConditionMSSQL(quotedColumns []string, chunk types.Chunk, extraFilter string) string {
	// splitBoundaryValues turns the chunk boundary string into a list of values
	splitBoundaryValues := func(boundary any) []string {
		if boundary == nil {
			return nil
		}
		values := strings.Split(boundary.(string), ",")
		for idx, value := range values {
			values[idx] = strings.TrimSpace(value)
		}
		return values
	}

	// formatSqlLiteral returns a SQL literal for the given boundary value.
	// It avoids quoting numeric/boolean values (to prevent type conversion issues)
	// and only quotes actual strings.
	formatSqlLiteral := func(value string) string {
		if value == "" {
			return "''"
		}

		// Integers stay unquoted.
		if _, err := strconv.ParseInt(value, 10, 64); err == nil {
			return value
		}

		// Floats stay unquoted.
		if _, err := strconv.ParseFloat(value, 64); err == nil {
			return value
		}

		// Boolean values stay unquoted.
		if value == "true" || value == "false" {
			return value
		}

		// Everything else is treated as a string and escaped.
		escaped := strings.ReplaceAll(value, "'", "''")
		return fmt.Sprintf("'%s'", escaped)
	}

	// buildLowerBoundCondition builds a lexicographic ">= lowerBound" condition.
	// For columns (c1, c2, c3) and values (m1, m2, m3) it produces:
	//   (c1 > m1) OR
	//   (c1 = m1 AND c2 > m2) OR
	//   (c1 = m1 AND c2 = m2 AND c3 >= m3)
	buildLowerBoundCondition := func(lowerBoundValues []string) string {
		if lowerBoundValues == nil {
			return ""
		}

		orGroups := make([]string, 0, len(quotedColumns))

		for columnIndex := range quotedColumns {
			andConditions := make([]string, 0, columnIndex+1)

			// Prefix columns must match exactly: c1 = m1 AND c2 = m2 ...
			for prefixIndex := 0; prefixIndex < columnIndex; prefixIndex++ {
				if prefixIndex < len(lowerBoundValues) {
					andConditions = append(
						andConditions,
						fmt.Sprintf("%s = %s", quotedColumns[prefixIndex], formatSqlLiteral(lowerBoundValues[prefixIndex])),
					)
				}
			}

			// For the current column, use ">" except on the last column where we use ">=".
			comparisonOp := ">"
			if columnIndex == len(quotedColumns)-1 {
				comparisonOp = ">="
			}

			if columnIndex < len(lowerBoundValues) {
				andConditions = append(
					andConditions,
					fmt.Sprintf("%s %s %s", quotedColumns[columnIndex], comparisonOp, formatSqlLiteral(lowerBoundValues[columnIndex])),
				)
			}

			if len(andConditions) > 0 {
				orGroups = append(orGroups, "("+strings.Join(andConditions, " AND ")+")")
			}
		}

		if len(orGroups) == 0 {
			return ""
		}
		return "(" + strings.Join(orGroups, " OR ") + ")"
	}

	// buildUpperBoundCondition builds a lexicographic "< upperBound" condition.
	// For columns (c1, c2, c3) and values (M1, M2, M3) it produces:
	//   (c1 < M1) OR
	//   (c1 = M1 AND c2 < M2) OR
	//   (c1 = M1 AND c2 = M2 AND c3 < M3)
	buildUpperBoundCondition := func(upperBoundValues []string) string {
		if upperBoundValues == nil {
			return ""
		}

		orGroups := make([]string, 0, len(quotedColumns))

		for columnIndex := range quotedColumns {
			andConditions := make([]string, 0, columnIndex+1)

			// Prefix columns must match exactly: c1 = M1 AND c2 = M2 ...
			for prefixIndex := 0; prefixIndex < columnIndex; prefixIndex++ {
				if prefixIndex < len(upperBoundValues) {
					andConditions = append(
						andConditions,
						fmt.Sprintf("%s = %s", quotedColumns[prefixIndex], formatSqlLiteral(upperBoundValues[prefixIndex])),
					)
				}
			}

			// Current column uses strict "<" for the upper bound.
			if columnIndex < len(upperBoundValues) {
				andConditions = append(
					andConditions,
					fmt.Sprintf("%s < %s", quotedColumns[columnIndex], formatSqlLiteral(upperBoundValues[columnIndex])),
				)
			}

			if len(andConditions) > 0 {
				orGroups = append(orGroups, "("+strings.Join(andConditions, " AND ")+")")
			}
		}

		if len(orGroups) == 0 {
			return ""
		}
		return "(" + strings.Join(orGroups, " OR ") + ")"
	}

	lowerBoundValues := splitBoundaryValues(chunk.Min)
	upperBoundValues := splitBoundaryValues(chunk.Max)

	var chunkCondition string

	switch {
	case chunk.Min != nil && chunk.Max != nil:
		lowerCondition := buildLowerBoundCondition(lowerBoundValues)
		upperCondition := buildUpperBoundCondition(upperBoundValues)
		if lowerCondition != "" && upperCondition != "" {
			chunkCondition = fmt.Sprintf("(%s) AND (%s)", lowerCondition, upperCondition)
		} else {
			chunkCondition = lowerCondition + upperCondition
		}
	case chunk.Min != nil:
		chunkCondition = buildLowerBoundCondition(lowerBoundValues)
	case chunk.Max != nil:
		chunkCondition = buildUpperBoundCondition(upperBoundValues)
	}

	// Combine with any additional filter if present.
	if extraFilter != "" && chunkCondition != "" {
		return fmt.Sprintf("(%s) AND (%s)", chunkCondition, extraFilter)
	}

	return chunkCondition
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

// MySQLTableExistsQuery returns the query to check if a table has any rows using EXISTS
func MySQLTableExistsQuery(stream types.StreamInterface) string {
	quotedTable := QuoteTable(stream.Namespace(), stream.Name(), constants.MySQL)
	return fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM %s LIMIT 1)", quotedTable)
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
// It returns the flavor, major and minor version of the MySQL server
func MySQLVersion(ctx context.Context, client *sqlx.DB) (string, int, int, error) {
	var version string
	err := client.QueryRowContext(ctx, "SELECT @@version").Scan(&version)
	if err != nil {
		return "", 0, 0, fmt.Errorf("failed to get MySQL version: %s", err)
	}

	parts := strings.Split(version, ".")
	if len(parts) < 2 {
		return "", 0, 0, fmt.Errorf("invalid version format")
	}
	majorVersion, err := strconv.Atoi(parts[0])
	if err != nil {
		return "", 0, 0, fmt.Errorf("invalid major version: %s", err)
	}

	minorVersion, err := strconv.Atoi(parts[1])
	if err != nil {
		return "", 0, 0, fmt.Errorf("invalid minor version: %s", err)
	}

	mysqlFlavor := "MySQL"
	if strings.Contains(strings.ToUpper(version), "MARIADB") {
		mysqlFlavor = "MariaDB"
	}

	return mysqlFlavor, majorVersion, minorVersion, nil
}

func WithIsolation(ctx context.Context, client *sqlx.DB, readOnly bool, fn func(tx *sql.Tx) error) error {
	tx, err := client.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  readOnly,
	})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %s", err)
	}
	defer func() {
		if rerr := tx.Rollback(); rerr != nil && rerr != sql.ErrTxDone {
			logger.Errorf("transaction rollback failed: %s", rerr)
		}
	}()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// MSSQL-Specific Queries

// MSSQLDiscoverTablesQuery returns the query to discover tables in a MSSQL database
func MSSQLDiscoverTablesQuery() string {
	return `
		SELECT
			t.TABLE_SCHEMA,
			t.TABLE_NAME
		FROM INFORMATION_SCHEMA.TABLES t
		WHERE t.TABLE_TYPE = 'BASE TABLE'
		AND t.TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA','sys')
	`
}

// MSSQLTableSchemaQuery returns the query to fetch the column_name, data_type, nullable, and primary_key information of a table in MSSQL
func MSSQLTableSchemaQuery() string {
	return `
		SELECT  c.COLUMN_NAME,
		        c.DATA_TYPE,
		        c.IS_NULLABLE,
		        CAST(CASE WHEN tc.CONSTRAINT_TYPE = 'PRIMARY KEY' THEN 1 ELSE 0 END AS BIT) AS IS_PRIMARY_KEY
		FROM    INFORMATION_SCHEMA.COLUMNS AS c
		LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu
		       ON  c.TABLE_CATALOG = kcu.TABLE_CATALOG
		       AND c.TABLE_SCHEMA  = kcu.TABLE_SCHEMA
		       AND c.TABLE_NAME    = kcu.TABLE_NAME
		       AND c.COLUMN_NAME   = kcu.COLUMN_NAME
		LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
		       ON  kcu.TABLE_CATALOG = tc.TABLE_CATALOG
		       AND kcu.TABLE_SCHEMA  = tc.TABLE_SCHEMA
		       AND kcu.TABLE_NAME    = tc.TABLE_NAME
		       AND kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
		       AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
		WHERE   c.TABLE_SCHEMA = @p1
		  AND   c.TABLE_NAME   = @p2
		ORDER BY c.ORDINAL_POSITION
	`
}

// MSSQLPhysLocExtremesQuery returns the query to fetch MIN and MAX %%physloc%% values for a table
func MSSQLPhysLocExtremesQuery(stream types.StreamInterface) string {
	quotedTable := QuoteTable(stream.Namespace(), stream.Name(), constants.MSSQL)
	return fmt.Sprintf("SELECT MIN(%%%%physloc%%%%), MAX(%%%%physloc%%%%) FROM %s", quotedTable)
}

// MSSQLPhysLocNextChunkEndQuery returns the query to find the next %%physloc%% chunk boundary
func MSSQLPhysLocNextChunkEndQuery(stream types.StreamInterface, chunkSize int64) string {
	quotedTable := QuoteTable(stream.Namespace(), stream.Name(), constants.MSSQL)
	return fmt.Sprintf(`
		WITH ordered AS (
			SELECT %%%%physloc%%%% AS physloc, ROW_NUMBER() OVER (ORDER BY %%%%physloc%%%%) AS rn
			FROM %s
			WHERE %%%%physloc%%%% > @p1
		)
		SELECT physloc
		FROM ordered
		WHERE rn = %d
	`, quotedTable, chunkSize)
}

// MSSQLCDCSupportQuery returns the query to check if CDC is enabled for the current database
func MSSQLCDCSupportQuery() string {
	return `
		SELECT is_cdc_enabled
		FROM sys.databases
		WHERE name = DB_NAME()
	`
}

// TODO: check about `sys.fn_cdc_get_min_lsn`
// MSSQLCDCMaxLSNQuery returns the query to fetch the current maximum LSN for CDC
func MSSQLCDCMaxLSNQuery() string {
	return "SELECT sys.fn_cdc_get_max_lsn()"
}

// MSSQLCDCAdvanceLSNQuery returns the query to increment an LSN for CDC
func MSSQLCDCAdvanceLSNQuery() string {
	return "SELECT sys.fn_cdc_increment_lsn(@p1)"
}

// MSSQLCDCDiscoverQuery returns the query to discover CDC-enabled capture instances
func MSSQLCDCDiscoverQuery() string {
	return `
		SELECT s.name AS schema_name,
			t.name AS table_name,
			c.capture_instance
		FROM sys.tables t
		JOIN sys.schemas s ON t.schema_id = s.schema_id
		JOIN cdc.change_tables c ON t.object_id = c.source_object_id
	`
}

// MSSQLCDCGetChangesQuery returns the query to fetch CDC changes for a capture instance
func MSSQLCDCGetChangesQuery(captureInstance string) string {
	return fmt.Sprintf(`
		SELECT *
		FROM cdc.fn_cdc_get_all_changes_%s(@p1, @p2, 'all')
	`, captureInstance)
}

// buildMSSQLConcat builds a CONCAT expression for SQL Server (2012+)
// Uses CONCAT instead of CONCAT_WS for maximum compatibility:
// - CONCAT works on SQL Server 2012+ (vs CONCAT_WS which requires 2017+)
func buildMSSQLConcat(quotedCols []string) string {
	if len(quotedCols) == 1 {
		return quotedCols[0]
	}

	// Manual separator insertion required
	parts := make([]string, 0, len(quotedCols)*2-1)
	for i, col := range quotedCols {
		if i > 0 {
			parts = append(parts, "','")
		}
		parts = append(parts, col)
	}
	return fmt.Sprintf("CONCAT(%s)", strings.Join(parts, ", "))
}

func MinMaxQueryMSSQL(stream types.StreamInterface, columns []string) string {
	quotedCols := QuoteColumns(columns, constants.MSSQL)
	quotedTable := QuoteTable(stream.Namespace(), stream.Name(), constants.MSSQL)

	concatCols := buildMSSQLConcat(quotedCols)

	orderAsc := strings.Join(quotedCols, ", ")
	descCols := make([]string, len(quotedCols))
	for i, col := range quotedCols {
		descCols[i] = col + " DESC"
	}
	orderDesc := strings.Join(descCols, ", ")

	return fmt.Sprintf(`
    SELECT
        (SELECT TOP 1 %s FROM %s ORDER BY %s) AS min_value,
        (SELECT TOP 1 %s FROM %s ORDER BY %s) AS max_value
    `,
		concatCols, quotedTable, orderAsc,
		concatCols, quotedTable, orderDesc,
	)
}

// MSSQLNextChunkEndQuery returns the query to calculate the next chunk boundary.
//
// Example:
//
//	stream.Namespace()   = "dbo"
//	stream.Name()        = "orders"
//	orderingColumns      = []string{"order_id", "item_id"}
//	chunkSize            = 1000
//
// Conceptual output:
//
//	WITH ordered AS (
//	  SELECT
//	    <concat(order_id, item_id)> AS key_str,
//	    ROW_NUMBER() OVER (ORDER BY [order_id], [item_id]) AS rn
//	  FROM [dbo].[orders]
//	  WHERE
//	    ([order_id] > @p1)
//	     OR ([order_id] = @p1 AND [item_id] > @p2)
//	)
//	SELECT key_str FROM ordered WHERE rn = 1000;
func MSSQLNextChunkEndQuery(stream types.StreamInterface, orderingColumns []string, chunkSize int64) string {
	// Quote table and column names for MSSQL.
	quotedColumns := QuoteColumns(orderingColumns, constants.MSSQL)
	quotedTableName := QuoteTable(stream.Namespace(), stream.Name(), constants.MSSQL)

	var sqlBuilder strings.Builder

	// Expression that concatenates the ordering columns into a single key string.
	concatenatedKeyExpression := buildMSSQLConcat(quotedColumns)

	// Start CTE: select key_str and row number over the ordering columns.
	fmt.Fprintf(
		&sqlBuilder,
		"WITH ordered AS (SELECT %s AS key_str, ROW_NUMBER() OVER (ORDER BY %s) AS rn FROM %s",
		concatenatedKeyExpression,
		strings.Join(quotedColumns, ", "),
		quotedTableName,
	)

	// Build WHERE clause: lexicographic "greater than" using parameter placeholders (@p1, @p2, ...).
	// For columns (c1, c2, c3) and parameters (@p1, @p2, @p3), this becomes:
	//   (c1 > @p1)
	//   OR (c1 = @p1 AND c2 > @p2)
	//   OR (c1 = @p1 AND c2 = @p2 AND c3 > @p3)
	sqlBuilder.WriteString(" WHERE ")

	nextParameterIndex := 1
	for columnIndex := 0; columnIndex < len(orderingColumns); columnIndex++ {
		if columnIndex > 0 {
			sqlBuilder.WriteString(" OR ")
		}

		sqlBuilder.WriteString("(")

		// Prefix columns must match exactly: c1 = @p1 AND c2 = @p2 ...
		for prefixColumnIndex := 0; prefixColumnIndex < columnIndex; prefixColumnIndex++ {
			fmt.Fprintf(
				&sqlBuilder,
				"%s = @p%d AND ",
				quotedColumns[prefixColumnIndex],
				nextParameterIndex,
			)
			nextParameterIndex++
		}

		// Current column must be strictly greater than its parameter.
		fmt.Fprintf(
			&sqlBuilder,
			"%s > @p%d",
			quotedColumns[columnIndex],
			nextParameterIndex,
		)
		nextParameterIndex++

		sqlBuilder.WriteString(")")
	}

	// Close CTE and select the row at position = chunkSize.
	fmt.Fprintf(
		&sqlBuilder,
		") SELECT key_str FROM ordered WHERE rn = %d",
		chunkSize,
	)

	return sqlBuilder.String()
}

// MSSQLPhysLocChunkScanQuery returns the SQL query for scanning a chunk using %%physloc%% in MSSQL
func MSSQLPhysLocChunkScanQuery(stream types.StreamInterface, chunk types.Chunk, filter string) string {
	tableName := QuoteTable(stream.Namespace(), stream.Name(), constants.MSSQL)

	// Helper to format %%physloc%% value (binary) as hex literal
	formatPhysLocValue := func(val any) string {
		if val == nil {
			return "NULL"
		}
		// %%physloc%% is always binary, convert to hex literal
		if b, ok := val.([]byte); ok {
			if len(b) == 0 {
				return "0x"
			}
			hexString := fmt.Sprintf("%X", b)
			return "0x" + hexString
		}
		// If it's a string (from utils.ConvertToString on []byte), convert bytes to hex
		if s, ok := val.(string); ok {
			// If it's already a hex string like "0x...", use it directly
			if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
				return s
			}
			// Convert string of bytes to hex (utils.ConvertToString converts []byte to string of bytes)
			hexString := fmt.Sprintf("%X", []byte(s))
			return "0x" + hexString
		}
		return fmt.Sprintf("%v", val)
	}

	var chunkCond string
	// Use %%%% to escape %% in fmt.Sprintf (%% becomes % in output)
	switch {
	case chunk.Min != nil && chunk.Max != nil:
		chunkCond = fmt.Sprintf("%%%%physloc%%%% > %s AND %%%%physloc%%%% <= %s", formatPhysLocValue(chunk.Min), formatPhysLocValue(chunk.Max))
	case chunk.Min != nil:
		chunkCond = fmt.Sprintf("%%%%physloc%%%% > %s", formatPhysLocValue(chunk.Min))
	case chunk.Max != nil:
		chunkCond = fmt.Sprintf("%%%%physloc%%%% <= %s", formatPhysLocValue(chunk.Max))
	default:
		chunkCond = "1 = 1"
	}

	if filter != "" {
		chunkCond = fmt.Sprintf("(%s) AND (%s)", chunkCond, filter)
	}

	return fmt.Sprintf("SELECT * FROM %s WITH (READPAST) WHERE %s ORDER BY %%%%physloc%%%%", tableName, chunkCond)
}

// MSSQLChunkScanQuery returns the SQL query for scanning a chunk in MSSQL
func MSSQLChunkScanQuery(stream types.StreamInterface, filterColumns []string, chunk types.Chunk, filter string) string {
	tableName := QuoteTable(stream.Namespace(), stream.Name(), constants.MSSQL)
	quotedCols := QuoteColumns(filterColumns, constants.MSSQL)
	condition := buildChunkConditionMSSQL(quotedCols, chunk, filter)
	if condition == "" {
		condition = utils.Ternary(filter != "", filter, "1 = 1").(string)
	}

	orderBy := strings.Join(quotedCols, ", ")
	return fmt.Sprintf("SELECT * FROM %s WITH (READPAST) WHERE %s ORDER BY %s", tableName, condition, orderBy)
}

// MSSQLTableRowStatsQuery returns the query to fetch the estimated row count and average row size of a table in MSSQL
func MSSQLTableRowStatsQuery() string {
	return `
		SELECT 
			SUM(p.rows) AS row_count,
			CEILING((SUM(a.total_pages) * 8.0 * 1024.0) / NULLIF(SUM(p.rows), 0)) AS avg_row_bytes
		FROM sys.tables t
		JOIN sys.indexes i ON t.object_id = i.object_id
		JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
		JOIN sys.allocation_units a ON p.partition_id = a.container_id
		JOIN sys.schemas s ON t.schema_id = s.schema_id
		WHERE t.type = 'U'
		AND i.index_id IN (0,1)
		AND s.name = @p1
		AND t.name = @p2
	`
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
	chunkMin := chunk.Min.(string)
	quotedTable := QuoteTable(stream.Namespace(), stream.Name(), constants.Oracle)

	filterClause := utils.Ternary(filter == "", "", " AND ("+filter+")").(string)

	if chunk.Max != nil {
		chunkMax := chunk.Max.(string)
		return fmt.Sprintf("SELECT * FROM %s WHERE ROWID >= '%v' AND ROWID < '%v' %s",
			quotedTable, chunkMin, chunkMax, filterClause)
	}
	return fmt.Sprintf("SELECT * FROM %s WHERE ROWID >= '%v' %s",
		quotedTable, chunkMin, filterClause)
}

// OracleTableRowStatsQuery returns the query to fetch the estimated row count of a table in Oracle
func OracleTableRowStatsQuery() string {
	return `SELECT NUM_ROWS FROM ALL_TABLES WHERE OWNER = :1 AND TABLE_NAME = :2`
}

// OracleTableSizeQuery returns the query to fetch the size of a table in bytes in OracleDB
func OracleBlockSizeQuery() string {
	return `SELECT CEIL(BYTES / NULLIF(BLOCKS, 0)) FROM user_segments WHERE BLOCKS IS NOT NULL AND ROWNUM =1`
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
func OracleIncrementalValueFormatter(ctx context.Context, cursorField, argumentPlaceholder string, isBackfill bool, lastCursorValue any, opts DriverOptions) (string, any, error) {
	// Get the datatype of the cursor field from streams
	stream := opts.Stream
	// in case of incremental sync mode, during backfill to avoid duplicate records we need to use '<=', otherwise use '>'
	operator := utils.Ternary(isBackfill, "<=", ">").(string)
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
	err = opts.Client.QueryRowContext(ctx, query).Scan(&datatype)
	if err != nil {
		return "", nil, fmt.Errorf("failed to get column datatype: %s", err)
	}
	// if the cursor field is a timestamp and not timezone aware, we need to cast the value as timestamp
	quotedCol := QuoteIdentifier(cursorField, constants.Oracle)
	if isTimestamp && !strings.Contains(string(datatype), "TIME ZONE") {
		return fmt.Sprintf("%s %s CAST(%s AS TIMESTAMP)", quotedCol, operator, argumentPlaceholder), formattedValue, nil
	}
	return fmt.Sprintf("%s %s %s", quotedCol, operator, argumentPlaceholder), formattedValue, nil
}

// ParseFilter converts a filter string to a valid SQL WHERE condition, also appends the threshold filter if present
func SQLFilter(stream types.StreamInterface, driver string, thresholdFilter string) (string, error) {
	buildCondition := func(cond types.Condition, driver string) (string, error) {
		var driverType constants.DriverType
		switch driver {
		case "mysql":
			driverType = constants.MySQL
		case "postgres":
			driverType = constants.Postgres
		case "oracle":
			driverType = constants.Oracle
		case "mssql":
			driverType = constants.MSSQL
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

	var finalFilter string
	var filterErr error
	switch {
	case len(filter.Conditions) == 0:
		return thresholdFilter, nil
	case len(filter.Conditions) == 1:
		finalFilter, filterErr = buildCondition(filter.Conditions[0], driver)
	default:
		conditions := make([]string, 0, len(filter.Conditions))
		err := utils.ForEach(filter.Conditions, func(cond types.Condition) error {
			formatted, err := buildCondition(cond, driver)
			if err != nil {
				return err
			}
			conditions = append(conditions, formatted)
			return nil
		})
		finalFilter, filterErr = strings.Join(conditions, fmt.Sprintf(" %s ", filter.LogicalOperator)), err
	}

	return utils.Ternary(thresholdFilter == "", finalFilter, fmt.Sprintf("(%s) AND (%s)", thresholdFilter, finalFilter)).(string), filterErr
}

// DriverOptions contains options for creating various queries
type DriverOptions struct {
	Driver constants.DriverType
	Stream types.StreamInterface
	State  *types.State
	Client *sqlx.DB
}

// BuildIncrementalQuery generates the incremental query SQL based on driver type
func BuildIncrementalQuery(ctx context.Context, opts DriverOptions) (string, []any, error) {
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

	placeholder := GetPlaceholder(opts.Driver)

	// buildCursorCondition creates the SQL condition for incremental queries based on cursor fields.
	buildCursorCondition := func(cursorField string, lastCursorValue any, argumentPosition int) (string, any, error) {
		if opts.Driver == constants.Oracle {
			return OracleIncrementalValueFormatter(ctx, cursorField, placeholder(argumentPosition), false, lastCursorValue, opts)
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

	logger.Infof("Starting incremental sync for stream[%s] with condition: %s and args: %v", opts.Stream.ID(), incrementalCondition, queryArgs)

	// Use QuoteTable helper function for consistent table quoting
	quotedTable := QuoteTable(opts.Stream.Namespace(), opts.Stream.Name(), opts.Driver)
	incrementalQuery := fmt.Sprintf("SELECT * FROM %s WHERE (%s)", quotedTable, incrementalCondition)

	return incrementalQuery, queryArgs, nil
}

func GetMaxCursorValues(ctx context.Context, client *sqlx.DB, driverType constants.DriverType, stream types.StreamInterface) (any, any, error) {
	primaryCursor, secondaryCursor := stream.Cursor()
	quotedTable := QuoteTable(stream.Namespace(), stream.Name(), driverType)

	primaryCursorQuoted := QuoteIdentifier(primaryCursor, driverType)
	secondaryCursorQuoted := QuoteIdentifier(secondaryCursor, driverType)

	var maxPrimaryCursorValue, maxSecondaryCursorValue any

	bytesConverter := func(value any) any {
		switch v := value.(type) {
		case []byte:
			return string(v)
		default:
			return v
		}
	}

	cursorValueQuery := utils.Ternary(secondaryCursor == "",
		fmt.Sprintf("SELECT MAX(%s) FROM %s", primaryCursorQuoted, quotedTable),
		fmt.Sprintf("SELECT MAX(%s), MAX(%s) FROM %s", primaryCursorQuoted, secondaryCursorQuoted, quotedTable)).(string)

	if secondaryCursor != "" {
		err := client.QueryRowContext(ctx, cursorValueQuery).Scan(&maxPrimaryCursorValue, &maxSecondaryCursorValue)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to scan the cursor values: %s", err)
		}
		maxSecondaryCursorValue = bytesConverter(maxSecondaryCursorValue)
	} else {
		err := client.QueryRowContext(ctx, cursorValueQuery).Scan(&maxPrimaryCursorValue)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to scan primary cursor value: %s", err)
		}
	}
	return bytesConverter(maxPrimaryCursorValue), maxSecondaryCursorValue, nil
}

// ThresholdFilter is used to update the filter for initial run of incremental sync during backfill.
// This is to avoid dupliction of records, as max cursor value is fetched before the chunk creation.
func ThresholdFilter(ctx context.Context, opts DriverOptions) (string, []any, error) {
	if opts.Stream.GetSyncMode() != types.INCREMENTAL {
		return "", nil, nil
	}
	primaryCursor, secondaryCursor := opts.Stream.Cursor()
	primaryCursorValue := opts.State.GetCursor(opts.Stream.Self(), primaryCursor)
	placeholder := GetPlaceholder(opts.Driver)

	createThresholdCondition := func(argumentPosition int, cursorField string, cursorValue any) (string, any, error) {
		if opts.Driver == constants.Oracle {
			return OracleIncrementalValueFormatter(ctx, cursorField, placeholder(argumentPosition), true, cursorValue, opts)
		}
		conditionFilter := fmt.Sprintf("%s <= %s", QuoteIdentifier(cursorField, opts.Driver), placeholder(argumentPosition))
		return conditionFilter, cursorValue, nil
	}

	thresholdFilter, argument, err := createThresholdCondition(1, primaryCursor, primaryCursorValue)
	if err != nil {
		return "", nil, fmt.Errorf("failed to format primary cursor value: %s", err)
	}
	// IS NULL condition is required to handle the case where cursor value is NULL for some rows.
	// Some driver will avoid returning such rows when <= condition is used.
	thresholdFilter = fmt.Sprintf("(%s IS NULL OR %s)", QuoteIdentifier(primaryCursor, opts.Driver), thresholdFilter)
	arguments := []any{argument}
	if secondaryCursor != "" {
		secondaryCursorValue := opts.State.GetCursor(opts.Stream.Self(), secondaryCursor)
		secondaryCondition, argument, err := createThresholdCondition(2, secondaryCursor, secondaryCursorValue)
		if err != nil {
			return "", nil, fmt.Errorf("failed to format secondary cursor value: %s", err)
		}
		thresholdFilter = fmt.Sprintf("%s AND (%s IS NULL OR %s)", thresholdFilter, QuoteIdentifier(secondaryCursor, opts.Driver), secondaryCondition)
		arguments = append(arguments, argument)
	}
	return thresholdFilter, arguments, nil
}
