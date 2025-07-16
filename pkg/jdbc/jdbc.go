package jdbc

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/jmoiron/sqlx"
)

// MinMaxQuery returns the query to fetch MIN and MAX values of a column in a Postgres table
func MinMaxQuery(stream types.StreamInterface, column string) string {
	return fmt.Sprintf(`SELECT MIN(%[1]s) AS min_value, MAX(%[1]s) AS max_value FROM %[2]s.%[3]s`, column, stream.Namespace(), stream.Name())
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
//	SELECT MAX(key_str) FROM (
//	  SELECT CONCAT_WS(',', id, created_at) AS key_str
//	  FROM `mydb`.`users`
//	  WHERE (`id` > ?) OR (`id` = ? AND `created_at` > ?)
//	  ORDER BY id, created_at
//	  LIMIT 1000
//	) AS subquery
func NextChunkEndQuery(stream types.StreamInterface, columns []string, chunkSize int64, filter string) string {
	var query strings.Builder
	// SELECT with quoted and concatenated values
	fmt.Fprintf(&query, "SELECT MAX(key_str) FROM (SELECT CONCAT_WS(',', %s) AS key_str FROM `%s`.`%s`",
		strings.Join(columns, ", "),
		stream.Namespace(),
		stream.Name(),
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
			fmt.Fprintf(&query, "`%s` = ? AND ", columns[equalityColIndex])
		}
		fmt.Fprintf(&query, "`%s` > ?", columns[currentColIndex])
		query.WriteString(")")
	}
	if filter != "" {
		query.WriteString(" AND (" + filter + ")")
	}
	// ORDER + LIMIT
	fmt.Fprintf(&query, " ORDER BY %s", strings.Join(columns, ", "))
	fmt.Fprintf(&query, " LIMIT %d) AS subquery", chunkSize)
	return query.String()
}

// PostgreSQL-Specific Queries
// TODO: Rewrite queries for taking vars as arguments while execution.

// PostgresWithoutState returns the query for a simple SELECT without state
func PostgresWithoutState(stream types.StreamInterface) string {
	return fmt.Sprintf(`SELECT * FROM "%s"."%s" ORDER BY %s`, stream.Namespace(), stream.Name(), stream.Cursor())
}

// PostgresWithState returns the query for a SELECT with state
func PostgresWithState(stream types.StreamInterface) string {
	return fmt.Sprintf(`SELECT * FROM "%s"."%s" where "%s">$1 ORDER BY "%s" ASC NULLS FIRST`, stream.Namespace(), stream.Name(), stream.Cursor(), stream.Cursor())
}

// PostgresRowCountQuery returns the query to fetch the estimated row count in PostgreSQL
func PostgresRowCountQuery(stream types.StreamInterface) string {
	return fmt.Sprintf(`SELECT reltuples::bigint AS approx_row_count FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = '%s' AND n.nspname = '%s';`, stream.Name(), stream.Namespace())
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
func PostgresNextChunkEndQuery(stream types.StreamInterface, filterColumn string, filterValue interface{}, batchSize int, extraFilter string) string {
	baseCond := fmt.Sprintf(`%s > %v`, filterColumn, filterValue)
	baseCond = utils.Ternary(extraFilter == "", baseCond, fmt.Sprintf(`(%s) AND (%s)`, baseCond, extraFilter)).(string)
	return fmt.Sprintf(`SELECT MAX(%s) FROM (SELECT %s FROM "%s"."%s" WHERE %s ORDER BY %s ASC LIMIT %d) AS T`, filterColumn, filterColumn, stream.Namespace(), stream.Name(), baseCond, filterColumn, batchSize)
}

// PostgresMinQuery returns the query to fetch the minimum value of a column in PostgreSQL
func PostgresMinQuery(stream types.StreamInterface, filterColumn string, filterValue interface{}) string {
	return fmt.Sprintf(`SELECT MIN(%s) FROM "%s"."%s" WHERE %s > %v`, filterColumn, stream.Namespace(), stream.Name(), filterColumn, filterValue)
}

// PostgresBuildSplitScanQuery builds a chunk scan query for PostgreSQL
func PostgresChunkScanQuery(stream types.StreamInterface, filterColumn string, chunk types.Chunk, filter string) string {
	chunkCond := ""
	if chunk.Min != nil && chunk.Max != nil {
		chunkCond = fmt.Sprintf("%s >= %v AND %s < %v", filterColumn, chunk.Min, filterColumn, chunk.Max)
	} else if chunk.Min != nil {
		chunkCond = fmt.Sprintf("%s >= %v", filterColumn, chunk.Min)
	} else if chunk.Max != nil {
		chunkCond = fmt.Sprintf("%s < %v", filterColumn, chunk.Max)
	}

	chunkCond = utils.Ternary(filter != "" && chunkCond != "", fmt.Sprintf("(%s) AND (%s)", chunkCond, filter), chunkCond).(string)
	return fmt.Sprintf(`SELECT * FROM "%s"."%s" WHERE %s`, stream.Namespace(), stream.Name(), chunkCond)
}

// MySQL-Specific Queries
// buildChunkConditionMySQL builds the condition for a chunk in MySQL
func buildChunkConditionMySQL(filterColumns []string, chunk types.Chunk, extraFilter string) string {
	colTuple := "(" + strings.Join(filterColumns, ", ") + ")"

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
	query := fmt.Sprintf("SELECT * FROM `%s`.`%s`", stream.Namespace(), stream.Name())
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
	return fmt.Sprintf("SELECT * FROM `%s`.`%s` WHERE %s", stream.Namespace(), stream.Name(), condition)
}

// MinMaxQueryMySQL returns the query to fetch MIN and MAX values of a column in a MySQL table
func MinMaxQueryMySQL(stream types.StreamInterface, columns []string, filter string) string {
	concatCols := fmt.Sprintf("CONCAT_WS(',', %s)", strings.Join(columns, ", "))
	orderAsc := strings.Join(columns, ", ")
	descCols := make([]string, len(columns))
	for i, col := range columns {
		descCols[i] = col + " DESC"
	}
	orderDesc := strings.Join(descCols, ", ")
	filterClause := ""
	if filter != "" {
		filterClause = fmt.Sprintf("WHERE %s", filter)
	}
	return fmt.Sprintf(`
	SELECT
		(SELECT %s FROM %s.%s %s ORDER BY %s LIMIT 1) AS min_value,
		(SELECT %s FROM %s.%s %s ORDER BY %s LIMIT 1) AS max_value
	`,
		concatCols, stream.Namespace(), stream.Name(), filterClause, orderAsc,
		concatCols, stream.Namespace(), stream.Name(), filterClause, orderDesc,
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

// MySQLTableRowsQuery returns the query to fetch the estimated row count of a table in MySQL
func MySQLTableRowsQuery() string {
	return `
		SELECT TABLE_ROWS
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

// MySQLTableColumnsQuery returns the query to fetch column names of a table in MySQL
func MySQLTableColumnsQuery() string {
	return `
		SELECT COLUMN_NAME 
		FROM INFORMATION_SCHEMA.COLUMNS 
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? 
		ORDER BY ORDINAL_POSITION
	`
}

// MySQLIncrementalQuery returns the query for incremental sync in MySQL
func MySQLIncrementalQuery(stream types.StreamInterface) string {
	return fmt.Sprintf(`SELECT * FROM %s.%s WHERE %s >= ? ORDER BY %s`, stream.Namespace(), stream.Name(), stream.Cursor(), stream.Cursor())
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

// OracleTableDetailsQuery returns the query to fetch the details of a table in OracleDB
func OracleTableDetailsQuery(schemaName, tableName string) string {
	return fmt.Sprintf("SELECT column_name, data_type, nullable, data_precision, data_scale FROM all_tab_columns WHERE owner = '%s' AND table_name = '%s'", schemaName, tableName)
}

// OraclePrimaryKeyQuery returns the query to fetch all the primary key columns of a table in OracleDB
func OraclePrimaryKeyColummsQuery(schemaName, tableName string) string {
	return fmt.Sprintf(`SELECT cols.column_name FROM all_constraints cons, all_cons_columns cols WHERE cons.constraint_type = 'P' AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner AND cons.owner = '%s' AND cols.table_name = '%s'`, schemaName, tableName)
}

// NextRowIDQuery returns the query to fetch the next max row id
func NextRowIDQuery(stream types.StreamInterface, currentSCN string, ROWID string, chunkSize int64) string {
	return fmt.Sprintf("SELECT MAX(ROWID),COUNT(*) AS row_count FROM(SELECT ROWID FROM %q.%q AS OF SCN %s WHERE ROWID >= '%s' ORDER BY ROWID FETCH FIRST %d ROWS ONLY)", stream.Namespace(), stream.Name(), currentSCN, ROWID, chunkSize)
}

// OracleChunkScanQuery returns the query to fetch the rows of a table in OracleDB
func OracleChunkScanQuery(stream types.StreamInterface, chunk types.Chunk) string {
	currentSCN := strings.Split(chunk.Min.(string), ",")[0]
	chunkMin := strings.Split(chunk.Min.(string), ",")[1]

	baseCondition := fmt.Sprintf("SELECT * FROM %q.%q AS OF SCN %s", stream.Namespace(), stream.Name(), currentSCN)

	if chunk.Min != nil && chunk.Max != nil {
		chunkMax := strings.Split(chunk.Max.(string), ",")[1]
		return fmt.Sprintf("%s WHERE ROWID >= '%v' AND ROWID < '%v'", baseCondition, chunkMin, chunkMax)
	} else if chunk.Min != nil {
		return fmt.Sprintf("%s WHERE ROWID >= '%v'", baseCondition, chunkMin)
	}
	return baseCondition
}

// OracleMinMaxCountQuery returns the query to fetch the min ROWID, max ROWID and number of rows of a table in OracleDB
func OracleMinMaxCountQuery(stream types.StreamInterface, currentSCN string) string {
	return fmt.Sprintf(`SELECT MIN(ROWID) AS minRowId, MAX(ROWID) AS maxRowId, COUNT(*) AS totalRows FROM %q.%q AS OF SCN %s`, stream.Namespace(), stream.Name(), currentSCN)
}

// OracleTableSizeQuery returns the query to fetch the size of a table in bytes in OracleDB
func OracleTableSizeQuery(stream types.StreamInterface) string {
	return fmt.Sprintf(`SELECT SUM(bytes) AS size_kb FROM user_segments WHERE segment_name = '%s' AND segment_type = 'TABLE'`, stream.Name())
}

// OracleCurrentSCNQuery returns the query to fetch the current SCN in OracleDB
func OracleCurrentSCNQuery() string {
	return `SELECT CURRENT_SCN FROM V$DATABASE`
}

// OracleEmptyCheckQuery returns the query to check if a table is empty in OracleDB
func OracleEmptyCheckQuery(stream types.StreamInterface) string {
	return fmt.Sprintf("SELECT 1 FROM %q.%q WHERE ROWNUM = 1", stream.Namespace(), stream.Name())
}

// ParseFilter converts a filter string to a valid SQL WHERE condition
func SQLFilter(stream types.StreamInterface, driver string) (string, error) {
	buildCondition := func(cond types.Condition, driver string) (string, error) {
		// Get column quote style
		quote := utils.Ternary(driver == "mysql", "`", "\"").(string)
		quotedColumn := fmt.Sprintf("%s%s%s", quote, cond.Column, quote)

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
