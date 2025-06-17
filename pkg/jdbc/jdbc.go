package jdbc

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/datazip-inc/olake/types"
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
func NextChunkEndQuery(stream types.StreamInterface, columns []string, chunkSize int64) string {
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
	// ORDER + LIMIT
	fmt.Fprintf(&query, " ORDER BY %s", strings.Join(columns, ", "))
	fmt.Fprintf(&query, " LIMIT %d) AS subquery", chunkSize)
	return query.String()
}

// buildChunkCondition builds the condition for a chunk
func buildChunkCondition(filterColumn string, chunk types.Chunk, parsedFilter string) string {
	chunkCond := ""
	if chunk.Min != nil && chunk.Max != nil {
		chunkCond = fmt.Sprintf("%s >= %v AND %s < %v", filterColumn, chunk.Min, filterColumn, chunk.Max)
	} else if chunk.Min != nil {
		chunkCond = fmt.Sprintf("%s >= %v", filterColumn, chunk.Min)
	} else if chunk.Max != nil {
		chunkCond = fmt.Sprintf("%s < %v", filterColumn, chunk.Max)
	}

	if parsedFilter != "" {
		if chunkCond != "" {
			return fmt.Sprintf("(%s) AND (%s)", chunkCond, parsedFilter)
		}
		return parsedFilter
	}
	return chunkCond
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
func PostgresNextChunkEndQuery(stream types.StreamInterface, filterColumn string, filterValue interface{}, batchSize int, parsedFilter string) string {
	baseCond := fmt.Sprintf(`%s > %v`, filterColumn, filterValue)
	if parsedFilter != "" {
		baseCond = fmt.Sprintf(`(%s) AND (%s)`, baseCond, parsedFilter)
	}
	return fmt.Sprintf(`SELECT MAX(%s) FROM (SELECT %s FROM "%s"."%s" WHERE %s ORDER BY %s ASC LIMIT %d) AS T`, filterColumn, filterColumn, stream.Namespace(), stream.Name(), baseCond, filterColumn, batchSize)
}

// PostgresMinQuery returns the query to fetch the minimum value of a column in PostgreSQL
func PostgresMinQuery(stream types.StreamInterface, filterColumn string, filterValue interface{}) string {
	return fmt.Sprintf(`SELECT MIN(%s) FROM "%s"."%s" WHERE %s > %v`, filterColumn, stream.Namespace(), stream.Name(), filterColumn, filterValue)
}

// PostgresBuildSplitScanQuery builds a chunk scan query for PostgreSQL
func PostgresChunkScanQuery(stream types.StreamInterface, filterColumn string, chunk types.Chunk, parsedFilter string) string {
	condition := buildChunkCondition(filterColumn, chunk, parsedFilter)
	return fmt.Sprintf(`SELECT * FROM "%s"."%s" WHERE %s`, stream.Namespace(), stream.Name(), condition)
}

// MySQL-Specific Queries

// buildChunkConditionMySQL builds the condition for a chunk in MySQL
func buildChunkConditionMySQL(filterColumns []string, chunk types.Chunk) string {
	colTuple := "(" + strings.Join(filterColumns, ", ") + ")"

	buildSQLTuple := func(val any) string {
		parts := strings.Split(val.(string), ",")
		for i, part := range parts {
			parts[i] = fmt.Sprintf("'%s'", strings.TrimSpace(part))
		}
		return strings.Join(parts, ", ")
	}
	switch {
	case chunk.Min != nil && chunk.Max != nil:
		return fmt.Sprintf("%s >= (%s) AND %s < (%s)", colTuple, buildSQLTuple(chunk.Min), colTuple, buildSQLTuple(chunk.Max))
	case chunk.Min != nil:
		return fmt.Sprintf("%s >= (%s)", colTuple, buildSQLTuple(chunk.Min))
	case chunk.Max != nil:
		return fmt.Sprintf("%s < (%s)", colTuple, buildSQLTuple(chunk.Max))
	default:
		return ""
	}
}

// MysqlLimitOffsetScanQuery is used to get the rows
func MysqlLimitOffsetScanQuery(stream types.StreamInterface, chunk types.Chunk) string {
	query := ""
	if chunk.Min == nil {
		maxVal, _ := strconv.ParseUint(chunk.Max.(string), 10, 64)
		query = fmt.Sprintf("SELECT * FROM `%s`.`%s` LIMIT %d", stream.Namespace(), stream.Name(), maxVal)
	} else if chunk.Min != nil && chunk.Max != nil {
		minVal, _ := strconv.ParseUint(chunk.Min.(string), 10, 64)
		maxVal, _ := strconv.ParseUint(chunk.Max.(string), 10, 64)
		query = fmt.Sprintf("SELECT * FROM `%s`.`%s` LIMIT %d OFFSET %d", stream.Namespace(), stream.Name(), maxVal-minVal, minVal)
	} else {
		minVal, _ := strconv.ParseUint(chunk.Min.(string), 10, 64)
		maxNum := ^uint64(0)
		query = fmt.Sprintf("SELECT * FROM `%s`.`%s` LIMIT %d OFFSET %d", stream.Namespace(), stream.Name(), maxNum, minVal)
	}
	return query
}

// MySQLWithoutState builds a chunk scan query for MySql
func MysqlChunkScanQuery(stream types.StreamInterface, filterColumns []string, chunk types.Chunk) string {
	condition := buildChunkConditionMySQL(filterColumns, chunk)
	return fmt.Sprintf("SELECT * FROM `%s`.`%s` WHERE %s", stream.Namespace(), stream.Name(), condition)
}

// MinMaxQueryMySQL returns the query to fetch MIN and MAX values of a column in a MySQL table
func MinMaxQueryMySQL(stream types.StreamInterface, columns []string) string {
	concatCols := fmt.Sprintf("CONCAT_WS(',', %s)", strings.Join(columns, ", "))
	orderAsc := strings.Join(columns, ", ")
	descCols := make([]string, len(columns))
	for i, col := range columns {
		descCols[i] = col + " DESC"
	}
	orderDesc := strings.Join(descCols, ", ")
	return fmt.Sprintf(`
		SELECT
			(SELECT %s FROM %s.%s ORDER BY %s LIMIT 1) AS min_value,
			(SELECT %s FROM %s.%s ORDER BY %s LIMIT 1) AS max_value
	`,
		concatCols, stream.Namespace(), stream.Name(), orderAsc,
		concatCols, stream.Namespace(), stream.Name(), orderDesc,
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

// MySQLVersion returns the version of the MySQL server
// It returns the major and minor version of the MySQL server
func MySQLVersion(client *sqlx.DB) (int, int, error) {
	var version string
	err := client.QueryRow("SELECT @@version").Scan(&version)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get MySQL version: %w", err)
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

// ParseFilter converts a filter string to a valid SQL WHERE condition
func ParseFilter(filter, driver string) (string, error) {
	if strings.TrimSpace(filter) == "" {
		return "", nil
	}

	conditions, operators, err := splitFilterConditions(filter)
	if err != nil {
		return "", err
	}

	var parsedConditions []string
	for _, cond := range conditions {
		cond = strings.TrimSpace(cond)
		var parsed string
		switch driver {
		case "postgres":
			parsed, err = parsePostgresCondition(cond)
		case "mysql":
			parsed, err = parseMySQLCondition(cond)
		default:
			return "", fmt.Errorf("unsupported driver: %s", driver)
		}
		if err != nil {
			return "", fmt.Errorf("invalid condition '%s': %w", cond, err)
		}
		parsedConditions = append(parsedConditions, parsed)
	}

	result := parsedConditions[0]
	for i, op := range operators {
		result += " " + strings.ToUpper(op) + " " + parsedConditions[i+1]
	}
	return result, nil
}

// splitFilterConditions splits the filter by 'and'/'or' and returns conditions and operators
func splitFilterConditions(filter string) ([]string, []string, error) {
	var conditions []string
	var operators []string
	pattern := regexp.MustCompile(`\s+(and|or)\s+`)

	inQuotes := false
	var currentCondition strings.Builder
	i := 0

	for i < len(filter) {
		char := filter[i]
		if char == '"' {
			inQuotes = !inQuotes
			currentCondition.WriteByte(char)
			i++
			continue
		}

		if !inQuotes {
			remaining := filter[i:]
			if match := pattern.FindStringIndex(remaining); match != nil && match[0] == 0 {
				opMatch := pattern.FindStringSubmatch(remaining)
				operator := opMatch[1]

				conditions = append(conditions, strings.TrimSpace(currentCondition.String()))
				operators = append(operators, operator)

				currentCondition.Reset()
				i += match[1]
				continue
			}
		}
		currentCondition.WriteByte(char)
		i++
	}

	conditions = append(conditions, strings.TrimSpace(currentCondition.String()))
	if len(conditions) == 0 {
		return nil, nil, fmt.Errorf("no valid conditions found")
	}
	return conditions, operators, nil
}

var pattern = regexp.MustCompile(`^\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*(=|!=|>|>=|<|<=)\s*("([^"]*)"|(\S+))\s*$`)

// parsePostgresCondition parses a single condition for PostgreSQL
func parsePostgresCondition(cond string) (string, error) {
	matches := pattern.FindStringSubmatch(cond)
	if matches == nil {
		return "", fmt.Errorf("invalid filter format: %s", cond)
	}

	column := matches[1]
	operator := matches[2]
	value := matches[3]

	if strings.HasPrefix(value, `"`) && strings.HasSuffix(value, `"`) {
		value = value[1 : len(value)-1]
		value = "'" + escapeString(value) + "'"
	} else {
		if num, err := strconv.ParseFloat(value, 64); err == nil {
			value = fmt.Sprintf("%v", num)
		} else if value == "true" || value == "false" {
			value = fmt.Sprintf("%s::boolean", value)
		} else {
			return "", fmt.Errorf("unquoted string values are not allowed: %s", value)
		}
	}

	return fmt.Sprintf(`"%s" %s %s`, column, operator, value), nil
}

// parseMySQLCondition parses a single condition for MySQL
func parseMySQLCondition(cond string) (string, error) {
	match := pattern.FindStringSubmatch(cond)
	if match == nil {
		return "", fmt.Errorf("could not parse condition")
	}

	col := match[1]
	op := match[2]
	val := match[3]

	if strings.HasPrefix(val, "\"") && strings.HasSuffix(val, "\"") {
		unquoted := val[1 : len(val)-1]
		escaped := escapeString(unquoted)
		return fmt.Sprintf("`%s` %s '%s'", col, op, escaped), nil
	}

	if num, err := strconv.ParseFloat(val, 64); err == nil {
		return fmt.Sprintf("`%s` %s %v", col, op, num), nil
	}
	lower := strings.ToLower(val)
	if lower == "true" || lower == "false" {
		return fmt.Sprintf("`%s` %s %s", col, op, lower), nil
	}

	return "", fmt.Errorf("unsupported literal: %s", val)
}

// escapeString escapes single quotes
func escapeString(s string) string {
	return strings.Replace(s, "'", "''", -1)
}
