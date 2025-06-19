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
func NextChunkEndQuery(stream types.StreamInterface, columns []string, chunkSize int64, parsedFilter string) string {
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
	if parsedFilter != "" {
		query.WriteString(" AND (" + parsedFilter + ")")
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

	if parsedFilter != "" && chunkCond != "" {
		return fmt.Sprintf("(%s) AND (%s)", chunkCond, parsedFilter)
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
func buildChunkConditionMySQL(filterColumns []string, chunk types.Chunk, parsedFilter string) string {
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
	if parsedFilter != "" && chunkCond != "" {
		return fmt.Sprintf("(%s) AND (%s)", chunkCond, parsedFilter)
	}
	return chunkCond
}

// MysqlLimitOffsetScanQuery is used to get the rows
func MysqlLimitOffsetScanQuery(stream types.StreamInterface, chunk types.Chunk, parsedFilter string) string {
	baseQuery := fmt.Sprintf("SELECT * FROM `%s`.`%s`", stream.Namespace(), stream.Name())
	if parsedFilter != "" {
		baseQuery += " WHERE " + parsedFilter
	}
	query := baseQuery
	if chunk.Min == nil {
		maxVal, _ := strconv.ParseUint(chunk.Max.(string), 10, 64)
		query += fmt.Sprintf(" LIMIT %d", maxVal)
	} else if chunk.Min != nil && chunk.Max != nil {
		minVal, _ := strconv.ParseUint(chunk.Min.(string), 10, 64)
		maxVal, _ := strconv.ParseUint(chunk.Max.(string), 10, 64)
		query += fmt.Sprintf(" LIMIT %d OFFSET %d", maxVal-minVal, minVal)
	} else {
		minVal, _ := strconv.ParseUint(chunk.Min.(string), 10, 64)
		maxNum := ^uint64(0)
		query += fmt.Sprintf(" LIMIT %d OFFSET %d", maxNum, minVal)
	}
	return query
}

// MySQLWithoutState builds a chunk scan query for MySql
func MysqlChunkScanQuery(stream types.StreamInterface, filterColumns []string, chunk types.Chunk, parsedFilter string) string {
	condition := buildChunkConditionMySQL(filterColumns, chunk, parsedFilter)
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
	// Split into individual conditions and logical operators (AND/OR)
	conditions, operators, err := splitFilterConditions(filter)
	if err != nil {
		return "", err
	}

	var parsedConditions []string
	// Parse each condition according to the SQL dialect
	for _, cond := range conditions {
		cond = strings.TrimSpace(cond)
		var formatted string
		switch driver {
		case "postgres":
			formatted, err = parsePostgresCondition(cond)
		case "mysql":
			formatted, err = parseMySQLCondition(cond)
		default:
			return "", fmt.Errorf("unsupported driver: %s", driver)
		}
		if err != nil {
			return "", fmt.Errorf("invalid condition '%s': %w", cond, err)
		}
		parsedConditions = append(parsedConditions, formatted)
	}

	// Build the final WHERE fragment by combining parsed conditions and operators
	var sqlBuilder strings.Builder
	sqlBuilder.WriteString(parsedConditions[0])
	for i, operator := range operators {
		sqlBuilder.WriteString(" ")
		sqlBuilder.WriteString(strings.ToUpper(operator))
		sqlBuilder.WriteString(" ")
		sqlBuilder.WriteString(parsedConditions[i+1])
	}
	return sqlBuilder.String(), nil
}

// splitFilterConditions splits the filter by 'and'/'or' and returns conditions and operators
func splitFilterConditions(filterExpr string) ([]string, []string, error) {
	var conditions []string
	var logicalOps []string
	// Regex matches 'and' or 'or' with surrounding whitespace
	delimiter := regexp.MustCompile(`\s+(and|or)\s+`)
	depth := 0
	inQuotes := false
	var currentCondition strings.Builder

	// Iterate over characters manually
	for index := 0; index < len(filterExpr); {
		currChar := filterExpr[index]
		if currChar == '"' {
			// Toggle quote state
			inQuotes = !inQuotes
			currentCondition.WriteByte(currChar)
			index++
			continue
		}
		if !inQuotes {
			if currChar == '(' {
				depth++
				currentCondition.WriteByte(currChar)
				index++
				continue
			}
			if currChar == ')' {
				if depth > 0 {
					depth--
				}
				currentCondition.WriteByte(currChar)
				index++
				continue
			}
			if depth == 0 {
				remainingSubstr := filterExpr[index:]
				// If a delimiter matches at this position, split here
				if match := delimiter.FindStringIndex(remainingSubstr); match != nil && match[0] == 0 {
					// Extract the logical operator (and/or)
					operatorMatch := delimiter.FindStringSubmatch(remainingSubstr)
					logicalOps = append(logicalOps, operatorMatch[1])
					// Save the condition built so far
					conditions = append(conditions, strings.TrimSpace(currentCondition.String()))
					currentCondition.Reset()
					// Advance index skip the matched operator
					index += match[1]
					continue
				}
			}
		}
		// Otherwise, accumulate character into current condition
		currentCondition.WriteByte(currChar)
		index++
	}

	// Append the final condition
	conditions = append(conditions, strings.TrimSpace(currentCondition.String()))
	if len(conditions) == 0 {
		return nil, nil, fmt.Errorf("no valid conditions found")
	}
	return conditions, logicalOps, nil
}

var conditionPattern = regexp.MustCompile(`^\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*(=|!=|>|>=|<|<=)\s*("([^"]*)"|(\S+))\s*$`)

// parsePostgresCondition parses a single condition for PostgreSQL
func parsePostgresCondition(cond string) (string, error) {
	// Handle expressions wrapped in level 1 parentheses
	if strings.HasPrefix(cond, "(") && strings.HasSuffix(cond, ")") {
		inner := cond[1 : len(cond)-1]
		parsedCondition, err := ParseFilter(inner, "postgres")
		if err != nil {
			return "", fmt.Errorf("invalid parenthesized condition '%s': %w", cond, err)
		}
		return fmt.Sprintf("(%s)", parsedCondition), nil
	}

	matches := conditionPattern.FindStringSubmatch(cond)
	if matches == nil {
		return "", fmt.Errorf("invalid filter format: %s", cond)
	}

	column := matches[1]
	operator := matches[2]
	rawValue := matches[3]

	// Handle quoted strings by escaping single quotes
	if strings.HasPrefix(rawValue, `"`) && strings.HasSuffix(rawValue, `"`) {
		rawValue = rawValue[1 : len(rawValue)-1]
		rawValue = "'" + escapeString(rawValue) + "'"
	} else {
		if num, err := strconv.ParseFloat(rawValue, 64); err == nil {
			rawValue = fmt.Sprintf("%v", num)
		} else if rawValue == "true" || rawValue == "false" {
			rawValue = fmt.Sprintf("%s::boolean", rawValue)
		} else {
			// Reject unquoted strings to prevent SQL injection
			return "", fmt.Errorf("unquoted string values are not allowed: %s", rawValue)
		}
	}
	// Quote column names with double quotes per PG convention
	return fmt.Sprintf(`"%s" %s %s`, column, operator, rawValue), nil
}

// parseMySQLCondition parses a single condition for MySQL
func parseMySQLCondition(cond string) (string, error) {
	// Handle expressions wrapped in level 1 parentheses
	if strings.HasPrefix(cond, "(") && strings.HasSuffix(cond, ")") {
		inner := cond[1 : len(cond)-1]
		parsedCondition, err := ParseFilter(inner, "postgres")
		if err != nil {
			return "", fmt.Errorf("invalid parenthesized condition '%s': %w", cond, err)
		}
		return fmt.Sprintf("(%s)", parsedCondition), nil
	}

	match := conditionPattern.FindStringSubmatch(cond)
	if match == nil {
		return "", fmt.Errorf("could not parse condition")
	}

	column := match[1]
	operator := match[2]
	rawValue := match[3]

	// Handle quoted string for values
	if strings.HasPrefix(rawValue, "\"") && strings.HasSuffix(rawValue, "\"") {
		rawValue := rawValue[1 : len(rawValue)-1]
		escaped := escapeString(rawValue)
		return fmt.Sprintf("`%s` %s '%s'", column, operator, escaped), nil
	}
	if num, err := strconv.ParseFloat(rawValue, 64); err == nil {
		return fmt.Sprintf("`%s` %s %v", column, operator, num), nil
	}
	lower := strings.ToLower(rawValue)
	if lower == "true" || lower == "false" {
		return fmt.Sprintf("`%s` %s %s", column, operator, lower), nil
	}
	return "", fmt.Errorf("unsupported literal: %s", rawValue)
}

// escapeString escapes single quotes
func escapeString(s string) string {
	return strings.Replace(s, "'", "''", -1)
}
