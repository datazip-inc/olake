package jdbc

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/jmoiron/sqlx"
)

// MinMaxQuery returns the query to fetch MIN and MAX values of a column in a table
func MinMaxQuery(stream protocol.Stream, column string) string {
	return fmt.Sprintf(`SELECT MIN(%[1]s) AS min_value, MAX(%[1]s) AS max_value FROM %[2]s.%[3]s`, column, stream.Namespace(), stream.Name())
}

// NextChunkEndQuery returns the query to calculate the next chunk boundary
func NextChunkEndQuery(stream protocol.Stream, column string, chunkSize int) string {
	return fmt.Sprintf(`SELECT MAX(%[1]s) FROM (SELECT %[1]s FROM %[2]s.%[3]s WHERE %[1]s > ? ORDER BY %[1]s LIMIT %[4]d) AS subquery`, column, stream.Namespace(), stream.Name(), chunkSize)
}

// buildChunkCondition builds the condition for a chunk
func buildChunkCondition(filterColumn string, chunk types.Chunk, filter string) string {
	condition := ""
	if chunk.Min != nil && chunk.Max != nil {
		condition = fmt.Sprintf("%s >= %v AND %s < %v", filterColumn, chunk.Min, filterColumn, chunk.Max)
	} else if chunk.Min != nil {
		condition = fmt.Sprintf("%s >= %v", filterColumn, chunk.Min)
	} else if chunk.Max != nil {
		condition = fmt.Sprintf("%s < %v", filterColumn, chunk.Max)
	}

	if filter != "" {
		if condition != "" {
			condition = fmt.Sprintf("(%s) AND (%s)", condition, filter)
		} else {
			condition = filter
		}
	}
	return condition
}

// PostgreSQL-Specific Queries
// TODO: Rewrite queries for taking vars as arguments while execution.

// PostgresWithoutState returns the query for a simple SELECT without state
func PostgresWithoutState(stream protocol.Stream) string {
	return fmt.Sprintf(`SELECT * FROM "%s"."%s" ORDER BY %s`, stream.Namespace(), stream.Name(), stream.Cursor())
}

// PostgresWithState returns the query for a SELECT with state
func PostgresWithState(stream protocol.Stream) string {
	return fmt.Sprintf(`SELECT * FROM "%s"."%s" where "%s">$1 ORDER BY "%s" ASC NULLS FIRST`, stream.Namespace(), stream.Name(), stream.Cursor(), stream.Cursor())
}

// PostgresRowCountQuery returns the query to fetch the estimated row count in PostgreSQL
func PostgresRowCountQuery(stream protocol.Stream) string {
	return fmt.Sprintf(`SELECT reltuples::bigint AS approx_row_count FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = '%s' AND n.nspname = '%s';`, stream.Name(), stream.Namespace())
}

// PostgresRelPageCount returns the query to fetch relation page count in PostgreSQL
func PostgresRelPageCount(stream protocol.Stream) string {
	return fmt.Sprintf(`SELECT relpages FROM pg_class WHERE relname = '%s' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '%s')`, stream.Name(), stream.Namespace())
}

// PostgresWalLSNQuery returns the query to fetch the current WAL LSN in PostgreSQL
func PostgresWalLSNQuery() string {
	return `SELECT pg_current_wal_lsn()::text::pg_lsn`
}

// PostgresNextChunkEndQuery generates a SQL query to fetch the maximum value of a specified column
func PostgresNextChunkEndQuery(stream protocol.Stream, filterColumn string, filterValue interface{}, batchSize int, filter string) string {
	baseCond := fmt.Sprintf(`%s > %v`, filterColumn, filterValue)
	if filter != "" {
		baseCond = fmt.Sprintf(`(%s) AND (%s)`, baseCond, filter)
	}

	return fmt.Sprintf(`
        SELECT MAX(%[1]s)
          FROM (
                SELECT %[1]s
                  FROM "%[2]s"."%[3]s"
                 WHERE %[4]s
              ORDER BY %[1]s ASC
                 LIMIT %[5]d
               ) AS T
    `,
		filterColumn,
		stream.Namespace(),
		stream.Name(),
		baseCond,
		batchSize,
	)
	// return fmt.Sprintf(`SELECT MAX(%s) FROM (SELECT %s FROM "%s"."%s" WHERE %s > %v ORDER BY %s ASC LIMIT %d) AS T`, filterColumn, filterColumn, stream.Namespace(), stream.Name(), filterColumn, filterValue, filterColumn, batchSize)
}

// PostgresMinQuery returns the query to fetch the minimum value of a column in PostgreSQL
func PostgresMinQuery(stream protocol.Stream, filterColumn string, filterValue interface{}) string {
	return fmt.Sprintf(`SELECT MIN(%s) FROM "%s"."%s" WHERE %s > %v`, filterColumn, stream.Namespace(), stream.Name(), filterColumn, filterValue)
}

// PostgresBuildSplitScanQuery builds a chunk scan query for PostgreSQL
func PostgresChunkScanQuery(stream protocol.Stream, filterColumn string, chunk types.Chunk, filter string) string {
	condition := buildChunkCondition(filterColumn, chunk, filter)
	return fmt.Sprintf(`SELECT * FROM "%s"."%s" WHERE %s`, stream.Namespace(), stream.Name(), condition)
}

// MySQL-Specific Queries

// MySQLWithoutState builds a chunk scan query for MySql
func MysqlChunkScanQuery(stream protocol.Stream, filterColumn string, chunk types.Chunk, filter string) string {
	condition := buildChunkCondition(filterColumn, chunk, filter)
	return fmt.Sprintf("SELECT * FROM `%s`.`%s` WHERE %s", stream.Namespace(), stream.Name(), condition)
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
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if rerr := tx.Rollback(); rerr != nil && rerr != sql.ErrTxDone {
			fmt.Printf("transaction rollback failed: %v\n", rerr)
		}
	}()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// ParseFilter converts a filter string to a valid PostgreSQL WHERE condition
func ParseFilter(filter string) (string, error) {
	if filter == "" {
		return "", nil
	}

	// Split by 'and' and 'or' while preserving the operators
	conditions, operators, err := splitFilterConditions(filter)
	if err != nil {
		return "", err
	}

	// Parse each condition
	var parsedConditions []string
	for _, condition := range conditions {
		parsed, err := parseEachCondition(strings.TrimSpace(condition))
		if err != nil {
			return "", err
		}
		parsedConditions = append(parsedConditions, parsed)
	}

	// Combine conditions with operators
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

	// Regular expression to find 'and' or 'or' that are not inside quotes
	pattern := regexp.MustCompile(`\s+(and|or)\s+`)

	// Track quote state to avoid splitting inside quoted strings
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
			// Check for 'and' or 'or'
			remaining := filter[i:]
			if match := pattern.FindStringIndex(remaining); match != nil && match[0] == 0 {
				// Found an operator at current position
				opMatch := pattern.FindStringSubmatch(remaining)
				operator := opMatch[1]

				// Add current condition
				conditions = append(conditions, strings.TrimSpace(currentCondition.String()))
				operators = append(operators, operator)

				// Reset for next condition
				currentCondition.Reset()
				i += match[1] // Skip past the operator
				continue
			}
		}

		currentCondition.WriteByte(char)
		i++
	}

	// Add the last condition
	conditions = append(conditions, strings.TrimSpace(currentCondition.String()))

	if len(conditions) == 0 {
		return nil, nil, fmt.Errorf("no valid conditions found")
	}

	return conditions, operators, nil
}

// parseEachCondition parses a single condition (original logic)
func parseEachCondition(condition string) (string, error) {
	// Regular expression to match filter components
	pattern := regexp.MustCompile(`^\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*(=|!=|>|>=|<|<=)\s*("([^"]*)"|(\S+))\s*$`)
	matches := pattern.FindStringSubmatch(condition)
	if matches == nil {
		return "", fmt.Errorf("invalid filter format: %s", condition)
	}

	column := matches[1]
	operator := matches[2]
	value := matches[3]

	// Handle quoted values
	if strings.HasPrefix(value, `"`) && strings.HasSuffix(value, `"`) {
		value = value[1 : len(value)-1]
		value = "'" + escapeString(value) + "'"
	} else {
		// Try to parse as number or boolean
		if num, err := strconv.ParseFloat(value, 64); err == nil {
			value = fmt.Sprintf("%v", num)
		} else if value == "true" || value == "false" {
			// Keep as is
			value = fmt.Sprintf("%s::boolean", value)
		} else {
			return "", fmt.Errorf("unquoted string values are not allowed: %s", value)
		}
	}

	return fmt.Sprintf(`"%s" %s %s`, column, operator, value), nil
}

// escapeString escapes single quotes for PostgreSQL
func escapeString(s string) string {
	return strings.Replace(s, "'", "''", -1)
}
