package driver

const (
	// get all schemas and table
	getPrivilegedTablesTmpl = `SELECT nspname as table_schema,
relname as table_name
FROM pg_class c
JOIN pg_namespace n ON c.relnamespace = n.oid
WHERE has_table_privilege(c.oid, 'SELECT')
AND has_schema_privilege(current_user, nspname, 'USAGE')
AND relkind IN ('r', 'm', 'v', 't', 'f', 'p')
AND nspname NOT LIKE 'pg_%'  -- Exclude default system schemas
AND nspname != 'information_schema';  -- Exclude information_schema`
	// get table schema
	getTableSchemaTmpl = `SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position`
	// get primary key columns
	getTablePrimaryKey = `SELECT column_name FROM information_schema.key_column_usage WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position`
)
