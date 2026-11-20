package destinationtest

import (
	"context"
	"errors"
	"fmt"

	"github.com/datazip-inc/olake/utils/testutils/testhandler"
)

const (
	jdbcCatalogDatabase = "olake_testhandler_catalog"
	// postgresPassword is the password of drivers/postgres/docker-compose.yml, whose server keeps the JDBC catalog.
	postgresPassword = "secret1234"
)

// jdbcCatalogService is the Postgres server the integration suites share. Ready also creates the catalog's database;
// another process may create it at the same moment, so only its absence is an error.
var jdbcCatalogService = &testhandler.Service{
	Container: "olake_postgres-test",
	Port:      5433,
	Target:    "olake.postgres.start",
	Ready: func(ctx context.Context, s *testhandler.Service) error {
		_, createErr := psql(ctx, s, "postgres", `CREATE DATABASE `+jdbcCatalogDatabase)
		found, err := psql(ctx, s, "postgres", `SELECT count(*) FROM pg_database WHERE datname = '`+jdbcCatalogDatabase+`'`)
		if err != nil || found != "1" {
			return fmt.Errorf("no JDBC catalog database: %v", errors.Join(createErr, err))
		}
		return nil
	},
}

// IcebergJDBC writes Iceberg tables through a JDBC catalog kept in the Postgres container.
var IcebergJDBC testhandler.DestinationKind = &icebergKind{
	name:      "iceberg-jdbc",
	catalog:   jdbcCatalogService,
	warehouse: "s3a://" + bucket + "/iceberg_jdbc",
	config: map[string]any{
		"catalog_type":  "jdbc",
		"jdbc_url":      fmt.Sprintf("jdbc:postgresql://%s/%s", jdbcCatalogService.Addr(), jdbcCatalogDatabase),
		"jdbc_username": "postgres",
		"jdbc_password": postgresPassword,
	},
	dropNamespace: dropJDBCNamespace,
	files: func(namespace string) string {
		return "iceberg_jdbc/" + namespace
	},
}

// dropJDBCNamespace removes a namespace and its tables from the catalog's two tables.
// The catalog creates those tables on first use, so they may not exist yet.
func dropJDBCNamespace(ctx context.Context, namespace string) error {
	_, err := psql(ctx, jdbcCatalogService, jdbcCatalogDatabase, `DO $$ BEGIN
		IF to_regclass('iceberg_tables') IS NOT NULL THEN
			DELETE FROM iceberg_tables WHERE table_namespace = '`+namespace+`';
		END IF;
		IF to_regclass('iceberg_namespace_properties') IS NOT NULL THEN
			DELETE FROM iceberg_namespace_properties WHERE namespace = '`+namespace+`';
		END IF;
	END $$`)
	return err
}

// psql runs statements against one database of the server, each in its own transaction.
func psql(ctx context.Context, s *testhandler.Service, database string, statements ...string) (string, error) {
	command := []string{
		"psql", "--host", "127.0.0.1", "--username", "postgres", "--dbname", database,
		"--no-psqlrc", "--quiet", "--tuples-only", "--no-align", "--set", "ON_ERROR_STOP=1",
	}
	for _, statement := range statements {
		command = append(command, "--command", statement)
	}
	return s.Exec(ctx, nil, command...)
}
