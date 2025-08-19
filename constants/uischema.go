// uischema is used to serve the UI specifications of the jsonschema of all the drivers and destinations
package constants

import (
	"fmt"
)

var uiSchemaMap = map[string]string{
	"mongodb":  MongoDBUISchema,
	"postgres": PostgresUISchema,
	"mysql":    MySQLUISchema,
	"oracle":   OracleUISchema,
	"parquet":  ParquetUISchema,
	"iceberg":  IcebergUISchema,
}

const MongoDBUISchema = `{
    "ui:grid": [
        { "hosts": 12, "database": 12 },
        { "authdb": 12, "username": 12 },
        { "password": 12, "replica_set": 12 },
        { "read_preference": 12, "srv": 12 },
        { "max_threads": 12, "backoff_retry_count": 12 },
        { "chunking_strategy": 12 }
    ],
    "srv": {
        "ui:widget": "boolean"
    },
    "hosts": {
        "ui:options": {
            "label": false
        }
    }
}`

const PostgresUISchema = `{
  "ui:grid": [
    { "host": 12, "database": 12 },
    { "username": 12, "password": 12 },
    { "port": 12, "jdbc_url_params": 12 },
    { "ssl": 12, "max_threads": 12 },
    { "reader_batch_size": 12, "update_method": 12 }
  ],
  "ssl": {
    "ui:options": {
      "title": false,
      "description": false
    }
  },
  "update_method": {
    "ui:grid": [
      { "initial_wait_time": 12, "replication_slot": 12 }
    ],
    "ui:options": {
      "title": false,
      "description": false
    },
     "type": {
      "ui:widget": "hidden"
    }
  },
}`

const MySQLUISchema = `{
  "ui:grid": [
    { "hosts": 12, "database": 12 },
    { "username": 12, "password": 12 },
    { "port": 12, "max_threads": 12 },
    { "backoff_retry_count": 12, "tls_skip_verify": 12 },
    { "update_method": 12 }
  ],
  "tls_skip_verify": {
    "ui:widget": "boolean"
  },
  "update_method": {
    "ui:options": {
      "title": false,
      "description": false
    },
    "type": {
      "ui:widget": "hidden"
    }
  }
}`

const OracleUISchema = `{
    "ui:grid": [
        { "host": 12, "username": 12 },
        { "password": 12, "port": 12 },
        { "connection_type": 12, "ssl": 12 },
        { "max_threads": 12, "backoff_retry_count": 12 },
        { "jdbc_url_params": 12 }
    ],
    "ssl": {
        "ui:options": {
            "title": false,
            "description": false
        }
    },
    "connection_type": {
        "ui:options": {
            "title": false,
            "description": false
        }
    }
}`

const ParquetUISchema = `{
    "ui:grid": [
        { "s3_bucket": 12, "s3_region": 12 },
        { "s3_access_key": 12, "s3_secret_key": 12 },
        { "s3_path": 12 }
    ]
}`

const IcebergUISchema = `{
    "ui:grid": [
        { "iceberg_s3_path": 12, "iceberg_db": 12 },
        { "aws_access_key": 12, "aws_secret_key": 12 },
        { "aws_region": 12 },
        { "catalog_type": 24 }
    ],
    "catalog_type": {
        "ui:grid": [
            { "grpc_port": 12, "sink_rpc_server_host": 12 },
            { "rest_catalog_url": 12, "token": 12 },
            { "rest_auth_type": 12, "oauth2_uri": 12 },
            { "credential": 12, "no_identifier_fields": 12 },
            { "rest_signing_name": 12, "rest_signing_region": 12 },
            { "rest_signing_v_4": 12 },
            { "hive_uri": 12, "s3_use_ssl": 12 },
            { "s3_path_style": 12, "hive_clients": 12 },
            { "hive_sasl_enabled": 12 },
            { "jdbc_url": 12, "jdbc_username": 12 },
            { "jdbc_password": 12, "s3_endpoint": 12 }
        ],
        "ui:options": {
            "title": false,
            "description": false
        },
        "no_identifier_fields": {
            "ui:widget": "boolean"
        },
        "rest_signing_v_4": {
            "ui:widget": "boolean"
        },
        "s3_use_ssl": {
            "ui:widget": "boolean"
        },
        "hive_sasl_enabled": {
            "ui:widget": "boolean"
        },
        "s3_path_style": {
            "ui:widget": "boolean"
        }
    }
}`

func LoadUISchema(schemaType string) (string, error) {
	jsonStr, ok := uiSchemaMap[schemaType]
	if !ok {
		return "", fmt.Errorf("schema not found")
	}
	return jsonStr, nil
}
