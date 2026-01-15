// uischema is used to serve the UI specifications of the jsonschema of all the drivers and destinations
package spec

import (
	"fmt"
)

var uiSchemaMap = map[string]string{
	"mongodb":  MongoDBUISchema,
	"postgres": PostgresUISchema,
	"mysql":    MySQLUISchema,
	"oracle":   OracleUISchema,
	"s3":       S3UISchema,
	"parquet":  ParquetUISchema,
	"iceberg":  IcebergUISchema,
	"db2":      DB2UISchema,
	"kafka":    KafkaUISchema,
}

const MongoDBUISchema = `{
    "ui:grid": [
        { "hosts": 12, "database": 12 },
        { "authdb": 12, "username": 12 },
        { "password": 12, "replica_set": 12 },
        { "read_preference": 12, "srv": 12 },
        { "max_threads": 12, "backoff_retry_count": 12 },
        { "chunking_strategy": 12, "use_iam": 12 },
        { "ssh_config": 12 }
    ],
    "srv": {
        "ui:widget": "boolean"
    },
    "use_iam": {
        "ui:widget": "boolean"
    },
    "hosts": {
        "ui:options": {
            "label": false
        }
    },
    "ssh_config": {
        "ui:options": {
            "title": false,
            "description": false
        },
        "ui:grid": [
            { "host": 12, "port": 12 },
            { "username": 12, "private_key": 12 },
            { "passphrase": 12, "password": 12 }
        ],
        "private_key": {
            "ui:widget": "textarea",
            "ui:options": {
                "rows": 1
            }
        }
    }
}`

const PostgresUISchema = `{
      "ui:grid": [
        { "host": 12, "database": 12 },
        { "username": 12, "password": 12 },
        { "port": 12, "jdbc_url_params": 12 },
        { "ssl": 12, "max_threads": 12 },
        { "update_method": 12, "retry_count": 12 },
        { "ssh_config": 12 }
      ],
      "ssl": {
        "ui:options": {
          "title": false
        }
      },
      "update_method": {
        "ui:widget": "radio",
        "ui:grid": [{ "replication_slot": 12, "initial_wait_time": 12}, { "publication": 12 }],
        "ui:options": {
          "title": false,
          "description": false
        }
      },
      "ssh_config": {
        "ui:options": {
          "title": false,
          "description": false
        },
        "ui:grid": [
          { "host": 12, "port": 12 },
          { "username": 12, "private_key": 12 },
          { "passphrase": 12, "password": 12 }
        ],
        "private_key": {
          "ui:widget": "textarea",
          "ui:options": {
            "rows": 1
          }
        }
      }
    }`

const MySQLUISchema = `{
  "ui:grid": [
    { "hosts": 12, "database": 12 },
    { "username": 12, "password": 12 },
    { "port": 12, "max_threads": 12 },
    { "backoff_retry_count": 12, "jdbc_url_params": 12 },
    { "ssl": 12, "update_method": 12 },
    { "ssh_config": 12 }
  ],
  "ssl": {
    "ui:options": {
      "title": false,
      "description": false
    },
    "ui:grid": [
      { "mode": 12 },
      { "server_ca": 12 },
      { "client_cert": 12 },
      { "client_key": 12 }
    ],
    "server_ca": {
      "ui:widget": "textarea",
      "ui:options": {
        "rows": 1
      }
    },
    "client_cert": {
      "ui:widget": "textarea",
      "ui:options": {
        "rows": 1
      }
    },
    "client_key": {
      "ui:widget": "textarea",
      "ui:options": {
        "rows": 1
      }
    }
  },
  "update_method": {
    "ui:widget": "radio",
    "ui:options": {
      "title": false,
      "description": false
    },
    "type": {
      "ui:widget": "hidden"
    }
  },
  "ssh_config": {
    "ui:options": {
      "title": false,
      "description": false
    },
    "ui:grid": [
      { "host": 12, "port": 12 },
      { "username": 12, "private_key": 12 },
      { "passphrase": 12, "password": 12 }
    ],
    "private_key": {
      "ui:widget": "textarea",
      "ui:options": {
        "rows": 1
      }
    }
  }
}`
const OracleUISchema = `{
  "ui:grid": [
    { "host": 12, "connection_type": 12 },
    { "username": 12, "sid": 12, "service_name": 12 },
    { "password": 12, "port": 12 },
    { "jdbc_url_params": 12, "ssl": 12 },
    { "max_threads": 12, "backoff_retry_count": 12 },
    { "ssh_config": 12 }
  ],
  "ssl": {
    "ui:options": {
      "title": false,
      "description": false
    }
  },
  "connection_type": {
    "ui:grid": [
      { "sid": 12, "service_name": 12 }
    ],
     "ui:enumNames": [
      "SID",
      "Service Name"
    ]
  },
  "ssh_config": {
    "ui:options": {
      "title": false,
      "description": false
    },
    "ui:grid": [
      { "host": 12, "port": 12 },
      { "username": 12, "private_key": 12 },
      { "passphrase": 12, "password": 12 }
    ],
    "private_key": {
      "ui:widget": "textarea",
      "ui:options": {
        "rows": 1
      }
    }
  }
}`

const S3UISchema = `{
  "ui:grid": [
    { "bucket_name": 12, "region": 12 },
    { "access_key_id": 12, "secret_access_key": 12 },
    { "path_prefix": 12, "endpoint": 12 },
    { "file_pattern": 12, "compression": 12 },
    { "retry_count": 12, "max_threads": 12 },
    { "file_format": 12},
    { "csv": 12, "parquet": 12, "json": 12 }
  ],
  "file_format": {
    "ui:enumNames": [
      "CSV",
      "JSON",
      "Parquet"
    ]
  },
  "csv": {
    "ui:options": {
      "title": false,
      "description": false
    },
    "ui:grid": [
      { "delimiter": 12, "has_header": 12 },
      { "skip_rows": 12, "quote_character": 12 }
    ],
    "has_header": {
      "ui:widget": "boolean"
    }
  },
  "json": {
    "ui:options": {
      "title": false,
      "description": false
    },
    "ui:grid": [
      { "line_delimited": 12 }
    ],
    "line_delimited": {
      "ui:widget": "boolean"
    }
  },
  "parquet": {
    "ui:options": {
      "title": false,
      "description": false
    },
    "ui:grid": [
      { "streaming_enabled": 12 }
    ],
    "streaming_enabled": {
      "ui:widget": "boolean"
    }
  }
}`

const KafkaUISchema = `{
    "ui:grid": [
      { "bootstrap_servers": 12, "consumer_group_id": 12 },
      { "threads_equal_total_partitions": 12, "max_threads": 12 },
      { "backoff_retry_count": 12, "protocol": 12 }
    ],
    "protocol": {
      "ui:grid": [
        { "sasl_mechanism": 12, "sasl_jaas_config": 12 },
        { "tls_skip_verify": 24 },
        { "ssl": 24 }
      ],
      "sasl_jaas_config": {
        "ui:widget": "textarea",
        "ui:options": {
          "rows": 1
        }
      },
      "tls_skip_verify": {
        "ui:widget": "boolean"
      },
      "ssl": {
        "ui:options": {
          "title": false,
          "description": false
        },
        "ui:grid": [
          { "server_ca": 12, "client_cert": 12 },
          { "client_key": 12 }
        ],
        "server_ca": {
          "ui:widget": "textarea",
          "ui:options": {
            "rows": 1
          }
        },
        "client_cert": {
          "ui:widget": "textarea",
          "ui:options": {
            "rows": 1
          }
        },
        "client_key": {
          "ui:widget": "textarea",
          "ui:options": {
            "rows": 1
          }
        }
      },
      "ui:options": {
        "title": false
      }
    },
    "threads_equal_total_partitions": {
      "ui:widget": "boolean"
    }
}`

const ParquetUISchema = `{
  "type": {
    "ui:widget": "hidden"
  },
  "writer": {
    "ui:grid": [
      { "s3_bucket": 12, "s3_region": 12 },
      { "s3_endpoint": 12, "s3_access_key": 12 },
      { "s3_secret_key": 12, "s3_path": 12 }
    ],
    "ui:options": {
      "label": false
    }
  }
}`

const IcebergUISchema = `{
  "type": {
    "ui:widget": "hidden"
  },
  "writer": {
    "ui:grid": [
      { "catalog_type": 12, "catalog_name": 12 },
      { "rest_catalog_url": 12, "hive_uri": 12 },
      { "jdbc_url": 12, "jdbc_username": 12, "jdbc_password": 12 },
      { "iceberg_s3_path": 12, "hive_clients": 12 },
      { "s3_use_ssl": 12, "hive_sasl_enabled": 12 },
      { "s3_path_style": 12, "rest_auth_type": 12 },
      { "token": 12, "oauth2_uri": 12 },
      { "credential": 12, "no_identifier_fields": 12 },
      { "rest_signing_name": 12, "rest_signing_region": 12 },
      { "rest_signing_v_4": 12, "scope": 12, "s3_endpoint": 12 },
      { "aws_access_key": 12, "aws_secret_key": 12 },
      { "aws_region": 12, "arrow_writes": 12 }
    ],
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
    },
    "arrow_writes": {
      "ui:widget": "boolean"
    },
    "catalog_type": {
      "ui:enumNames": [
        "AWS Glue",
        "JDBC",
        "Hive",
        "REST"
      ]
    },
    "ui:options": {
      "label": false
    }
  }
}`

const DB2UISchema = `{
  "ui:grid": [
    { "host": 12, "port": 12 },
    { "database": 12, "max_threads": 12 },
    { "username": 12, "password": 12 },
    { "jdbc_url_params": 12, "retry_count": 12 },
    { "ssl": 12 }, { "ssh_config": 12 }
  ],
  "ssl": {
    "ui:options": {
      "title": false,
      "description": false
    }
  },
  "ssh_config": {
    "ui:options": {
      "title": false,
      "description": false
    },
    "ui:grid": [
      { "host": 12, "port": 12 },
      { "username": 12, "private_key": 12 },
      { "passphrase": 12, "password": 12 }
    ],
    "private_key": {
      "ui:widget": "textarea",
      "ui:options": {
        "rows": 1
      }
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
