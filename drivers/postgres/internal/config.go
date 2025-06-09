package driver

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/lib/pq"
)

type Config struct {
	// Connection
	//
	// @jsonSchema(
	//   title="Connection",
	//   description="Connection URL",
	//   type="string"
	// )
	Connection *url.URL `json:"-"`

	// Host
	//
	// @jsonSchema(
	//   title="Postgres Host",
	//   description="Hostname or IP address of the PostgreSQL server",
	//   type="string",
	//   required=true
	// )
	Host string `json:"host"`

	// Port
	//
	// @jsonSchema(
	//   title="Postgres Port",
	//   description="Port number of the PostgreSQL server",
	//   type="integer",
	//   default=5432,
	//   required=true
	// )
	Port int `json:"port"`

	// Database
	//
	// @jsonSchema(
	//   title="Database Name",
	//   description="Name of the PostgreSQL database",
	//   type="string",
	//   required=true
	// )
	Database string `json:"database"`

	// Username
	//
	// @jsonSchema(
	//   title="Username",
	//   description="Database user for authentication",
	//   type="string",
	//   required=true
	// )
	Username string `json:"username"`

	// Password
	//
	// @jsonSchema(
	//   title="Password",
	//   description="Password for the database user",
	//   type="string",
	//   format="password",
	//   required=true
	// )
	Password string `json:"password"`

	// JDBCURLParams
	//
	// @jsonSchema(
	//   title="JDBC URL Parameters",
	//   description="Optional JDBC parameters as key-value pairs",
	//   type="string"
	// )
	JDBCURLParams map[string]string `json:"jdbc_url_params"`

	// SSLConfiguration
	//
	// @jsonSchema(
	//   title="SSL Configuration",
	//   type="object",
	//   properties={
	//     "mode": {
	//       "type": "string",
	//       "title": "SSL Mode",
	//       "description": "SSL mode to connect (disable, require, verify-ca, etc.)",
	//       "enum": ["disable", "require", "verify-ca", "verify-full"],
	//       "default": "disable"
	//     }
	//   }
	// )
	SSLConfiguration *utils.SSLConfig `json:"ssl"`

	// UpdateMethod
	//
	// @jsonSchema(
	//   title="Update Method",
	//   type="object",
	//   properties={
	//     "replication_slot": {
	//       "type": "string",
	//       "title": "Replication Slot",
	//       "description": "Slot name for CDC",
	//       "default": "postgres_slot"
	//     },
	//     "intial_wait_time": {
	//       "type": "integer",
	//       "title": "Initial Wait Time",
	//       "description": "Seconds to wait before starting CDC",
	//       "default": 10
	//     }
	//   }
	// )
	UpdateMethod CDC `json:"update_method"`

	// DefaultSyncMode
	//
	// @jsonSchema(
	//   title="Default Mode",
	//   description="Extraction mode (e.g., full or cdc)",
	//   type="string",
	//   default="cdc"
	// )
	DefaultSyncMode types.SyncMode `json:"default_mode"`

	// BatchSize
	//
	// @jsonSchema(
	//   title="Reader Batch Size",
	//   description="Number of records to read in each batch",
	//   type="integer",
	//   default=100000
	// )
	BatchSize int `json:"reader_batch_size"`

	// MaxThreads
	//
	// @jsonSchema(
	//   title="Max Threads",
	//   description="Number of threads to use for backfill",
	//   type="integer",
	//   default=5
	// )
	MaxThreads int `json:"max_threads"`
}

// Capture Write Ahead Logs
type CDC struct {
	// ReplicationSlot
	//
	// @jsonSchema(
	// title="Replication Slot",
	// description="Slot name for CDC",
	// type="string",
	// default="postgres_slot"
	// )
	ReplicationSlot string `json:"replication_slot"`

	// InitialWaitTime
	//
	// @jsonSchema(
	//   title="Initial Wait Time",
	//   description="Seconds to wait before starting CDC",
	//   type="integer",
	//   default=10
	// )
	InitialWaitTime int `json:"intial_wait_time"`
}

func (c *Config) Validate() error {
	if c.Host == "" {
		return fmt.Errorf("empty host name")
	} else if strings.Contains(c.Host, "https") || strings.Contains(c.Host, "http") {
		return fmt.Errorf("host should not contain http or https")
	}

	// Validate port
	if c.Port <= 0 || c.Port > 65535 {
		return fmt.Errorf("invalid port number: must be between 1 and 65535")
	}

	// Set default values if not provided
	if c.BatchSize <= 0 {
		c.BatchSize = 10000 // default batch size
	}

	// default number of threads
	if c.MaxThreads <= 0 {
		c.MaxThreads = 2
	}

	// construct the connection string
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", url.QueryEscape(c.Username), url.QueryEscape(c.Password), c.Host, c.Port, url.QueryEscape(c.Database))
	parsed, err := url.Parse(connStr)
	if err != nil {
		return fmt.Errorf("failed to parse connection string: %s", err)
	}

	query := parsed.Query()

	// Set additional connection parameters if available
	if len(c.JDBCURLParams) > 0 {
		params := ""
		for k, v := range c.JDBCURLParams {
			params += fmt.Sprintf("%s=%s ", pq.QuoteIdentifier(k), pq.QuoteLiteral(v))
		}

		query.Add("options", params)
	}

	if c.SSLConfiguration == nil {
		c.SSLConfiguration = &utils.SSLConfig{
			Mode: "disable",
		}
	}

	sslmode := string(c.SSLConfiguration.Mode)
	if sslmode != "" {
		query.Add("sslmode", sslmode)
	}

	err = c.SSLConfiguration.Validate()
	if err != nil {
		return fmt.Errorf("failed to validate ssl config: %s", err)
	}

	if c.SSLConfiguration.ServerCA != "" {
		query.Add("sslrootcert", c.SSLConfiguration.ServerCA)
	}

	if c.SSLConfiguration.ClientCert != "" {
		query.Add("sslcert", c.SSLConfiguration.ClientCert)
	}

	if c.SSLConfiguration.ClientKey != "" {
		query.Add("sslkey", c.SSLConfiguration.ClientKey)
	}

	parsed.RawQuery = query.Encode()
	c.Connection = parsed

	return nil
}

type Table struct {
	Schema string `db:"table_schema"`
	Name   string `db:"table_name"`
}

type ColumnDetails struct {
	Name       string  `db:"column_name"`
	DataType   *string `db:"data_type"`
	IsNullable *string `db:"is_nullable"`
}
