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
	Connection *url.URL `json:"-"`

	// Host
	//
	// @jsonSchema(
	//   title="Postgres Host",
	//   description="Database host address for connection",
	//   type="string",
	//   required=true,
	//   order=1
	// )
	Host string `json:"host"`

	// Port
	//
	// @jsonSchema(
	//   title="Postgres Port",
	//   description="Database server listening port",
	//   type="integer",
	//   default=5432,
	//   required=true,
	//   order=2
	// )
	Port int `json:"port"`

	// Database
	//
	// @jsonSchema(
	//   title="Database Name",
	//   description="Name of the database to use for connection",
	//   type="string",
	//   required=true,
	//   order=3
	// )
	Database string `json:"database"`

	// Username
	//
	// @jsonSchema(
	//   title="Username",
	//   description="Username used to authenticate with the database",
	//   type="string",
	//   required=true,
	//   order=4
	// )
	Username string `json:"username"`

	// Password
	//
	// @jsonSchema(
	//   title="Password",
	//   description="Password for database authentication",
	//   type="string",
	//   format="password",
	//   required=true,
	//   order=5
	// )
	Password string `json:"password"`

	// JDBCURLParams
	//
	// @jsonSchema(
	//   title="JDBC URL Parameters",
	//   description="Additional JDBC URL parameters for connection tuning (optional)",
	//   type="string",
	//   order=6
	// )
	JDBCURLParams map[string]string `json:"jdbc_url_params"`

	// SSLConfiguration
	//
	// @jsonSchema(
	//   title="SSL Configuration",
	//   description="Database connection SSL configuration (e.g., SSL mode)",
	//   order=7
	// )
	SSLConfiguration *utils.SSLConfig `json:"ssl"`

	// @jsonSchema(
	//   title="Update Method",
	//   description="Method to use for updates (CDC - Change Data Capture or Full Refresh)",
	//   oneOf=["CDC","FullRefresh"],
	//   order=8
	// )
	UpdateMethod interface{} `json:"update_method"`

	// DefaultSyncMode
	//
	// @jsonSchema(
	//   title="Default Mode",
	//   description="Default sync mode (CDC - Change Data Capture or Full Refresh)",
	//   type="string",
	//   default="cdc",
	//   order=10
	// )
	DefaultSyncMode types.SyncMode `json:"default_mode"`

	// BatchSize
	//
	// @jsonSchema(
	//   title="Reader Batch Size",
	//   description="Max batch size for read operations",
	//   type="integer",
	//   default=100000,
	//   order=9
	// )
	BatchSize int `json:"reader_batch_size"`

	// MaxThreads
	//
	// @jsonSchema(
	//   title="Max Threads",
	//   description="Max parallel threads for chunk snapshotting",
	//   type="integer",
	//   default=5,
	//   order=11
	// )
	MaxThreads int `json:"max_threads"`
	RetryCount int `json:"retry_count"`
}

// CDC represents the Change Data Capture configuration
//
// @jsonSchema(
//
//	title="CDC",
//	description="Change Data Capture configuration"
//
// )
type CDC struct {
	// ReplicationSlot
	//
	// @jsonSchema(
	// title="Replication Slot",
	// description="Slot to retain WAL logs for consistent replication",
	// type="string",
	// default="postgres_slot"
	// )
	ReplicationSlot string `json:"replication_slot"`

	// InitialWaitTime
	//
	// @jsonSchema(
	//   title="Initial Wait Time",
	//   description="Idle timeout for WAL log reading",
	//   type="integer",
	//   default=10
	// )
	InitialWaitTime int `json:"intial_wait_time"`

	RetryCount int `json:"retry_count"`
}

// FullRefresh represents the full refresh configuration
//
// @jsonSchema(
//
//	title="Full Refresh"
//
// )
type FullRefresh struct{}

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
