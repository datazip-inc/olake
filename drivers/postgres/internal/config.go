package driver

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils"
)

type Config struct {
	Connection *url.URL `json:"-"`

	// Host
	//
	// @jsonSchema(
	//   title="Postgres Host",
	//   description="Database host address for connection",
	//   type="string",
	//   required=true
	// )
	Host string `json:"host"`

	// Port
	//
	// @jsonSchema(
	//   title="Postgres Port",
	//   description="Database server listening port",
	//   type="integer",
	//   required=true
	// )
	Port int `json:"port"`

	// Database
	//
	// @jsonSchema(
	//   title="Database Name",
	//   description="Name of the database to use for connection",
	//   type="string",
	//   required=true
	// )
	Database string `json:"database"`

	// Username
	//
	// @jsonSchema(
	//   title="Username",
	//   description="Username used to authenticate with the database",
	//   type="string",
	//   required=true
	// )
	Username string `json:"username"`

	// Password
	//
	// @jsonSchema(
	//   title="Password",
	//   description="Password for database authentication",
	//   type="string",
	//   format="password",
	//   required=true
	// )
	Password string `json:"password"`

	// JDBCURLParams
	//
	// @jsonSchema(
	//   title="JDBC URL Parameters",
	//   description="Additional JDBC URL parameters for connection tuning (optional)",
	//   additionalProperties="string"
	// )
	JDBCURLParams map[string]string `json:"jdbc_url_params"`

	// SSLConfiguration
	//
	// @jsonSchema(
	//   title="SSL Configuration",
	//   description="Database connection SSL configuration (e.g., SSL mode)"
	// )
	SSLConfiguration *utils.SSLConfig `json:"ssl"`

	// @jsonSchema(
	//   title="Update Method",
	//   description="Method to use for updates (CDC - Change Data Capture or Full Refresh)",
	//   oneOf=["CDC","Standalone"]
	// )
	UpdateMethod interface{} `json:"update_method"`

	// BatchSize
	//
	// @jsonSchema(
	//   title="Reader Batch Size",
	//   description="Max batch size for read operations",
	//   type="integer"
	// )
	BatchSize int `json:"reader_batch_size"`

	// MaxThreads
	//
	// @jsonSchema(
	//   title="Max Threads",
	//   description="Max parallel threads for chunk snapshotting",
	//   type="integer",
	//   default=3
	// )
	MaxThreads int `json:"max_threads"`

	// RetryCount
	//
	// @jsonSchema(
	//   title="Retry Count",
	//   description="Number of sync retry attempts using exponential backoff",
	//   type="integer",
	//   default=3
	// )
	RetryCount int `json:"retry_count"`
}

// CDC represents the Change Data Capture configuration
//
// @jsonSchema(
//
//	title="CDC",
//	description="Change Data Capture configuration",
//	additionalProperties=false
//
// )
type CDC struct {
	// ReplicationSlot
	//
	// @jsonSchema(
	// title="Replication Slot",
	// description="Slot to retain WAL logs for consistent replication",
	// type="string",
	// required=true,
	// default="postgres_slot"
	// )
	ReplicationSlot string `json:"replication_slot"`

	// InitialWaitTime
	//
	// @jsonSchema(
	//   title="Initial Wait Time",
	//   description="Idle timeout for WAL log reading",
	//   type="integer",
	//   required=true
	// )
	InitialWaitTime int `json:"initial_wait_time"`
}

// Standalone represents the standalone configuration
//
// @jsonSchema(
//
//	title="Standalone",
//	additionalProperties=false
//
// )
type Standalone struct{}

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
		c.MaxThreads = constants.DefaultThreadCount
	}

	// Add the connection parameters to the url
	parsed := &url.URL{
		Scheme: "postgres",
		User:   utils.Ternary(c.Password != "", url.UserPassword(c.Username, c.Password), url.User(c.Username)).(*url.Userinfo),
		Host:   fmt.Sprintf("%s:%d", c.Host, c.Port),
		Path:   "/" + c.Database,
	}

	query := parsed.Query()

	// Set additional connection parameters if available
	if len(c.JDBCURLParams) > 0 {
		for k, v := range c.JDBCURLParams {
			query.Add(k, v)
		}
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

	err := c.SSLConfiguration.Validate()
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
