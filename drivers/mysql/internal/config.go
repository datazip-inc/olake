package driver

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/datazip-inc/olake/drivers/base"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
)

type Config struct {
	// Host
	//
	// @jsonSchema(
	//   title="MySQL Host",
	//   description="Comma-separated list of MySQL hosts",
	//   type="string",
	//   default="mysql-host",
	//   order=1
	// )
	Host string `json:"hosts"`

	// Username
	//
	// @jsonSchema(
	//   title="Username",
	//   description="MySQL username",
	//   type="string",
	//   default="mysql-user",
	//   order=4
	// )
	Username string `json:"username"`

	// Password
	//
	// @jsonSchema(
	//   title="Password",
	//   description="Password for the MySQL user",
	//   type="string",
	//   format="password",
	//   default="mysql-password",
	//   order=5
	// )
	Password string `json:"password"`

	// Database
	//
	// @jsonSchema(
	//   title="Database",
	//   description="Target MySQL database name",
	//   type="string",
	//   default="mysql-database",
	//   order=3
	// )
	Database string `json:"database"`

	// Port
	//
	// @jsonSchema(
	//   title="Port",
	//   description="Port number for MySQL",
	//   type="integer",
	//   default=3306,
	//   order=2
	// )
	Port int `json:"port"`

	// TLSSkipVerify
	//
	// @jsonSchema(
	//   title="Skip TLS Verification",
	//   description="Whether to skip TLS certificate verification",
	//   type="boolean",
	//   default=true,
	//   order=10
	// )
	TLSSkipVerify bool `json:"tls_skip_verify"`

	// UpdateMethod
	//
	// @jsonSchema(
	//   title="Update Method",
	//   description="Method to use for updates",
	//   oneOf=["CDC","FullRefresh"],
	//   order=6
	// )
	UpdateMethod interface{} `json:"update_method"`

	// DefaultMode
	//
	// @jsonSchema(
	//   title="Default Mode",
	//   description="Extraction mode (e.g., full or cdc)",
	//   type="string",
	//   default="cdc",
	//   order=7
	// )
	DefaultMode types.SyncMode `json:"default_mode"`

	// MaxThreads
	//
	// @jsonSchema(
	//   title="Max Threads",
	//   description="Number of parallel threads",
	//   type="integer",
	//   default=5,
	//   order=8
	// )
	MaxThreads int `json:"max_threads"`

	// RetryCount
	//
	// @jsonSchema(
	//   title="Backoff Retry Count",
	//   description="Retry attempts before failing",
	//   type="integer",
	//   default=2,
	//   order=9
	// )
	RetryCount int `json:"backoff_retry_count"`
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
	// InitialWaitTime
	//
	// @jsonSchema(
	//   title="Initial Wait Time",
	//   description="Wait time in seconds before retrying",
	//   type="integer",
	//   default=10
	// )
	InitialWaitTime int `json:"intial_wait_time"`
}

// FullRefresh represents the full refresh configuration
//
// @jsonSchema(
//
//	title="Full Refresh",
//	description="Full Refresh configuration"
//
// )
type FullRefresh struct{}

// URI generates the connection URI for the MySQL database
func (c *Config) URI() string {
	// Set default port if not specified
	if c.Port == 0 {
		c.Port = 3306
	}
	// Construct host string
	hostStr := c.Host
	if c.Host == "" {
		hostStr = "localhost"
	}

	// Construct full connection string
	return fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s",
		url.QueryEscape(c.Username),
		url.QueryEscape(c.Password),
		hostStr,
		c.Port,
		url.QueryEscape(c.Database),
	)
}

// Validate checks the configuration for any missing or invalid fields
func (c *Config) Validate() error {
	if c.Host == "" {
		return fmt.Errorf("empty host name")
	} else if strings.Contains(c.Host, "https") || strings.Contains(c.Host, "http") {
		return fmt.Errorf("host should not contain http or https: %s", c.Host)
	}

	// Validate port
	if c.Port <= 0 || c.Port > 65535 {
		return fmt.Errorf("invalid port number: must be between 1 and 65535")
	}

	// Validate required fields
	if c.Username == "" {
		return fmt.Errorf("username is required")
	}
	if c.Password == "" {
		return fmt.Errorf("password is required")
	}

	// Optional database name, default to 'mysql'
	if c.Database == "" {
		c.Database = "mysql"
	}

	// Set default number of threads if not provided
	if c.MaxThreads <= 0 {
		c.MaxThreads = base.DefaultThreadCount // Aligned with PostgreSQL default
	}

	// Set default retry count if not provided
	if c.RetryCount <= 0 {
		c.RetryCount = base.DefaultRetryCount // Reasonable default for retries
	}

	return utils.Validate(c)
}
