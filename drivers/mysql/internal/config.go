package driver

import (
	"fmt"
	"strings"

	"github.com/go-sql-driver/mysql"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils"
)

type Config struct {
	// Host
	//
	// @jsonSchema(
	//   title="MySQL Host",
	//   description="Database host addresses for connection",
	//   type="string",
	//   required=true
	// )
	Host string `json:"hosts"`

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

	// Database
	//
	// @jsonSchema(
	//   title="Database",
	//   description="Name of the database to use for connection",
	//   type="string",
	//   required=true
	// )
	Database string `json:"database"`

	// Port
	//
	// @jsonSchema(
	//   title="Port",
	//   description="Database server listening port",
	//   type="integer",
	//   required=true
	// )
	Port int `json:"port"`

	// TLSSkipVerify
	//
	// @jsonSchema(
	//   title="Skip TLS Verification",
	//   description="Determines if TLS certificate verification should be skipped for secure connections",
	//   type="boolean",
	//   default=true
	// )
	TLSSkipVerify bool `json:"tls_skip_verify"`

	// UpdateMethod
	//
	// @jsonSchema(
	//   title="Update Method",
	//   description="Method to use for updates",
	//   oneOf=["CDC","FullRefresh"]
	// )
	UpdateMethod interface{} `json:"update_method"`

	// MaxThreads
	//s
	// @jsonSchema(
	//   title="Max Threads",
	//   description="Maximum concurrent threads for data sync",
	//   type="integer",
	//   default=3
	// )
	MaxThreads int `json:"max_threads"`

	// RetryCount
	//
	// @jsonSchema(
	//   title="Backoff Retry Count",
	//   description="Number of sync retries (exponential backoff on failure)",
	//   type="integer",
	//   default=3
	// )
	RetryCount int `json:"backoff_retry_count"`
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
	// InitialWaitTime
	//
	// @jsonSchema(
	//   title="Initial Wait Time",
	//   description="Idle timeout for Bin log reading",
	//   type="integer",
	//   default=120,
	//   required=true
	// )
	InitialWaitTime int `json:"initial_wait_time"`
}

// FullRefresh represents the full refresh configuration
//
// @jsonSchema(
//
//	title="Full Refresh",
//	additionalProperties=false
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

	cfg := mysql.Config{
		User:                 c.Username,
		Passwd:               c.Password,
		Net:                  "tcp",
		Addr:                 fmt.Sprintf("%s:%d", hostStr, c.Port),
		DBName:               c.Database,
		AllowNativePasswords: true,
	}

	return cfg.FormatDSN()
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
		c.MaxThreads = constants.DefaultThreadCount // Aligned with PostgreSQL default
	}

	// Set default retry count if not provided
	if c.RetryCount <= 0 {
		c.RetryCount = constants.DefaultRetryCount // Reasonable default for retries
	}

	return utils.Validate(c)
}
