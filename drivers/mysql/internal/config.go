package driver

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/datazip-inc/olake/drivers/base"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
)

// Config represents the configuration for connecting to a MySQL database
type Config struct {
	Host          string         `json:"hosts"`
	Username      string         `json:"username"`
	Password      string         `json:"password"`
	Database      string         `json:"database"`
	Port          int            `json:"port"`
	TLSSkipVerify bool           `json:"tls_skip_verify"` // Add this field
	UpdateMethod  interface{}    `json:"update_method"`
	DefaultMode   types.SyncMode `json:"default_mode"`
	MaxThreads    int            `json:"max_threads"`
	RetryCount    int            `json:"backoff_retry_count"`
}
type CDC struct {
	InitialWaitTime int `json:"intial_wait_time"`
}

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
