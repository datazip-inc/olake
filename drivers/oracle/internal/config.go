package driver

import (
	"fmt"
	"strings"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils"
)

// Config represents the configuration for connecting to an Oracle database
type Config struct {
	Host          string      `json:"hosts"`
	Username      string      `json:"username"`
	Password      string      `json:"password"`
	Database      string      `json:"database"`
	Port          int         `json:"port"`
	ServiceName   string      `json:"service_name"`
	SID           string      `json:"sid"`
	TLSSkipVerify bool        `json:"tls_skip_verify"`
	UpdateMethod  interface{} `json:"update_method"`
	MaxThreads    int         `json:"max_threads"`
	RetryCount    int         `json:"backoff_retry_count"`
}

// CDC represents Oracle CDC configuration
type CDC struct {
	InitialWaitTime int `json:"intial_wait_time"`
}

// URI generates the connection URI for the Oracle database
func (c *Config) URI() string {
	// Set default port if not specified
	if c.Port == 0 {
		c.Port = 1521
	}
	
	// Construct host string
	hostStr := c.Host
	if c.Host == "" {
		hostStr = "localhost"
	}

	// Build connection string
	connStr := fmt.Sprintf("oracle://%s:%s@%s:%d", c.Username, c.Password, hostStr, c.Port)
	
	// Add service name or SID
	if c.ServiceName != "" {
		connStr += "/" + c.ServiceName
	} else if c.SID != "" {
		connStr += "/" + c.SID
	} else {
		// Default to XE service for Oracle Express Edition
		connStr += "/XE"
	}

	return connStr
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

	// Set default database name if not provided
	if c.Database == "" {
		c.Database = "ORCL"
	}

	// Set default number of threads if not provided
	if c.MaxThreads <= 0 {
		c.MaxThreads = constants.DefaultThreadCount
	}

	// Set default retry count if not provided
	if c.RetryCount <= 0 {
		c.RetryCount = constants.DefaultRetryCount
	}

	return utils.Validate(c)
} 