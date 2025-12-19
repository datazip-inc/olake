package driver

import (
	"fmt"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils"
)

type Config struct {
	Host             string            `json:"host"`
	Port             int               `json:"port"`
	Database         string            `json:"database"`
	Username         string            `json:"username"`
	Password         string            `json:"password"`
	JDBCURLParams    map[string]string `json:"jdbc_url_params"`
	SSLConfiguration *utils.SSLConfig  `json:"ssl"`
	MaxThreads       int               `json:"max_threads"`
	RetryCount       int               `json:"retry_count"`
}

func (c *Config) BuildDSN() string {
	// Base DSN
	dsn := fmt.Sprintf(
		"HOSTNAME=%s;PORT=%d;DATABASE=%s;UID=%s;PWD=%s;TxnIsolation=4;",
		c.Host,
		c.Port,
		c.Database,
		c.Username,
		c.Password,
	)

	// JDBC-style params if provided
	for k, v := range c.JDBCURLParams {
		dsn += fmt.Sprintf(";%s=%s", k, v)
	}

	// SSL if enabled
	if c.SSLConfiguration != nil && c.SSLConfiguration.Mode != "disable" {
		dsn += ";SECURITY=SSL"
		// TODO: Add support for more SSL params
	}

	return dsn
}

func (c *Config) Validate() error {
	if c.Host == "" {
		return fmt.Errorf("empty host name")
	}

	if c.Port <= 0 || c.Port > 65535 {
		return fmt.Errorf("invalid port number: must be between 1 and 65535")
	}

	if c.Username == "" {
		return fmt.Errorf("username is required")
	}

	if c.Database == "" {
		return fmt.Errorf("database name is required")
	}

	if c.MaxThreads <= 0 {
		c.MaxThreads = constants.DefaultThreadCount
	}

	if c.SSLConfiguration == nil {
		c.SSLConfiguration = &utils.SSLConfig{
			Mode: "disable",
		}
	}

	if err := c.SSLConfiguration.Validate(); err != nil {
		return fmt.Errorf("invalid SSL configuration: %w", err)
	}

	return nil
}
