package driver

import (
	"fmt"
	"strings"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils"
)

// Config represents the configuration for connecting to a MSSQL database.
type Config struct {
	Host             string           `json:"host"`
	Port             int              `json:"port"`
	Database         string           `json:"database"`
	Username         string           `json:"username"`
	Password         string           `json:"password"`
	UpdateMethod     interface{}      `json:"update_method"`
	MaxThreads       int              `json:"max_threads"`
	RetryCount       int              `json:"retry_count"`
	SSLConfiguration *utils.SSLConfig `json:"ssl"`
}

// CDC configuration for SQL Server.
type CDC struct {
	InitialWaitTime int `json:"initial_wait_time"`
}

// Validate checks and normalises MSSQL configuration.
func (c *Config) Validate() error {
	if c.Host == "" {
		return fmt.Errorf("empty host name")
	} else if strings.Contains(c.Host, "https") || strings.Contains(c.Host, "http") {
		return fmt.Errorf("host should not contain http or https")
	}

	if c.Port <= 0 || c.Port > 65535 {
		return fmt.Errorf("invalid port number: must be between 1 and 65535")
	}

	if c.Username == "" {
		return fmt.Errorf("username is required")
	}
	if c.Password == "" {
		return fmt.Errorf("password is required")
	}
	if c.Database == "" {
		return fmt.Errorf("database is required")
	}

	if c.MaxThreads <= 0 {
		c.MaxThreads = constants.DefaultThreadCount
	}

	if c.RetryCount <= 0 {
		c.RetryCount = constants.DefaultRetryCount
	}

	if c.SSLConfiguration == nil {
		c.SSLConfiguration = &utils.SSLConfig{
			Mode: utils.SSLModeDisable,
		}
	}

	err := c.SSLConfiguration.Validate()
	if err != nil {
		return fmt.Errorf("failed to validate ssl config: %s", err)
	}

	return utils.Validate(c)
}
