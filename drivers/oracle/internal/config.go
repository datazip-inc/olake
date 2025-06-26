package driver

import (
	"fmt"
	"strings"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils"
	go_ora "github.com/sijms/go-ora/v2"
)

type Config struct {
	Host             string            `json:"host"`
	Username         string            `json:"username"`
	Password         string            `json:"password"`
	ServiceName      string            `json:"service_name"`
	Port             int               `json:"port"`
	MaxThreads       int               `json:"max_threads"`
	RetryCount       int               `json:"retry_count"`
	SSLConfiguration *utils.SSLConfig  `json:"ssl"`
	JDBCURLParams    map[string]string `json:"jdbc_url_params"`
}

func (c *Config) connectionString() string {
	urlOptions := map[string]string{}
	// Add JDBC-style URL params
	for k, v := range c.JDBCURLParams {
		urlOptions[k] = v
	}

	// Add SSL params if provided
	if c.SSLConfiguration != nil {
		sslmode := string(c.SSLConfiguration.Mode)
		if sslmode != "disable" {
			urlOptions["ssl"] = "true"
			urlOptions["ssl verify"] = "false"
		}
		// TODO: Add support for more SSL params
	}

	return go_ora.BuildUrl(c.Host, c.Port, c.ServiceName, c.Username, c.Password, urlOptions)
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
	if c.ServiceName == "" {
		return fmt.Errorf("service_name is required")
	}

	// Set default number of threads if not provided
	if c.MaxThreads <= 0 {
		c.MaxThreads = constants.DefaultThreadCount
	}

	if c.SSLConfiguration == nil {
		c.SSLConfiguration = &utils.SSLConfig{
			Mode: "disable",
		}
	}
	err := c.SSLConfiguration.Validate()
	if err != nil {
		return fmt.Errorf("failed to validate ssl config: %s", err)
	}

	return utils.Validate(c)
}
