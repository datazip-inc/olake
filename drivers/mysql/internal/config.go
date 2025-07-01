package driver

import (
	"fmt"
	"strings"

	"github.com/go-sql-driver/mysql"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils"
)

// Config represents the configuration for connecting to a MySQL database
type Config struct {
	Host          string            `json:"hosts"`
	Username      string            `json:"username"`
	Password      string            `json:"password"`
	Database      string            `json:"database"`
	Port          int               `json:"port"`
	TLSSkipVerify bool              `json:"tls_skip_verify"`
	UpdateMethod  interface{}       `json:"update_method"`
	MaxThreads    int               `json:"max_threads"`
	RetryCount    int               `json:"backoff_retry_count"`
	JDBCURLParams map[string]string `json:"jdbc_url_params,omitempty"` // Custom connection parameters
	SSLConfig     *utils.SSLConfig  `json:"ssl_config,omitempty"`      // SSL configuration
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

	cfg := mysql.Config{
		User:                 c.Username,
		Passwd:               c.Password,
		Net:                  "tcp",
		Addr:                 fmt.Sprintf("%s:%d", hostStr, c.Port),
		DBName:               c.Database,
		AllowNativePasswords: true,
	}

	// Apply TLS configuration
	if c.SSLConfig != nil {
		switch c.SSLConfig.Mode {
		case utils.SSLModeDisable:
			cfg.TLSConfig = "false"
		case utils.SSLModeRequire:
			cfg.TLSConfig = "true"
		case utils.SSLModeVerifyCA, utils.SSLModeVerifyFull:
			// Register a custom TLS config with the MySQL driver
			tlsConfigName := fmt.Sprintf("custom-tls-%s-%d", c.Host, c.Port)
			err := registerTLSConfig(tlsConfigName, c.SSLConfig)
			if err == nil {
				cfg.TLSConfig = tlsConfigName
			}
		}
	} else if c.TLSSkipVerify {
		cfg.TLSConfig = "skip-verify"
	}

	// Apply custom JDBC URL parameters
	if c.JDBCURLParams != nil && len(c.JDBCURLParams) > 0 {
		params := make(map[string]string)
		for k, v := range c.JDBCURLParams {
			params[k] = v
		}
		cfg.Params = params
	}

	return cfg.FormatDSN()
}

// registerTLSConfig registers a custom TLS configuration with the MySQL driver
func registerTLSConfig(name string, sslConfig *utils.SSLConfig) error {
	tlsConfig, err := utils.CreateTLSConfiguration(sslConfig)
	if err != nil {
		return err
	}
	return mysql.RegisterTLSConfig(name, tlsConfig)
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

	// Validate SSL configuration if provided
	if c.SSLConfig != nil {
		if err := c.SSLConfig.Validate(); err != nil {
			return fmt.Errorf("invalid SSL configuration: %w", err)
		}
	}

	return utils.Validate(c)
}
