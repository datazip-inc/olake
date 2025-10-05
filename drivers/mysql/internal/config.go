package driver

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"

	"github.com/go-sql-driver/mysql"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils"
)

// Config represents the configuration for connecting to a MySQL database
type Config struct {
	Host             string            `json:"hosts"`
	Username         string            `json:"username"`
	Password         string            `json:"password"`
	Database         string            `json:"database"`
	Port             int               `json:"port"`
	TLSSkipVerify    bool              `json:"tls_skip_verify"`
	JDBCURLParams    map[string]string `json:"jdbc_url_params"`
	SSLConfiguration *utils.SSLConfig  `json:"ssl"`
	UpdateMethod     interface{}       `json:"update_method"`
	MaxThreads       int               `json:"max_threads"`
	RetryCount       int               `json:"backoff_retry_count"`
	SSHConfig        *utils.SSHConfig  `json:"ssh_config"`
}

type CDC struct {
	InitialWaitTime int `json:"initial_wait_time"`
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

	// Apply SSL/TLS configuration
	if c.SSLConfiguration != nil {
		switch c.SSLConfiguration.Mode {
		case utils.SSLModeDisable:
			cfg.TLSConfig = "false"
		case utils.SSLModeRequire:
			cfg.TLSConfig = "true"
		case utils.SSLModeVerifyCA, utils.SSLModeVerifyFull:
			// For certificate-based SSL, we need to register a custom TLS config
			if err := c.registerTLSConfig(); err == nil {
				cfg.TLSConfig = "custom"
			} else {
				// Fallback to skip-verify if registration fails
				cfg.TLSConfig = "skip-verify"
			}
		}
	} else if c.TLSSkipVerify {
		// Fallback to legacy TLSSkipVerify field for backward compatibility
		cfg.TLSConfig = "skip-verify"
	}

	// Apply custom JDBC URL parameters
	if len(c.JDBCURLParams) > 0 {
		params := make(map[string]string)
		for k, v := range c.JDBCURLParams {
			params[k] = v
		}
		cfg.Params = params
	}

	return cfg.FormatDSN()
}

// registerTLSConfig registers a custom TLS configuration for certificate-based SSL
func (c *Config) registerTLSConfig() error {
	if c.SSLConfiguration == nil {
		return fmt.Errorf("SSL configuration is nil")
	}

	rootCertPool := x509.NewCertPool()
	
	// Load CA certificate if provided
	if c.SSLConfiguration.ServerCA != "" {
		pem, err := os.ReadFile(c.SSLConfiguration.ServerCA)
		if err != nil {
			return fmt.Errorf("failed to read CA certificate: %s", err)
		}
		if ok := rootCertPool.AppendCertsFromPEM(pem); !ok {
			return fmt.Errorf("failed to append CA certificate")
		}
	}

	tlsConfig := &tls.Config{
		RootCAs: rootCertPool,
	}

	// Set InsecureSkipVerify based on SSL mode
	if c.SSLConfiguration.Mode == utils.SSLModeVerifyCA {
		tlsConfig.InsecureSkipVerify = true
	}

	// Load client certificate and key if provided (for mutual TLS)
	if c.SSLConfiguration.ClientCert != "" && c.SSLConfiguration.ClientKey != "" {
		cert, err := tls.LoadX509KeyPair(c.SSLConfiguration.ClientCert, c.SSLConfiguration.ClientKey)
		if err != nil {
			return fmt.Errorf("failed to load client certificate and key: %s", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	// Register the TLS config with the MySQL driver
	if err := mysql.RegisterTLSConfig("custom", tlsConfig); err != nil {
		return fmt.Errorf("failed to register TLS config: %s", err)
	}

	return nil
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
	if c.SSLConfiguration != nil {
		if err := c.SSLConfiguration.Validate(); err != nil {
			return fmt.Errorf("failed to validate SSL config: %s", err)
		}
	}

	return utils.Validate(c)
}
