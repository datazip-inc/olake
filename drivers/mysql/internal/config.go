package driver

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"maps"
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

	if c.SSLConfiguration != nil {
		switch c.SSLConfiguration.Mode {
		case utils.SSLModeDisable:
			cfg.TLSConfig = "false"
		case utils.SSLModeRequire:
			cfg.TLSConfig = "true"
		case utils.SSLModeVerifyCA, utils.SSLModeVerifyFull:
			cfg.TLSConfig = "skip-verify" // Default fallback
			if tlsConfig, err := c.buildTLSConfig(); err == nil && mysql.RegisterTLSConfig("custom", tlsConfig) == nil {
				cfg.TLSConfig = "custom"
			}
		}
	}

	if len(c.JDBCURLParams) > 0 {
		if cfg.Params == nil {
			cfg.Params = make(map[string]string)
		}
		maps.Copy(cfg.Params, c.JDBCURLParams)
	}

	return cfg.FormatDSN()
}

// buildTLSConfig builds a custom TLS configuration for certificate-based SSL
func (c *Config) buildTLSConfig() (*tls.Config, error) {
	rootCertPool := x509.NewCertPool()

	if c.SSLConfiguration.ServerCA != "" {
		if ok := rootCertPool.AppendCertsFromPEM([]byte(c.SSLConfiguration.ServerCA)); !ok {
			return nil, fmt.Errorf("failed to append CA certificate")
		}
	}

	tlsConfig := &tls.Config{
		RootCAs: rootCertPool,
	}

	if c.SSLConfiguration.Mode == utils.SSLModeVerifyCA {
		tlsConfig.InsecureSkipVerify = true
	}

	if c.SSLConfiguration.ClientCert != "" && c.SSLConfiguration.ClientKey != "" {
		cert, err := tls.X509KeyPair([]byte(c.SSLConfiguration.ClientCert), []byte(c.SSLConfiguration.ClientKey))
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate and key: %s", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
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
