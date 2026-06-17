package driver

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils"
)

// Config represents the configuration for connecting to a MSSQL database.
type Config struct {
	Host                   string            `json:"host"`
	Port                   int               `json:"port"`
	Database               string            `json:"database"`
	Username               string            `json:"username"`
	Password               string            `json:"password"`
	MaxThreads             int               `json:"max_threads"`
	RetryCount             int               `json:"retry_count"`
	JDBCURLParams          map[string]string `json:"jdbc_url_params"`
	SSLConfiguration       *utils.SSLConfig  `json:"ssl"`
	ManageCaptureInstances bool              `json:"manage_capture_instances"`
	SSHConfig              *utils.SSHConfig  `json:"ssh_config"`
	PrimaryConfig          *PrimaryConfig    `json:"primary_config,omitempty"`
}

// PrimaryConfig holds connection details for the primary replica when the main
// connection targets a read-only secondary.
// Used exclusively for CDC capture instance management.
type PrimaryConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
}

func (p *PrimaryConfig) Validate() error {
	if err := validateSQLConnection(p.Host, p.Port, p.Username, p.Password, true); err != nil {
		return err
	}
	return utils.Validate(p)
}

// Validate checks and normalises MSSQL configuration.
func (c *Config) Validate() error {
	if err := validateSQLConnection(c.Host, c.Port, c.Username, c.Password, false); err != nil {
		return err
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

	if c.PrimaryConfig != nil {
		if err := c.PrimaryConfig.Validate(); err != nil {
			return err
		}
	}

	return utils.Validate(c)
}

func validateSQLConnection(host string, port int, username, password string, isPrimary bool) error {
	prefix := utils.Ternary(isPrimary, "primary_config:", "").(string)
	if host == "" {
		return fmt.Errorf("%sempty host name", prefix)
	}
	if strings.Contains(host, "https") || strings.Contains(host, "http") {
		return fmt.Errorf("%s host should not contain http or https", prefix)
	}
	if port <= 0 || port > 65535 {
		return fmt.Errorf("%s invalid port number: must be between 1 and 65535", prefix)
	}
	if username == "" {
		return fmt.Errorf("%s username is required", prefix)
	}
	if password == "" {
		return fmt.Errorf("%s password is required", prefix)
	}
	return nil
}

// URI returns the sqlserver:// connection string for go-mssqldb.
func (c *Config) URI() string {
	return c.buildURI(c.Host, c.Port, c.Username, c.Password)
}

// primaryURI returns the sqlserver:// connection string for the primary replica,
// inheriting database, SSL, and JDBC params from the main config.
func (c *Config) primaryURI() string {
	if c.PrimaryConfig == nil {
		return ""
	}
	return c.buildURI(c.PrimaryConfig.Host, c.PrimaryConfig.Port, c.PrimaryConfig.Username, c.PrimaryConfig.Password)
}

func (c *Config) buildURI(host string, port int, username, password string) string {
	if !strings.Contains(host, ":") {
		host = fmt.Sprintf("%s:%d", host, port)
	}

	query := url.Values{}

	for k, v := range c.JDBCURLParams {
		query.Add(k, v)
	}

	query.Set("database", c.Database)

	if c.SSLConfiguration == nil {
		query.Set("encrypt", "disable")
	} else {
		switch string(c.SSLConfiguration.Mode) {
		case utils.SSLModeDisable:
			query.Set("encrypt", "disable")
		case utils.SSLModeRequire:
			query.Set("encrypt", "true")
			query.Set("TrustServerCertificate", "true")
		default:
			query.Set("encrypt", "disable")
		}
	}

	u := &url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(username, password),
		Host:     host,
		RawQuery: query.Encode(),
	}

	return u.String()
}
