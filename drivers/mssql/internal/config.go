package driver

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils"
)

// ChunkingStrategy controls how MSSQL tables are divided into parallel read chunks.
//   - "iam_walk"  (default): uses sys.dm_db_database_page_allocations to plan
//     page-aligned chunks in O(1) without scanning table data. Requires SQL Server
//     2012+, VIEW DATABASE STATE permission, and is unavailable on Azure SQL DB/MI.
//     Takes a schema-stability lock during planning, which blocks DDL on
//     the target table. Use when DDL is not running concurrently during sync.
//   - "sampling": uses TABLESAMPLE SYSTEM for boundary estimation; no DDL lock,
//     no special permissions, works on Azure SQL DB/MI.
type ChunkingStrategy string

const (
	IAMWalkStrategy  ChunkingStrategy = "iam_walk"
	SamplingStrategy ChunkingStrategy = "sampling"
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
	ChunkingStrategy       ChunkingStrategy  `json:"chunking_strategy,omitempty"`
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

// URI returns the sqlserver:// connection string for go-mssqldb.
func (c *Config) URI() string {
	host := c.Host
	if !strings.Contains(host, ":") {
		host = fmt.Sprintf("%s:%d", host, c.Port)
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
		User:     url.UserPassword(c.Username, c.Password),
		Host:     host,
		RawQuery: query.Encode(),
	}

	return u.String()
}
