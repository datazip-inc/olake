package driver

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils"
)

type Config struct {
	Connection       *url.URL          `json:"-"`
	Host             string            `json:"host"`
	Port             int               `json:"port"`
	Database         string            `json:"database"`
	Username         string            `json:"username"`
	Password         string            `json:"password"`
	JDBCURLParams    map[string]string `json:"jdbc_url_params"`
	SSLConfiguration *utils.SSLConfig  `json:"ssl"`
	UpdateMethod     interface{}       `json:"update_method"`
	MaxThreads       int               `json:"max_threads"`
	RetryCount       int               `json:"retry_count"`
	SSHConfig        *utils.SSHConfig  `json:"ssh_config"`
}

// Capture Write Ahead Logs
type CDC struct {
	ReplicationSlot string `json:"replication_slot"`
	// initial wait time must be in range [120,2400), default value 1200
	InitialWaitTime int `json:"initial_wait_time"`
	// Publications used when OutputPlugin is pgoutput
	Publication string `json:"publication"`
	// OnLSNMismatch defines behavior when LSN mismatch is detected
	// "fail" - fail sync and ask user to clear destination (default, prevents duplicates)
	// "full_load" - perform full load with new LSN (accepts duplicates, useful for S3/Parquet destinations)
	// Note: For Iceberg destinations, this should always be "fail" to prevent data duplication
	OnLSNMismatch string `json:"on_lsn_mismatch"`
}

func (c *Config) Validate() error {
	if c.Host == "" {
		return fmt.Errorf("empty host name")
	} else if strings.Contains(c.Host, "https") || strings.Contains(c.Host, "http") {
		return fmt.Errorf("host should not contain http or https")
	}

	// Validate port
	if c.Port <= 0 || c.Port > 65535 {
		return fmt.Errorf("invalid port number: must be between 1 and 65535")
	}

	// default number of threads
	if c.MaxThreads <= 0 {
		c.MaxThreads = constants.DefaultThreadCount
	}

	// Add the connection parameters to the url
	parsed := &url.URL{
		Scheme: "postgres",
		User:   utils.Ternary(c.Password != "", url.UserPassword(c.Username, c.Password), url.User(c.Username)).(*url.Userinfo),
		Host:   fmt.Sprintf("%s:%d", c.Host, c.Port),
		Path:   "/" + c.Database,
	}

	query := parsed.Query()

	// Set additional connection parameters if available
	if len(c.JDBCURLParams) > 0 {
		for k, v := range c.JDBCURLParams {
			query.Add(k, v)
		}
	}

	if c.SSLConfiguration == nil {
		c.SSLConfiguration = &utils.SSLConfig{
			Mode: "disable",
		}
	}

	sslmode := string(c.SSLConfiguration.Mode)
	if sslmode != "" {
		query.Add("sslmode", sslmode)
	}

	err := c.SSLConfiguration.Validate()
	if err != nil {
		return fmt.Errorf("failed to validate ssl config: %s", err)
	}

	if c.SSLConfiguration.ServerCA != "" {
		query.Add("sslrootcert", c.SSLConfiguration.ServerCA)
	}

	if c.SSLConfiguration.ClientCert != "" {
		query.Add("sslcert", c.SSLConfiguration.ClientCert)
	}

	if c.SSLConfiguration.ClientKey != "" {
		query.Add("sslkey", c.SSLConfiguration.ClientKey)
	}
	parsed.RawQuery = query.Encode()
	c.Connection = parsed

	return nil
}

type Table struct {
	Schema string `db:"table_schema"`
	Name   string `db:"table_name"`
}

type ColumnDetails struct {
	Name       string  `db:"column_name"`
	DataType   *string `db:"data_type"`
	IsNullable *string `db:"is_nullable"`
}
