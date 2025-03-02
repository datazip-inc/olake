package driver

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/lib/pq"
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
	DefaultSyncMode  types.SyncMode    `json:"default_mode"`
	BatchSize        int               `json:"reader_batch_size"`
	MaxThreads       int               `json:"max_threads"`
}

// Standard Sync
type Standard struct {
}

// Capture Write Ahead Logs
type CDC struct {
	ReplicationSlot string `json:"replication_slot"`
	InitialWaitTime int    `json:"intial_wait_time"`
}

func (c *Config) Validate() error {
	if c.Host == "" {
		return fmt.Errorf("empty host name")
	} else if strings.Contains(c.Host, "https") || strings.Contains(c.Host, "http") {
		return fmt.Errorf("host should not contain http or https")
	}

	if c.SSLConfiguration == nil {
		return fmt.Errorf("ssl config not set")
	}

	// construct the connection string
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", c.Username, c.Password, c.Host, c.Port, c.Database)
	parsed, err := url.Parse(connStr)
	if err != nil {
		return err
	}

	query := parsed.Query()

	// Set additional connection parameters if available
	if len(c.JDBCURLParams) > 0 {
		params := ""
		for k, v := range c.JDBCURLParams {
			params += fmt.Sprintf("%s=%s ", pq.QuoteIdentifier(k), pq.QuoteLiteral(v))
		}

		query.Add("options", params)
	}

	// Enable SSL if SSLConfig is provided
	if c.SSLConfiguration != nil {
		sslmode := string(c.SSLConfiguration.Mode)
		if sslmode != "" {
			query.Add("sslmode", sslmode)
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
	}
	parsed.RawQuery = query.Encode()
	c.Connection = parsed

	return c.SSLConfiguration.Validate()
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
