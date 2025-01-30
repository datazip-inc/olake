package driver

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/datazip-inc/olake/utils"
	"github.com/lib/pq"
)

type Config struct {
	Connection *url.URL `json:"-"`
	// Hostname of the database.
	//
	// @jsonschema(
	// required=true
	// )
	Host string `json:"host"`
	// Port of the database.
	//
	// @jsonschema(
	// required=true,
	//  minimum=0,
	//  maximum=65536,
	//  default=5432
	// )
	Port int `json:"port"`
	// Name of the database.
	//
	// @jsonschema(
	// required=true
	// )
	Database string `json:"database"`

	// user of the database.
	//
	// @jsonschema(
	// required=true
	// )
	Username string `json:"username"`
	// password of the user.
	//
	// @jsonschema(
	// required=true
	// )
	Password string `json:"password"`
	// JDBC URL Parameters (Advanced)
	//
	// @jsonschema(
	// description="Additional properties to pass to the JDBC URL string when connecting to the database. For more information read about https://jdbc.postgresql.org/documentation/head/connect.html"
	// )
	JDBCURLParams map[string]string `json:"jdbc_url_params"`
	// Hostname of the database.
	//
	// @jsonschema(
	// required=true
	// )
	SSLConfiguration *utils.SSLConfig `json:"ssl"`

	// Configures how data is extracted from the database.
	//
	// @jsonschema(
	// required=true,
	// oneOf=["Standard","CDC"]
	// )
	UpdateMethod interface{} `json:"update_method"`
}

// Standard Sync
type Standard struct {
}

// Capture Write Ahead Logs
type CDC struct {
	// A plugin logical replication slot. Read about replication slots.
	//
	// @jsonschema(
	// required=true
	// )
	ReplicationSlot string `json:"replication_slot"`
	// Initial Wait Time for first CDC Log
	//
	// @jsonschema(
	// required=true,
	// default=0
	// )
	InitialWaitTime int `json:"intial_wait_time"`
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
