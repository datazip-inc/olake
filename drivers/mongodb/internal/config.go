package driver

import (
	"fmt"
	"strings"

	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
)

type Config struct {
	// Hosts
	//
	// @jsonSchema(
	//   title="Hosts",
	//   description="List of MongoDB hosts (with port)",
	//   type="array",
	//   default=["host1:27017", "host2:27017"],
	//   order=1
	// )
	Hosts []string `json:"hosts" validate:"required" min:"1"`

	// Database
	//
	// @jsonSchema(
	//   title="Database Name",
	//   description="MongoDB target database",
	//   type="string",
	//   default="database",
	//   order=2
	// )
	Database string `json:"database" validate:"required"`

	// AuthDB
	//
	// @jsonSchema(
	//   title="Auth DB",
	//   description="Authentication database",
	//   type="string",
	//   default="admin",
	//   order=3
	// )
	AuthDB string `json:"authdb" validate:"required"`

	// Username
	//
	// @jsonSchema(
	//   title="Username",
	//   description="MongoDB username",
	//   type="string",
	//   default="test",
	//   order=4
	// )
	Username string `json:"username" validate:"required"`

	// Password
	//
	// @jsonSchema(
	//   title="Password",
	//   description="MongoDB password",
	//   type="string",
	//   format="password",
	//   default="test",
	//   order=5
	// )
	Password string `json:"password" validate:"required"`

	// ReplicaSet
	//
	// @jsonSchema(
	//   title="Replica Set",
	//   description="MongoDB replica set name",
	//   type="string",
	//   default="rs0",
	//   order=6
	// )
	ReplicaSet string `json:"replica_set"`

	// ReadPreference
	//
	// @jsonSchema(
	//   title="Read Preference",
	//   description="Read preference (e.g., primary, secondaryPreferred)",
	//   type="string",
	//   default="",
	//   order=7
	// )
	ReadPreference string `json:"read_preference"`

	// SRV
	//
	// @jsonSchema(
	//   title="Use SRV",
	//   description="Whether to use DNS SRV",
	//   type="boolean",
	//   default=false,
	//   order=8
	// )
	Srv bool `json:"srv"`

	// MaxThreads
	//
	// @jsonSchema(
	//   title="Max Threads",
	//   description="Maximum threads to use for ingestion",
	//   type="integer",
	//   default=5,
	//   order=9
	// )
	MaxThreads int `json:"max_threads" validate:"required" gt:"0"`

	// DefaultMode
	//
	// @jsonSchema(
	//   title="Default Mode",
	//   description="Extraction mode (e.g., full_refresh, cdc)",
	//   type="string",
	//   default="cdc",
	//   order=10
	// )
	DefaultMode types.SyncMode `json:"default_mode"`

	// RetryCount
	//
	// @jsonSchema(
	//   title="Retry Count",
	//   description="Number of retries before failure",
	//   type="integer",
	//   default=2,
	//   order=11
	// )
	RetryCount int `json:"backoff_retry_count"`

	// ChunkingStrategy
	//
	// @jsonSchema(
	//   title="Chunking Strategy",
	//   description="Strategy for collection chunking",
	//   type="string",
	//   default="",
	//   order=12
	// )
	ChunkingStrategy string `json:"chunking_strategy"`

	// ServerRAM
	//
	// @jsonSchema(
	//   title="Server RAM",
	//   description="Server memory in GB",
	//   type="integer",
	//   default=16,
	//   order=13
	// )
	ServerRAM int `json:"server_ram" validate:"required" gt:"0"`
}

func (c *Config) URI() string {
	connectionPrefix := "mongodb"
	options := fmt.Sprintf("?authSource=%s", c.AuthDB)
	if c.MaxThreads == 0 {
		// set default threads
		logger.Info("setting max threads to default[10]")
		c.MaxThreads = 10
	}
	if c.Srv {
		connectionPrefix = "mongodb+srv"
	}

	if c.ReplicaSet != "" {
		// configurations for a replica set
		if c.ReadPreference == "" {
			// set default
			c.ReadPreference = "secondaryPreferred"
		}
		options = fmt.Sprintf("%s&replicaSet=%s&readPreference=%s", options, c.ReplicaSet, c.ReadPreference)
	}

	//  Handle auth credentials
	auth := ""
	if c.Username != "" {
		auth = utils.Ternary(c.Password != "", c.Username+":"+c.Password+"@", c.Username+"@").(string)
	}

	// Final MongoDB URI
	return fmt.Sprintf(
		"%s://%s%s/%s",
		connectionPrefix, auth, strings.Join(c.Hosts, ","), options,
	)
}

func (c *Config) Validate() error {
	return utils.Validate(c)
}
