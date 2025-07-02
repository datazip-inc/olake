package driver

import (
	"fmt"
	"strings"

	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
)

type Config struct {
	// Hosts
	//
	// @jsonSchema(
	//   title="Hosts",
	//   description="Specifies the hostnames or IP addresses of MongoDB for connection",
	//   type="array",
	//   default=["host1:27017", "host2:27017"],
	//	 required=true
	// )
	Hosts []string `json:"hosts"`

	// Database
	//
	// @jsonSchema(
	//   title="Database Name",
	//   description="Name of the mongodb database selected for replication",
	//   type="string",
	//   default="database",
	// 	 required="true"
	// )
	Database string `json:"database"`

	// AuthDB
	//
	// @jsonSchema(
	//   title="Auth DB",
	//   description="Authentication database (mostly admin)",
	//   type="string",
	//   default="admin",
	//   required=true
	// )
	AuthDB string `json:"authdb"`

	// Username
	//
	// @jsonSchema(
	//   title="Username",
	//   description="Username for MongoDB authentication",
	//   type="string",
	//   default="test",
	//   required=true
	// )
	Username string `json:"username"`

	// Password
	//
	// @jsonSchema(
	//   title="Password",
	//   description="MongoDB password",
	//   type="string",
	//   format="password",
	//   default="test",
	//   required=true
	// )
	Password string `json:"password"`

	// ReplicaSet
	//
	// @jsonSchema(
	//   title="Replica Set",
	//   description="MongoDB replica set name (if applicable)",
	//   type="string",
	//   default="rs0"
	// )
	ReplicaSet string `json:"replica_set"`

	// ReadPreference
	//
	// @jsonSchema(
	//   title="Read Preference",
	//   description="Read preference for MongoDB (e.g., primary, secondaryPreferred)",
	//   type="string"
	// )
	ReadPreference string `json:"read_preference"`

	// SRV
	//
	// @jsonSchema(
	//   title="Use SRV",
	//   description="Enable this option if using DNS SRV connection strings. When set to true, the hosts field must contain only one entry - a DNS SRV address (['mongodataset.pigiy.mongodb.net'])",
	//   type="boolean",
	//   default=false
	// )
	Srv bool `json:"srv"`

	// MaxThreads
	//
	// @jsonSchema(
	//   title="Max Threads",
	//   description="Max parallel threads for chunk snapshotting",
	//   type="integer",
	//   default=5
	// )
	MaxThreads int `json:"max_threads" validate:"required" gt:"0"`

	// RetryCount
	//
	// @jsonSchema(
	//   title="Retry Count",
	//   description="Number of sync retry attempts using exponential backoff",
	//   type="integer",
	//   default=2
	// )
	RetryCount int `json:"backoff_retry_count"`

	// ChunkingStrategy
	//
	// @jsonSchema(
	//   title="Chunking Strategy",
	//   description="Chunking strategy (timestamp, uses split vector strategy if the field is left empty)",
	//   type="string"
	// )
	ChunkingStrategy string `json:"chunking_strategy"`

	ServerRAM int `json:"server_ram"`
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
