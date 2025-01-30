package driver

import (
	"fmt"
	"strings"

	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
)

type Config struct {
	Hosts          []string       `json:"hosts" validate:"required" min:"1"`
	Username       string         `json:"username" validate:"required"`
	Password       string         `json:"password" validate:"required"`
	AuthDB         string         `json:"authdb" validate:"required"`
	ReplicaSet     string         `json:"replica_set"`
	ReadPreference string         `json:"read_preference"`
	Srv            bool           `json:"srv"`
	ServerRAM      uint           `json:"server_ram" validate:"required" gt:"0"`
	MaxThreads     int            `json:"max_threads" validate:"required" gt:"0"`
	Database       string         `json:"database" validate:"required"`
	DefaultMode    types.SyncMode `json:"default_mode"`
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

	return fmt.Sprintf(
		"%s://%s:%s@%s/?%s", connectionPrefix,
		c.Username, c.Password, strings.Join(c.Hosts, ","), options,
	)
}

func (c *Config) Validate() error {
	return utils.Validate(c)
}
