package driver

import (
	"fmt"
	"strings"

	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
)

type Config struct {
	Hosts             []string       `json:"hosts"`
	Username          string         `json:"username"`
	Password          string         `json:"password"`
	AuthDB            string         `json:"authdb"`
	ReplicaSet        string         `json:"replica_set"`
	ReadPreference    string         `json:"read_preference"`
	Srv               bool           `json:"srv"`
	ServerRAM         uint           `json:"server_ram"`
	MaxThreads        int            `json:"max_threads"`
	Database          string         `json:"database"`
	DefaultMode       types.SyncMode `json:"default_mode"`
	RetryCount        int            `json:"backoff_retry_count"`
	PartitionStrategy string         `json:"partition_strategy"`
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

// TODO: Add go struct validation in Config
func (c *Config) Validate() error {
	return utils.Validate(c)
}
