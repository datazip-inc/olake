package driver

import (
	"fmt"
	"strings"
)

type Config struct {
	Hosts          []string `json:"hosts"`
	Username       string   `json:"username"`
	Password       string   `json:"password"`
	AuthDB         string   `json:"authdb"`
	ReplicaSet     string   `json:"replica_set"`
	ReadPreference string   `json:"read_preference"`
	Srv            bool     `json:"srv"`
	ServerRAM      uint     `json:"server_ram"`
	Database       string   `json:"databsae"`
}

func (c *Config) URI() string {
	connectionPrefix := "mongodb"
	options := fmt.Sprintf("?authSource=%s", c.AuthDB)

	if c.Srv {
		connectionPrefix = "mongodb+srv"
	}

	if c.ReplicaSet != "" {
		// configurations for a replica set
		options = fmt.Sprintf("%s&replicaSet=%s", options, c.ReplicaSet)
		if c.ReadPreference != "" {
			options = fmt.Sprintf("%s&readPreference=%s", options, c.ReadPreference)
		} else {
			// default secondaryPreferred
			options = fmt.Sprintf("%s&readPreference=%s", options, "secondaryPreferred")
		}
	}

	return fmt.Sprintf(
		"%s://%s:%s@%s/?%s", connectionPrefix,
		c.Username, c.Password, strings.Join(c.Hosts, ","), options,
	)
}
