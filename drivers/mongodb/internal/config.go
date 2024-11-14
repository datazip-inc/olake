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
	return fmt.Sprintf(
		"mongodb://%s:%s@%s/?authSource=%s&replicaSet=%s&readPreference=%s",
		c.Username, c.Password, strings.Join(c.Hosts, ","), c.AuthDB, c.ReplicaSet, c.ReadPreference,
	)
}
