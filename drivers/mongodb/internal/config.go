package driver

import (
	"fmt"
	"strings"
)

type Config struct {
	Hosts          []string
	Username       string
	Password       string
	AuthDB         string
	ReplicaSet     string
	ReadPreference string
	Srv            bool
}

func (c *Config) URI() string {
	return fmt.Sprintf(
		"mongodb://%s:%s@%s?authSource=%s&replicaSet=%s&readPreference=%s",
		c.Username, c.Password, strings.Join(c.Hosts, ","), c.AuthDB, c.ReplicaSet, c.ReadPreference,
	)
}
