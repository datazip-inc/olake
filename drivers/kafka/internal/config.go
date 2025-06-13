package driver

import (
	"fmt"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
)

type Config struct {
	BootstrapServers string         `json:"bootstrap_servers"`
	Protocol         ProtocolConfig `json:"protocol"`
	DefaultMode      types.SyncMode `json:"default_mode"`
	ConsumerGroup    string         `json:"consumer_group"`
	AutoOffsetReset  string         `json:"auto_offset_reset,omitempty"`
}

type ProtocolConfig struct {
	SecurityProtocol string `json:"security_protocol"`
	SASLMechanism    string `json:"sasl_mechanism,omitempty"`
	SASLJAASConfig   string `json:"sasl_jaas_config,omitempty"`
}

func (c *Config) Validate() error {
	if c.BootstrapServers == "" {
		return fmt.Errorf("bootstrap_servers is required")
	}
	if c.DefaultMode != types.FULLREFRESH && c.DefaultMode != types.INCREMENTAL {
		return fmt.Errorf("default_mode must be 'full_refresh' or 'incremental'")
	}
	if c.DefaultMode == types.INCREMENTAL {
		if c.AutoOffsetReset == "" {
			c.AutoOffsetReset = "latest" // Default to latest
		} else if c.AutoOffsetReset != "earliest" && c.AutoOffsetReset != "latest" && c.AutoOffsetReset != "none" {
			return fmt.Errorf("auto_offset_reset must be 'earliest', 'latest', or 'none'")
		}
	}
	return utils.Validate(c)
}
