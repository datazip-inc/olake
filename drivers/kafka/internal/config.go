package driver

import (
	"fmt"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils"
)

type Config struct {
	BootstrapServers string         `json:"bootstrap_servers"`
	Protocol         ProtocolConfig `json:"protocol"`
	ConsumerGroup    string         `json:"consumer_group,omitempty"`
	AutoOffsetReset  string         `json:"auto_offset_reset,omitempty"`
	MaxThreads       int            `json:"max_threads"`
	WaitTime         int            `json:"wait_time,omitempty"`
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

	if c.Protocol.SecurityProtocol == "" {
		return fmt.Errorf("security_protocol must be either PLAINTEXT or SASL_PLAINTEXT or SASL_SSL")
	}

	if c.Protocol.SecurityProtocol == "SASL_PLAINTEXT" || c.Protocol.SecurityProtocol == "SASL_SSL" {
		if c.Protocol.SASLMechanism == "" {
			return fmt.Errorf("sasl_mechanism must be either PLAIN or SCRAM-SHA-512")
		}
		if c.Protocol.SASLJAASConfig == "" {
			return fmt.Errorf("sasl_jaas_config must be provided")
		}
	}

	if c.WaitTime <= 0 {
		c.WaitTime = int(constants.DefaultWaitTime)
	}

	if c.AutoOffsetReset != "" && c.AutoOffsetReset != "earliest" && c.AutoOffsetReset != "latest" {
		return fmt.Errorf("auto_offset_reset must be either 'earliest' or 'latest'")
	}

	if c.MaxThreads <= 0 {
		c.MaxThreads = constants.DefaultThreadCount
	}

	return utils.Validate(c)
}
