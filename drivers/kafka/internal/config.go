package driver

import (
	"fmt"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils"
)

type Config struct {
	BootstrapServers            string         `json:"bootstrap_servers"`
	ConsumerGroupID             string         `json:"consumer_group_id,omitempty"`
	Protocol                    ProtocolConfig `json:"protocol"`
	MaxThreads                  int            `json:"max_threads"`
	RetryCount                  int            `json:"backoff_retry_count"`
	ThreadsEqualTotalPartitions bool           `json:"threads_equal_total_partitions,omitempty"`
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

	if c.MaxThreads <= 0 {
		c.MaxThreads = constants.DefaultThreadCount
	}

	if c.RetryCount <= 0 {
		c.RetryCount = constants.DefaultRetryCount
	}

	return utils.Validate(c)
}
