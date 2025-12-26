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
	SecurityProtocol string           `json:"security_protocol"`
	SASLMechanism    string           `json:"sasl_mechanism,omitempty"`
	SASLJAASConfig   string           `json:"sasl_jaas_config,omitempty"`
	TLSSkipVerify    bool             `json:"tls_skip_verify,omitempty"`
	SSL              *utils.SSLConfig `json:"ssl,omitempty"`
}

func (c *Config) Validate() error {
	if c.BootstrapServers == "" {
		return fmt.Errorf("bootstrap_servers is required")
	}

	if c.Protocol.SecurityProtocol == "" {
		return fmt.Errorf("security_protocol must be one of: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL")
	}

	if c.Protocol.SecurityProtocol == "SASL_PLAINTEXT" || c.Protocol.SecurityProtocol == "SASL_SSL" {
		if c.Protocol.SASLMechanism == "" {
			return fmt.Errorf("sasl_mechanism must be either PLAIN or SCRAM-SHA-512")
		}
		if c.Protocol.SASLJAASConfig == "" {
			return fmt.Errorf("sasl_jaas_config must be provided")
		}
	}

	if c.Protocol.SecurityProtocol == "SSL" || c.Protocol.SecurityProtocol == "SASL_SSL" {
		if c.Protocol.SSL != nil {
			// Server CA is always required
			if c.Protocol.SSL.ServerCA == "" {
				return fmt.Errorf("server_ca must be provided")
			}

			// Client Cert and Key are required together for mTLS
			if (c.Protocol.SSL.ClientCert != "") != (c.Protocol.SSL.ClientKey != "") {
				return fmt.Errorf("both client_cert and client_key must be provided together for mTLS")
			}
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
