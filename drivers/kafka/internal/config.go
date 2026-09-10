package driver

import (
	"fmt"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/pkg/kafka"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/errs"
)

type Config struct {
	BootstrapServers            string                      `json:"bootstrap_servers"`
	ConsumerGroupID             string                      `json:"consumer_group_id,omitempty"`
	Protocol                    ProtocolConfig              `json:"protocol"`
	MaxThreads                  int                         `json:"max_threads"`
	RetryCount                  int                         `json:"backoff_retry_count"`
	ThreadsEqualTotalPartitions bool                        `json:"threads_equal_total_partitions,omitempty"`
	SchemaRegistry              *kafka.SchemaRegistryClient `json:"schema_registry,omitempty"`
	TopicPattern                string                      `json:"topic_pattern,omitempty"` // Perl-style regex to filter topics (optional)
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
		return errs.Precondition(errs.ConfigInvalid, codeBootstrapServersMissing,
			fmt.Errorf("bootstrap_servers is required"))
	}

	if c.Protocol.SecurityProtocol == "" {
		return errs.Precondition(errs.ConfigInvalid, codeSecurityProtocolMissing,
			fmt.Errorf("security_protocol must be one of: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL"))
	}

	if c.Protocol.SecurityProtocol == "SASL_PLAINTEXT" || c.Protocol.SecurityProtocol == "SASL_SSL" {
		if c.Protocol.SASLMechanism == "" {
			return errs.Precondition(errs.ConfigInvalid, codeSASLMechanismMissing,
				fmt.Errorf("sasl_mechanism must be either PLAIN or SCRAM-SHA-512"))
		}
		if c.Protocol.SASLJAASConfig == "" {
			return errs.Precondition(errs.ConfigInvalid, codeSASLJAASConfigMissing,
				fmt.Errorf("sasl_jaas_config must be provided"))
		}
	}

	if c.Protocol.SecurityProtocol == "SSL" || c.Protocol.SecurityProtocol == "SASL_SSL" {
		if c.Protocol.SSL != nil {
			// Server CA is always required
			if c.Protocol.SSL.ServerCA == "" {
				return errs.Precondition(errs.ConfigInvalid, codeServerCAMissing,
					fmt.Errorf("server_ca must be provided"))
			}

			// Client Cert and Key are required together for mTLS
			if (c.Protocol.SSL.ClientCert != "") != (c.Protocol.SSL.ClientKey != "") {
				return errs.Precondition(errs.ConfigInvalid, codeClientKeypairIncomplete,
					fmt.Errorf("both client_cert and client_key must be provided together for mTLS"))
			}
		}
	}

	if c.SchemaRegistry != nil {
		if c.SchemaRegistry.Endpoint == "" {
			return errs.Precondition(errs.ConfigInvalid, codeSchemaRegistryEndpointMissing,
				fmt.Errorf("schema registry endpoint is required"))
		}
	}

	if c.MaxThreads <= 0 {
		c.MaxThreads = constants.DefaultThreadCount
	}

	if c.RetryCount <= 0 {
		c.RetryCount = constants.DefaultRetryCount
	}

	return errs.Precondition(errs.ConfigInvalid, codeConfigValidationFailed, utils.Validate(c))
}
