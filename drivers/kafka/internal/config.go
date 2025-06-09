package driver

import (
	"fmt"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
)

type Config struct {
	BootstrapServers string             `json:"bootstrap_servers"`
	Subscription     SubscriptionConfig `json:"subscription"`
	Protocol         ProtocolConfig     `json:"protocol"`
	// MaxThreads       int                `json:"max_threads"`
	// RetryCount       int                `json:"backoff_retry_count"`
	DefaultMode types.SyncMode `json:"default_mode"`
	// ConsumerGroup    string             `json:"consumer_group,omitempty"`
	// AutoOffsetReset  string             `json:"auto_offset_reset,omitempty"`
}

type SubscriptionConfig struct {
	SubscriptionType string `json:"subscription_type"`
	TopicPattern     string `json:"topic_pattern,omitempty"`
	TopicPartitions  string `json:"topic_partitions,omitempty"`
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
	if c.Subscription.SubscriptionType != "subscribe" && c.Subscription.SubscriptionType != "assign" {
		return fmt.Errorf("subscription_type must be 'subscribe' or 'assign'")
	}
	if c.Subscription.SubscriptionType == "subscribe" && c.Subscription.TopicPattern == "" {
		return fmt.Errorf("topic_pattern is required for subscribe")
	}
	if c.Subscription.SubscriptionType == "assign" && c.Subscription.TopicPartitions == "" {
		return fmt.Errorf("topic_partitions is required for assign")
	}
	if c.DefaultMode != types.FULLREFRESH && c.DefaultMode != types.INCREMENTAL {
		return fmt.Errorf("default_mode must be 'full_refresh' or 'incremental'")
	}
	// if c.MaxThreads == 0 {
	// 	logger.Info("setting max threads to default[10]")
	// 	c.MaxThreads = 10
	// }
	// if c.RetryCount == 0 {
	// 	c.RetryCount = 3
	// }
	// if c.AutoOffsetReset == "" {
	// 	c.AutoOffsetReset = "earliest"
	// }
	return utils.Validate(c)
}
