package kafka

import (
	"fmt"
)

type Config struct {
	BootstrapServers string             `json:"bootstrap_servers"`
	Subscription     SubscriptionConfig `json:"subscription"`
	Protocol         ProtocolConfig     `json:"protocol"`
	DefaultMode      string             `json:"default_mode"`
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
	if c.DefaultMode != "full_refresh" && c.DefaultMode != "incremental" {
		return fmt.Errorf("default_mode must be 'full_refresh' or 'incremental'")
	}
	return nil
}
