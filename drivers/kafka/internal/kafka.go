package kafka

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/datazip-inc/olake/drivers/base"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

type Kafka struct {
	*base.Driver
	config *Config
	client *kgo.Client
}

// createKafkaClient creates a Kafka client based on the configuration.
func createKafkaClient(cfg Config) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(cfg.BootstrapServers, ",")...),
	}

	switch cfg.Protocol.SecurityProtocol {
	case "PLAINTEXT":
	case "SASL_PLAINTEXT":
		// For now only PLAIN mechanism is supported
		if cfg.Protocol.SASLMechanism != "PLAIN" {
			return nil, fmt.Errorf("only PLAIN mechanism is supported")
		}
		username, password, err := parseSASLPlain(cfg.Protocol.SASLJAASConfig)
		if err != nil {
			return nil, err
		}
		sasl := plain.Auth{User: username, Pass: password}.AsMechanism()
		opts = append(opts, kgo.SASL(sasl))
	case "SASL_SSL":
		// For now only PLAIN mechanism is supported
		if cfg.Protocol.SASLMechanism != "PLAIN" {
			return nil, fmt.Errorf("only PLAIN mechanism is supported")
		}
		username, password, err := parseSASLPlain(cfg.Protocol.SASLJAASConfig)
		if err != nil {
			return nil, err
		}
		sasl := plain.Auth{User: username, Pass: password}.AsMechanism()
		opts = append(opts, kgo.SASL(sasl), kgo.DialTLS())
	default:
		return nil, fmt.Errorf("unsupported security protocol: %s", cfg.Protocol.SecurityProtocol)
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return cl, nil
}

// parseSASLPlain extracts username and password from the JAAS config.
func parseSASLPlain(jassConfig string) (string, string, error) {
	re := regexp.MustCompile(`username="([^"]+)"\s+password="([^"]+)"`)
	matches := re.FindStringSubmatch(jassConfig)
	if len(matches) != 3 {
		return "", "", fmt.Errorf("invalid sasl_jaas_config for PLAIN")
	}
	return matches[1], matches[2], nil
}

func (k *Kafka) Setup() error {
	if err := k.config.Validate(); err != nil {
		return fmt.Errorf("config validation failed: %v", err)
	}
	var err error
	k.client, err = createKafkaClient(*k.config)
	if err != nil {
		return fmt.Errorf("failed to create Kafka client: %v", err)
	}
	return nil
}

func (k *Kafka) Check() error {
	if k.client == nil {
		return fmt.Errorf("client not initialized")
	}
	cl, err := createKafkaClient(*k.config)
	if err != nil {
		return err
	}
	defer cl.Close()
	_, err = listAllTopics(cl)
	return err
}

func (k *Kafka) Discover(discoverSchema bool) ([]*types.Stream, error) {
	cl, err := createKafkaClient(*k.config)
	if err != nil {
		return nil, err
	}
	defer cl.Close()

	allTopics, err := listAllTopics(cl)
	if err != nil {
		return nil, err
	}

	topics, err := getTopicsToSubscribe(k.config, allTopics)
	if err != nil {
		return nil, err
	}

	streams := generateStreams(topics, k.config.DefaultMode)
	for _, stream := range streams {
		k.AddStream(stream)
	}

	return streams, nil
}

func (k *Kafka) GetConfigRef() protocol.Config {
	k.config = &Config{}
	return k.config
}

func (k *Kafka) Type() string {
	return "Kafka"
}

func listAllTopics(cl *kgo.Client) ([]string, error) {
	adm := kadm.NewClient(cl)
	topics, err := adm.ListTopics(context.Background())
	if err != nil {
		return nil, err
	}
	var topicNames []string
	for topic := range topics {
		topicNames = append(topicNames, topic)
	}
	return topicNames, nil
}

func getTopicsToSubscribe(cfg *Config, allTopics []string) ([]string, error) {
	switch cfg.Subscription.SubscriptionType {
	case "subscribe":
		re, err := regexp.Compile(cfg.Subscription.TopicPattern)
		if err != nil {
			return nil, fmt.Errorf("invalid topic pattern: %v", err)
		}
		var matchingTopics []string
		for _, topic := range allTopics {
			if re.MatchString(topic) {
				matchingTopics = append(matchingTopics, topic)
			}
		}
		return matchingTopics, nil
	case "assign":
		parts := strings.Split(cfg.Subscription.TopicPartitions, ",")
		topicSet := make(map[string]struct{})
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			topicPartition := strings.Split(part, ":")
			if len(topicPartition) != 2 {
				return nil, fmt.Errorf("invalid topic partition format: %s", part)
			}
			topicSet[topicPartition[0]] = struct{}{}
		}
		var topics []string
		for topic := range topicSet {
			topics = append(topics, topic)
		}
		return topics, nil
	default:
		return nil, fmt.Errorf("unknown subscription type: %s", cfg.Subscription.SubscriptionType)
	}
}

func generateStreams(topics []string, defaultMode string) []*types.Stream {
	streams := []*types.Stream{}
	for _, topic := range topics {
		stream := types.NewStream(topic, "")
		stream.WithSyncMode(types.FULLREFRESH, types.INCREMENTAL)
		stream.SyncMode = types.SyncMode(defaultMode)
		schema := types.NewTypeSchema()
		schema.AddTypes("message", types.String)
		stream.WithSchema(schema)
		streams = append(streams, stream)
	}
	return streams
}

// To be implemented later
func (k *Kafka) Read(pool *protocol.WriterPool, stream protocol.Stream) error {
	// TODO: Implement Kafka read logic here
	return nil
}

func (k *Kafka) StateType() types.StateType {
	return types.StreamType
}

func (k *Kafka) SetupState(state *types.State) {
	state.Type = k.StateType()
	k.State = state
}

func (k *Kafka) Spec() any {
	// Spec logic
	return nil
}
