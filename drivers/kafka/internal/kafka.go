package driver

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

type Kafka struct {
	config      *Config
	client      *kgo.Client
	clientMutex sync.Mutex
}

func (k *Kafka) GetConfigRef() abstract.Config {
	k.config = &Config{}
	return k.config
}

func (k *Kafka) Spec() any {
	return Config{}
}

func (k *Kafka) Type() string {
	return string(constants.Kafka)
}

func (k *Kafka) MaxConnections() int {
	return 1
}

func (k *Kafka) MaxRetries() int {
	return 1
}

func (k *Kafka) GetOrSplitChunks(_ context.Context, _ *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
	logger.Infof("Kafka does not support chunking; returning empty set for stream %s", stream.ID())
	return types.NewSet[types.Chunk](), nil
}

func (k *Kafka) ChunkIterator(_ context.Context, _ types.StreamInterface, _ types.Chunk, _ abstract.BackfillMsgFn) error {
	return fmt.Errorf("Kafka does not support chunk iteration; use CDC for streaming data")
}

func (k *Kafka) CDCSupported() bool {
	return false // Kafka uses incremental mode with offsets, not traditional CDC
}

func (k *Kafka) Setup(ctx context.Context) error {
	if err := k.config.Validate(); err != nil {
		return fmt.Errorf("config validation failed: %v", err)
	}

	client, err := k.createKafkaClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create Kafka client: %v", err)
	}

	if err := client.Ping(ctx); err != nil {
		return fmt.Errorf("failed to ping Kafka brokers: %v", err)
	}

	k.clientMutex.Lock()
	k.client = client
	k.clientMutex.Unlock()

	return nil
}

func (k *Kafka) Close(_ context.Context) error {
	k.clientMutex.Lock()
	defer k.clientMutex.Unlock()

	if k.client != nil {
		k.client.Close()
		k.client = nil
	}
	return nil
}

func (k *Kafka) GetStreamNames(ctx context.Context) ([]string, error) {
	logger.Infof("Starting discover for Kafka")
	adm := kadm.NewClient(k.client)
	topics, err := adm.ListTopics(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list topics: %v", err)
	}

	var topicNames []string
	for topic := range topics {
		topicNames = append(topicNames, topic)
	}

	return k.filterTopics(topicNames)
}

func (k *Kafka) ProduceSchema(_ context.Context, streamName string) (*types.Stream, error) {
	logger.Infof("Producing schema for topic [%s]", streamName)
	stream := types.NewStream(streamName, "").WithSyncMode(types.FULLREFRESH, types.INCREMENTAL)
	stream.SyncMode = k.config.DefaultMode

	// Kafka messages are typically schemaless; define a basic schema
	schema := types.NewTypeSchema()
	schema.AddTypes("message", types.String)  // Basic field for message payload
	schema.AddTypes("key", types.String)      // Kafka message key
	schema.AddTypes("offset", types.Int64)    // Offset for tracking
	schema.AddTypes("timestamp", types.Int64) // Message timestamp
	stream.WithSchema(schema)

	// Set partition as primary key
	stream.WithPrimaryKey("offset")

	return stream, nil
}

func (k *Kafka) createKafkaClient(_ context.Context) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(k.config.BootstrapServers, ",")...),
	}

	username, password, err := parseSASLPlain(k.config.Protocol.SASLJAASConfig)
	if err != nil && k.config.Protocol.SASLJAASConfig != "" {
		return nil, err
	}

	switch k.config.Protocol.SecurityProtocol {
	case "PLAINTEXT":
		// No SASL
	case "SASL_PLAINTEXT":
		switch k.config.Protocol.SASLMechanism {
		case "PLAIN":
			sasl := plain.Auth{User: username, Pass: password}.AsMechanism()
			opts = append(opts, kgo.SASL(sasl))
		case "SCRAM-SHA-512":
			sasl := scram.Auth{User: username, Pass: password}.AsSha512Mechanism()
			opts = append(opts, kgo.SASL(sasl))
		default:
			return nil, fmt.Errorf("unsupported SASL mechanism: %s", k.config.Protocol.SASLMechanism)
		}
	case "SASL_SSL":
		switch k.config.Protocol.SASLMechanism {
		case "PLAIN":
			sasl := plain.Auth{User: username, Pass: password}.AsMechanism()
			opts = append(opts, kgo.SASL(sasl), kgo.DialTLS())
		case "SCRAM-SHA-512":
			sasl := scram.Auth{User: username, Pass: password}.AsSha512Mechanism()
			opts = append(opts, kgo.SASL(sasl), kgo.DialTLS())
		default:
			return nil, fmt.Errorf("unsupported SASL mechanism: %s", k.config.Protocol.SASLMechanism)
		}
	default:
		return nil, fmt.Errorf("unsupported security protocol: %s", k.config.Protocol.SecurityProtocol)
	}

	return kgo.NewClient(opts...)
}

func (k *Kafka) filterTopics(allTopics []string) ([]string, error) {
	switch k.config.Subscription.SubscriptionType {
	case "subscribe":
		re, err := regexp.Compile(k.config.Subscription.TopicPattern)
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
		parts := strings.Split(k.config.Subscription.TopicPartitions, ",")
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
		return nil, fmt.Errorf("unknown subscription type: %s", k.config.Subscription.SubscriptionType)
	}
}

func parseSASLPlain(jassConfig string) (string, string, error) {
	if jassConfig == "" {
		return "", "", nil
	}
	re := regexp.MustCompile(`username="([^"]+)"\s+password="([^"]+)"`)
	matches := re.FindStringSubmatch(jassConfig)
	if len(matches) != 3 {
		return "", "", fmt.Errorf("invalid sasl_jaas_config for PLAIN")
	}
	return matches[1], matches[2], nil
}

func (k *Kafka) PreCDC(_ context.Context, _ *types.State, _ []types.StreamInterface) error {
	return fmt.Errorf("CDC not supported for Kafka")
}

func (k *Kafka) StreamChanges(_ context.Context, _ types.StreamInterface, _ abstract.CDCMsgFn) error {
	return fmt.Errorf("CDC not supported for Kafka")
}

func (k *Kafka) PostCDC(_ context.Context, _ *types.State, _ types.StreamInterface, _ bool) error {
	return fmt.Errorf("CDC not supported for Kafka")
}
