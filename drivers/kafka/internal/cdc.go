package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/datazip-inc/olake/drivers/abstract"
	kafkapkg "github.com/datazip-inc/olake/pkg/kafka"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/segmentio/kafka-go"
)

func (k *Kafka) PreCDC(ctx context.Context, streams []types.StreamInterface) error {
	if len(streams) == 0 {
		return fmt.Errorf("no valid streams found for CDC")
	}

	// Generate a new consumer group ID if not configured
	var groupID string
	if globalState := k.state.GetGlobal(); globalState != nil && globalState.State != nil {
		if stateMap, ok := globalState.State.(map[string]any); ok {
			if consumerGroupID, exists := stateMap["consumer_group_id"]; exists {
				if gID, ok := consumerGroupID.(string); ok && gID != "" {
					groupID = gID
				}
			}
		}
	}
	groupID = utils.Ternary(groupID == "", utils.Ternary(k.config.ConsumerGroupID != "", k.config.ConsumerGroupID, fmt.Sprintf("olake-consumer-group-%d", time.Now().Unix())), groupID).(string)
	k.consumerGroupID = groupID
	logger.Infof("configured consumer group id: %s", k.consumerGroupID)

	// Create reader manager and configure it
	readerConfig := kafkapkg.ReaderConfig{
		BootstrapServers:            k.config.BootstrapServers,
		MaxThreads:                  k.config.MaxThreads,
		ConsumerGroupID:             k.consumerGroupID,
		Dialer:                      k.dialer,
		AdminClient:                 k.adminClient,
		ThreadsEqualTotalPartitions: k.config.ThreadsEqualTotalPartitions,
	}
	readerManager := kafkapkg.NewReaderManager(readerConfig)

	// readers created
	if err := readerManager.CreateReaders(ctx, streams, k.consumerGroupID); err != nil {
		return fmt.Errorf("failed to create readers: %s", err)
	}

	// copy of readers created and respective metadata including last message stored in kafka struct
	k.readers = readerManager.GetReaders()
	k.partitionIndex = readerManager.GetPartitionIndex()
	readerLastMessages := readerManager.GetReaderLastMessages()

	for readerID, messages := range readerLastMessages {
		kafkaMessages := make(map[types.PartitionKey]kafka.Message)
		for partitionKey, message := range messages {
			// Convert types.PartitionKey from pkg format to driver format
			currentPartitionKey := types.PartitionKey{Topic: partitionKey.Topic, Partition: partitionKey.Partition}
			kafkaMessages[currentPartitionKey] = message
		}
		k.readerLastMessages.Store(readerID, kafkaMessages)
	}

	k.readerClientIDs = readerManager.GetReaderClientIDs()
	return nil
}

func (k *Kafka) PartitionStreamChanges(ctx context.Context, readerID string, processFn abstract.CDCMsgFn) error {
	// get reader
	reader := k.readers[readerID]
	if reader == nil {
		return fmt.Errorf("reader not found for readerID %s", readerID)
	}

	// track processing state
	lastMessages := make(map[types.PartitionKey]kafka.Message)
	observedPartitions := make(map[types.PartitionKey]struct{})
	completedPartitions := make(map[types.PartitionKey]struct{})

	defer func() {
		if len(lastMessages) > 0 {
			k.readerLastMessages.Store(readerID, lastMessages)
		}
	}()
	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			return fmt.Errorf("error reading message in Kafka CDC sync: %s", err)
		}

		// Get current partition metadata and key
		currentPartitionKey := types.PartitionKey{Topic: message.Topic, Partition: message.Partition}
		currentPartitionMeta, exists := k.partitionIndex[fmt.Sprintf("%s:%d", message.Topic, message.Partition)]
		if !exists {
			return fmt.Errorf("missing partition index for topic %s partition %d", message.Topic, message.Partition)
		}

		// Process message data
		messageData := func() map[string]any {
			var result map[string]any
			if message.Value == nil {
				logger.Warnf("received nil message value at offset %d for topic %s, partition %d", message.Offset, message.Topic, message.Partition)
				return nil
			}
			if err := json.Unmarshal(message.Value, &result); err != nil {
				logger.Errorf("failed to unmarshal message value: %s", err)
				return nil
			}
			result["partition"] = message.Partition
			result["offset"] = message.Offset
			result["key"] = string(message.Key)
			result["kafka_timestamp"], _ = typeutils.ReformatDate(message.Time.UnixMilli())
			return result
		}()
		if messageData == nil {
			// Even for nil data, we need to track the message for offset commit and check if partition is complete
			lastMessages[currentPartitionKey] = message
			if message.Offset >= currentPartitionMeta.EndOffset-1 {
				shouldExit, err := k.checkPartitionCompletion(ctx, readerID, currentPartitionKey, completedPartitions, observedPartitions)
				if err != nil {
					return err
				}
				if shouldExit {
					return nil
				}
			}
			continue
		}

		// Process the change
		if err := processFn(ctx, abstract.CDCChange{
			Stream:    currentPartitionMeta.Stream,
			Timestamp: message.Time,
			Kind:      "create",
			Data:      messageData,
		}); err != nil {
			return err
		}

		// Track last message
		lastMessages[currentPartitionKey] = message

		// Check if partition is complete
		if message.Offset >= currentPartitionMeta.EndOffset-1 {
			shouldExit, err := k.checkPartitionCompletion(ctx, readerID, currentPartitionKey, completedPartitions, observedPartitions)
			if err != nil {
				return err
			}
			if shouldExit {
				return nil
			}
		}
	}
}

func (k *Kafka) PostCDC(ctx context.Context, stream types.StreamInterface, noErr bool, readerID string) error {
	if !noErr {
		return nil
	}

	// Get accumulated messages for this reader
	lastMessagesMeta, hasMessages := k.readerLastMessages.Load(readerID)
	if !hasMessages {
		logger.Infof("reader %s has no accumulated offsets to commit", readerID)
		return nil
	}

	// Type assert and validate messages
	lastMessages, isValid := lastMessagesMeta.(map[types.PartitionKey]kafka.Message)
	if !isValid || len(lastMessages) == 0 {
		logger.Infof("reader %s has no accumulated offsets to commit", readerID)
		return nil
	}

	// Prepare messages for commit and track affected streams
	messages := make([]kafka.Message, 0, len(lastMessages))
	affectedStreams := make(map[string]types.StreamInterface)

	for partitionKey, message := range lastMessages {
		messages = append(messages, message)

		// Resolve stream for this partition
		partitionID := fmt.Sprintf("%s:%d", partitionKey.Topic, partitionKey.Partition)
		if partitionMeta, exists := k.partitionIndex[partitionID]; exists && partitionMeta.Stream != nil {
			affectedStreams[partitionMeta.Stream.ID()] = partitionMeta.Stream
		}
	}

	// Commit messages if any exist
	if len(messages) > 0 {
		reader := k.readers[readerID]
		if reader == nil {
			return fmt.Errorf("reader %s not found for commit", readerID)
		}

		if err := reader.CommitMessages(ctx, messages...); err != nil {
			return fmt.Errorf("commit failed for reader %s: %s", readerID, err)
		}

		logger.Infof("committed %d partitions for reader %s", len(messages), readerID)
	}

	// Update global state with consumer group ID and affected streams
	streamIDs := make([]string, 0, len(affectedStreams))
	for streamID := range affectedStreams {
		streamIDs = append(streamIDs, streamID)
	}

	k.state.SetGlobal(map[string]any{"consumer_group_id": k.consumerGroupID}, streamIDs...)
	logger.Infof("updated global state with consumer_group_id: %s for %d streams", k.consumerGroupID, len(streamIDs))

	// Clean up reader buffer
	k.readerLastMessages.Delete(readerID)
	return nil
}

func (k *Kafka) StreamChanges(_ context.Context, _ types.StreamInterface, _ abstract.CDCMsgFn) error {
	return nil
}
