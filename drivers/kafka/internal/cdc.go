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
	"github.com/segmentio/kafka-go"
)

func (k *Kafka) PreCDC(ctx context.Context, streams []types.StreamInterface) error {
	if len(streams) == 0 {
		return fmt.Errorf("no valid streams found for CDC")
	}

	var groupID string

	// NOTE: in kafka we are giving priority of available consumer group id from state over config
	// get consumer group id from global state
	if globalState := k.state.GetGlobal(); globalState != nil && globalState.State != nil {
		if stateMap, ok := globalState.State.(map[string]any); ok {
			if consumerGroupID, exists := stateMap["consumer_group_id"]; exists {
				if gID, ok := consumerGroupID.(string); ok && gID != "" {
					groupID = gID
				}
			}
		}
	}

	// generate a new consumer group id if not present in state or config
	groupID = utils.Ternary(groupID == "", utils.Ternary(k.config.ConsumerGroupID != "", k.config.ConsumerGroupID, fmt.Sprintf("olake-consumer-group-%d", time.Now().Unix())), groupID).(string)
	k.consumerGroupID = groupID
	logger.Infof("configured consumer group id: %s", k.consumerGroupID)

	// create a reader manager for kafka
	k.readerManager = kafkapkg.NewReaderManager(kafkapkg.ReaderConfig{
		BootstrapServers:            k.config.BootstrapServers,
		MaxThreads:                  k.config.MaxThreads,
		ConsumerGroupID:             k.consumerGroupID,
		Dialer:                      k.dialer,
		AdminClient:                 k.adminClient,
		ThreadsEqualTotalPartitions: k.config.ThreadsEqualTotalPartitions,
	})
	return k.readerManager.CreateReaders(ctx, streams, k.consumerGroupID)
}

func (k *Kafka) PartitionStreamChanges(ctx context.Context, readerID string, processFn abstract.CDCMsgFn) error {
	// get reader
	reader := k.readerManager.GetReader(readerID)
	if reader == nil {
		return fmt.Errorf("reader not found for readerID %s", readerID)
	}

	// track processing state
	lastMessages := make(map[types.PartitionKey]kafka.Message)
	// maintain completed partitions and observed partitions to track loop termination (for the current reader)
	completedPartitions := make(map[types.PartitionKey]struct{}) // completed partitions by the current reader
	observedPartitions := make(map[types.PartitionKey]struct{})  // cached partitions which are observed by the current reader

	defer func() {
		if len(lastMessages) > 0 {
			k.checkpointMessage.Store(readerID, lastMessages)
		}
	}()

	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			return fmt.Errorf("error reading message in Kafka CDC sync: %s", err)
		}

		// get current partition metadata and key
		currentPartitionKey := types.PartitionKey{Topic: message.Topic, Partition: message.Partition}
		currentPartitionMeta, exists := k.readerManager.GetPartitionIndex(fmt.Sprintf("%s:%d", message.Topic, message.Partition))
		if !exists {
			return fmt.Errorf("missing partition index for topic %s partition %d", message.Topic, message.Partition)
		}

		// process message data
		data := func() map[string]any {
			var result map[string]any
			if message.Value == nil {
				logger.Warnf("received nil message value at offset %d for topic %s, partition %d", message.Offset, message.Topic, message.Partition)
				return nil
			}
			if err := json.Unmarshal(message.Value, &result); err != nil {
				logger.Errorf("failed to unmarshal message value: %s", err)
				return nil
			}
			result[Partition] = message.Partition
			result[Offset] = message.Offset
			result[Key] = string(message.Key)
			result[KafkaTimestamp] = message.Time.UTC().Format(time.RFC3339)
			return result
		}()
		if data != nil {
			// process the change
			if err := processFn(ctx, abstract.CDCChange{
				Stream:    currentPartitionMeta.Stream,
				Timestamp: message.Time,
				Kind:      "create",
				Data:      data,
			}); err != nil {
				return err
			}
		}

		// track last message
		lastMessages[currentPartitionKey] = message

		// check if partition is complete
		if message.Offset >= currentPartitionMeta.EndOffset-1 {
			// mark current partition as completed
			completedPartitions[currentPartitionKey] = struct{}{}

			// check for all other assigned partitions to see if they are also completed
			shouldExit, err := k.checkPartitionCompletion(ctx, readerID, completedPartitions, observedPartitions)
			if err != nil || shouldExit {
				return err
			}
		}
	}
}

func (k *Kafka) PostCDC(ctx context.Context, stream types.StreamInterface, noErr bool, readerID string) error {
	if !noErr {
		return nil
	}

	// Get accumulated messages for this reader
	lastMessagesMeta, hasMessages := k.checkpointMessage.Load(readerID)
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
	syncedStreams := make(map[string]types.StreamInterface)

	for partitionKey, message := range lastMessages {
		messages = append(messages, message)

		// Resolve stream for this partition
		partitionID := fmt.Sprintf("%s:%d", partitionKey.Topic, partitionKey.Partition)
		if partitionMeta, exists := k.readerManager.GetPartitionIndex(partitionID); exists && partitionMeta.Stream != nil {
			syncedStreams[partitionMeta.Stream.ID()] = partitionMeta.Stream
		}
	}

	// Commit messages if any exist
	if len(messages) > 0 {
		reader := k.readerManager.GetReader(readerID)
		if reader == nil {
			return fmt.Errorf("reader %s not found for commit", readerID)
		}

		if err := reader.CommitMessages(ctx, messages...); err != nil {
			return fmt.Errorf("commit failed for reader %s: %s", readerID, err)
		}

		logger.Infof("committed %d partitions for reader %s", len(messages), readerID)
	}

	// Update global state with consumer group ID and affected streams
	streamIDs := make([]string, 0, len(syncedStreams))
	for streamID := range syncedStreams {
		streamIDs = append(streamIDs, streamID)
	}

	k.state.SetGlobal(map[string]any{"consumer_group_id": k.consumerGroupID}, streamIDs...)
	logger.Infof("updated global state with consumer_group_id: %s for %d streams", k.consumerGroupID, len(streamIDs))

	k.checkpointMessage.Delete(readerID)
	return nil
}

func (k *Kafka) StreamChanges(_ context.Context, _ types.StreamInterface, _ abstract.CDCMsgFn) error {
	return nil
}
