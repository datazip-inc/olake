package driver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/segmentio/kafka-go"
)

func (k *Kafka) PreCDC(ctx context.Context, streams []types.StreamInterface) error {
	if len(streams) == 0 {
		return fmt.Errorf("no valid streams found for CDC")
	}

	// Generate a new consumer group ID if not configured
	var groupID string
	if cursor := k.state.GetCursor(streams[0].Self(), "consumer_group_id"); cursor != nil {
		if gID, ok := cursor.(string); ok && gID != "" {
			groupID = gID
		}
	}
	groupID = utils.Ternary(groupID == "", utils.Ternary(k.config.ConsumerGroupID != "", k.config.ConsumerGroupID, fmt.Sprintf("olake-consumer-group-%d", time.Now().Unix())), groupID).(string)
	logger.Infof("using consumer group id: %s", groupID)

	var topics []string
	for _, stream := range streams {
		topics = append(topics, stream.Name())
	}

	// Create consumer group
	consumerGroup, err := kafka.NewConsumerGroup(kafka.ConsumerGroupConfig{
		ID:      groupID,
		Brokers: strings.Split(k.config.BootstrapServers, ","),
		Topics:  topics,
		Dialer:  k.dialer,
	})
	if err != nil {
		return fmt.Errorf("failed to create consumer group: %v", err)
	}
	k.consumerGroup = consumerGroup

	// Get current generation
	gen, err := consumerGroup.Next(ctx)
	if err != nil {
		return fmt.Errorf("failed to get consumer group generation: %v", err)
	}
	k.consumerGen = gen

	// Set partitions for all streams using assignments
	for _, stream := range streams {
		var allPartitions []abstract.PartitionMetaData
		assignments, ok := gen.Assignments[stream.Name()]
		if !ok {
			return fmt.Errorf("no assignments found for topic %s", stream.Name())
		}
		for _, assignment := range assignments {
			startOffset := assignment.Offset
			partition := assignment.ID
			readerID := fmt.Sprintf("%s_%s", stream.ID(), utils.ULID())
			// creating reader
			reader := kafka.NewReader(kafka.ReaderConfig{
				Brokers:   strings.Split(k.config.BootstrapServers, ","),
				Topic:     stream.Name(),
				Partition: partition,
				MinBytes:  1,
				MaxBytes:  10e6,
				Dialer:    k.dialer,
			})
			allPartitions = append(allPartitions, abstract.PartitionMetaData{
				ReaderID:    readerID,
				Stream:      stream,
				PartitionID: assignment.ID,
				StartOffset: startOffset,
			})
			// persisting reader
			k.readers.Store(readerID, reader)
		}
		// saved for usage in getPartition
		k.partitionMeta.Store(stream.ID(), allPartitions)
	}
	return nil
}

func (k *Kafka) PartitionStreamChanges(ctx context.Context, data abstract.PartitionMetaData, processFn abstract.CDCMsgFn) error {
	partitionCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	logger.Infof("starting kafka streaming for topic %s (reader %s)", data.Stream.Name(), data.ReaderID)
	var readerInstance *kafka.Reader
	// loading reader
	if reader, ok := k.readers.Load(data.ReaderID); ok {
		readerInstance, _ = reader.(*kafka.Reader)
	}
	if k.config.AutoOffsetReset == "earliest" {
		if setErr := readerInstance.SetOffset(kafka.FirstOffset); setErr != nil {
			return fmt.Errorf("failed to reset offset: %v", setErr)
		}
	} else {
		if err := readerInstance.SetOffset(data.StartOffset); err != nil {
			return fmt.Errorf("failed to set offset: %v", err)
		}
	}
	// wait time
	partitionIdleWait := time.Duration(k.config.WaitTime) * time.Second
	for {
		deadline := time.Now().Add(partitionIdleWait)
		fetchCtx, cancel := context.WithDeadline(partitionCtx, deadline)
		msg, err := readerInstance.FetchMessage(fetchCtx)
		cancel()

		if err != nil {
			if errors.Is(err, kafka.ErrGenerationEnded) {
				// rebalance / generation end - keep reader alive and let membership reassign
				logger.Infof("generation ended (rebalance) for partition %d, continuing", data.PartitionID)
				time.Sleep(100 * time.Millisecond)
				continue
			}
			if errors.Is(err, context.DeadlineExceeded) {
				logger.Infof("no messages in topic %s within wait time for reader %s", data.Stream.Name(), data.ReaderID)
				return nil
			}
			if errors.Is(err, context.Canceled) {
				logger.Infof("context canceled for reader %s", data.ReaderID)
				return nil
			}
			return fmt.Errorf("error reading message in incremental sync: %v", err)
		}

		// message from partitions
		messageData := func() map[string]any {
			var result map[string]any
			if msg.Value == nil {
				logger.Warnf("received nil message value at offset %d for topic %s, partition %d", msg.Offset, data.Stream.Name(), msg.Partition)
				return nil
			}
			if err := json.Unmarshal(msg.Value, &result); err != nil {
				logger.Errorf("failed to unmarshal message value: %v", err)
				return nil
			}
			result["partition"] = msg.Partition
			result["offset"] = msg.Offset
			result["key"] = string(msg.Key)
			result["kafka_timestamp"] = msg.Time.UnixMilli()
			return result
		}()

		if messageData == nil {
			// we skip storing nil-valued messages
			continue
		}

		if procErr := processFn(ctx, abstract.CDCChange{
			Stream:    data.Stream,
			Timestamp: msg.Time,
			Kind:      "create",
			Data:      messageData,
		}); procErr != nil {
			return procErr
		}

		// save offset processed message for commit
		k.lastOffsets.Store(data.ReaderID, msg.Offset)
		logger.Infof("processed message at offset %d for topic %s, partition %d", msg.Offset, data.Stream.Name(), msg.Partition)
	}
}

func (k *Kafka) PostCDC(_ context.Context, stream types.StreamInterface, noErr bool, readerID string) error {
	if !noErr {
		logger.Warnf("skipping commit for reader %s due to error", readerID)
		return nil
	}

	load := func(m *sync.Map, key string) (any, bool) {
		v, ok := m.Load(key)
		if !ok {
			logger.Warnf("missing key %s for reader", key)
		}
		return v, ok
	}

	// loaded the relevant sync maps
	offsetValue, _ := load(&k.lastOffsets, readerID)
	readerVal, _ := load(&k.readers, readerID)
	lastOffset, ok1 := offsetValue.(int64)
	reader, ok2 := readerVal.(*kafka.Reader)
	if !ok1 || !ok2 || reader == nil {
		logger.Warnf("invalid data for reader %s", readerID)
		return nil
	}

	var partition int
	if raw, ok := load(&k.partitionMeta, stream.ID()); ok {
		for _, p := range raw.([]abstract.PartitionMetaData) {
			if p.ReaderID == readerID {
				partition = p.PartitionID
				break
			}
		}
	}

	if k.consumerGen == nil {
		return fmt.Errorf("no active consumer generation for commit")
	}

	topic := stream.Name()
	commitOffsets := map[string]map[int]int64{topic: {partition: lastOffset + 1}}
	// commit message of current topic, partition
	if err := k.consumerGen.CommitOffsets(commitOffsets); err != nil {
		return fmt.Errorf("failed to commit offset %d for reader %s: %w", lastOffset+1, readerID, err)
	}

	// closing current reader
	if err := reader.Close(); err != nil {
		return fmt.Errorf("failed to close reader %s: %w", readerID, err)
	}

	logger.Infof("committed offset %d for reader %s", lastOffset+1, readerID)
	k.readers.Delete(readerID)
	k.lastOffsets.Delete(readerID)

	// Save consumer group ID to state
	k.state.SetCursor(stream.Self(), "consumer_group_id", k.consumerGen.GroupID)
	return nil
}

func (k *Kafka) GetPartitions() map[string][]abstract.PartitionMetaData {
	partitionData := make(map[string][]abstract.PartitionMetaData)
	k.partitionMeta.Range(func(key, value any) bool {
		streamID, ok := key.(string)
		if !ok {
			return true // skip invalid keys
		}
		partitions, ok := value.([]abstract.PartitionMetaData)
		if !ok {
			return true // skip invalid values
		}

		partitionData[streamID] = partitions
		return true
	})

	return partitionData
}

func (k *Kafka) StreamChanges(_ context.Context, _ types.StreamInterface, _ abstract.CDCMsgFn) error {
	return nil
}
