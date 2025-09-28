package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
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

	// Persist topics -> stream map for routing
	for _, stream := range streams {
		k.streamsByTopic.Store(stream.Name(), stream)
	}

	k.consumerGroupID = groupID

	// Determine start offset behavior for the group readers
	var startOffset int64
	switch k.config.AutoOffsetReset {
	case "earliest":
		startOffset = kafka.FirstOffset
	case "latest", "":
		startOffset = kafka.LastOffset
	default:
		startOffset = kafka.LastOffset
	}

	// Count total partitions across topics to choose default concurrency
	meta, err := k.adminClient.Metadata(ctx, &kafka.MetadataRequest{Topics: topics})
	if err != nil {
		return fmt.Errorf("failed to fetch topics metadata: %v", err)
	}
	totalPartitions := 0
	for _, t := range meta.Topics {
		totalPartitions += len(t.Partitions)
	}

	// Decide reader count = min(max_threads, total_partitions), default to total_partitions when max_threads <= 0
	readerCount := totalPartitions
	if k.config.MaxThreads > 0 {
		if k.config.MaxThreads < totalPartitions {
			readerCount = k.config.MaxThreads
		}
	}

	// Create group-based readers
	for i := 0; i < readerCount; i++ {
		readerID := fmt.Sprintf("group_reader_%d_%s", i+1, utils.ULID())
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:               strings.Split(k.config.BootstrapServers, ","),
			GroupID:               groupID,
			GroupTopics:           topics,
			Dialer:                k.dialer,
			MinBytes:              1,
			MaxBytes:              10e6,
			WatchPartitionChanges: true,
			StartOffset:           startOffset,
			CommitInterval:        0, // sync commits
		})
		k.readers.Store(readerID, r)
	}

	// expose a synthetic single partitionMeta per first stream so abstract layer can iterate
	if len(streams) > 0 {
		k.firstStreamID = streams[0].ID()
		var metaList []abstract.PartitionMetaData
		k.readers.Range(func(key, _ any) bool {
			metaList = append(metaList, abstract.PartitionMetaData{
				ReaderID: key.(string),
				Stream:   streams[0],
			})
			return true
		})
		k.partitionMeta.Store(k.firstStreamID, metaList)
	}
	return nil
}

func (k *Kafka) PartitionStreamChanges(ctx context.Context, data abstract.PartitionMetaData, processFn abstract.CDCMsgFn) error {
	// load reader
	raw, ok := k.readers.Load(data.ReaderID)
	if !ok {
		return fmt.Errorf("reader %s not found", data.ReaderID)
	}
	readerInstance := raw.(*kafka.Reader)

	// wait time
	idleWait := time.Duration(k.config.WaitTime) * time.Second
	for {
		deadline := time.Now().Add(idleWait)
		fetchCtx, cancel := context.WithDeadline(ctx, deadline)
		msg, err := readerInstance.FetchMessage(fetchCtx)
		cancel()
		if err != nil {
			if fetchCtx.Err() == context.DeadlineExceeded {
				logger.Infof("no messages within wait time for reader %s", data.ReaderID)
				return nil
			}
			if err == context.Canceled || err == context.DeadlineExceeded {
				return nil
			}
			return fmt.Errorf("error reading message: %v", err)
		}

		// route stream by topic
		rawStream, ok := k.streamsByTopic.Load(msg.Topic)
		if !ok {
			// if not found (e.g., first stream fallback)
			rawStream, _ = k.streamsByTopic.Load(data.Stream.Name())
		}
		stream := rawStream.(types.StreamInterface)

		var payload map[string]any
		if err := json.Unmarshal(msg.Value, &payload); err == nil {
			payload["partition"] = msg.Partition
			payload["offset"] = msg.Offset
			payload["key"] = string(msg.Key)
			payload["kafka_timestamp"] = msg.Time.UnixMilli()
		} else {
			// drop or store raw? keep minimal metadata
			payload = map[string]any{
				"partition":       msg.Partition,
				"offset":          msg.Offset,
				"key":             string(msg.Key),
				"kafka_timestamp": msg.Time.UnixMilli(),
			}
		}

		if err := processFn(ctx, abstract.CDCChange{
			Stream:    stream,
			Timestamp: msg.Time,
			Kind:      "create",
			Data:      payload,
		}); err != nil {
			return err
		}

		// remember last message per reader+partition for commit
		k.lastMessages.Store(data.ReaderID, map[int]kafka.Message{msg.Partition: msg})
	}
}

func (k *Kafka) PostCDC(_ context.Context, stream types.StreamInterface, noErr bool, readerID string) error {
	if !noErr {
		logger.Warnf("skipping commit for reader %s due to error", readerID)
		return nil
	}

	// load reader and last messages
	rawReader, ok := k.readers.Load(readerID)
	if !ok {
		return nil
	}
	reader := rawReader.(*kafka.Reader)

	rawLast, ok := k.lastMessages.Load(readerID)
	if ok {
		// commit all partitions seen by this reader
		msgByPartition := rawLast.(map[int]kafka.Message)
		msgs := make([]kafka.Message, 0, len(msgByPartition))
		for _, m := range msgByPartition {
			msgs = append(msgs, m)
		}
		if len(msgs) > 0 {
			if err := reader.CommitMessages(context.Background(), msgs...); err != nil {
				return fmt.Errorf("commit failed for reader %s: %w", readerID, err)
			}
		}
	}

	if stream != nil {
		k.state.SetCursor(stream.Self(), "consumer_group_id", k.consumerGroupID)
	}
	// do not close the group reader here; reused until idle loop returns
	return nil
}

func (k *Kafka) GetPartitions() map[string][]abstract.PartitionMetaData {
	partitionData := make(map[string][]abstract.PartitionMetaData)
	k.partitionMeta.Range(func(key, value any) bool {
		streamID, ok := key.(string)
		if !ok {
			return true
		}
		partitions, ok := value.([]abstract.PartitionMetaData)
		if !ok {
			return true
		}
		partitionData[streamID] = partitions
		return true
	})

	return partitionData
}

func (k *Kafka) StreamChanges(_ context.Context, _ types.StreamInterface, _ abstract.CDCMsgFn) error {
	return nil
}
