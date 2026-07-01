package driver

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	kafkapkg "github.com/datazip-inc/olake/pkg/kafka"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/linkedin/goavro/v2"
	"github.com/twmb/franz-go/pkg/kgo"
)

func (k *Kafka) ChangeStreamConfig() (bool, bool, bool) {
	return false, true, false // parallel change streams supported
}

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
		MaxThreads:                  k.config.MaxThreads,
		ConsumerGroupID:             k.consumerGroupID,
		Dialer:                      k.dialer,
		AdminClient:                 k.adminClient,
		ThreadsEqualTotalPartitions: k.config.ThreadsEqualTotalPartitions,
	})

	// remove stale consumers before creating new readers
	if err := k.readerManager.RemoveExistingConsumers(ctx, k.client); err != nil {
		return fmt.Errorf("failed to remove existing consumers: %s", err)
	}

	// create new readers and wait for partition assignment
	return k.readerManager.CreateReaders(ctx, streams)
}

func (k *Kafka) StreamChanges(ctx context.Context, readerID int, metadataStates map[string]any, processFn abstract.CDCMsgFn) (any, error) {
	// Fetch partition assignment once per StreamChanges attempt than reused for recovery and completion checks.
	assignedPartitions, err := k.getReaderAssignedPartitions(ctx, readerID)
	if err != nil {
		return nil, fmt.Errorf("reader[%d]: get assigned partitions failed: %s", readerID, err)
	}

	readerInstanceID, _ := k.readerManager.GetReaderIDAndClientID(readerID)
	logger.Infof("reader[%d] (%s): starting CDC with %d assigned partition(s)", readerID, readerInstanceID, len(assignedPartitions))
	for _, partition := range assignedPartitions {
		logger.Infof("reader[%d]: assigned topic=%s partition=%d", readerID, partition.Topic, partition.Partition)
	}

	// Recover broker offsets from destination metadata if needed.
	isRecoveryPerformed, err := k.syncCommittedOffsetsWithMetadata(ctx, readerID, k.readerManager.GetReader(readerID), metadataStates, assignedPartitions)
	if err != nil {
		return nil, fmt.Errorf("reader[%d]: sync committed offsets with metadata failed: %s", readerID, err)
	}

	// A successful recovery stops processing for this reader so the next run starts from the recovered offsets.
	if isRecoveryPerformed {
		logger.Infof("reader[%d]: offset recovery committed to broker; skipping consumption this sync", readerID)
		return nil, nil
	}

	// Restart the reader to create a fresh franz-go client for each StreamChanges attempt.
	// franz-go keeps uncommitted offsets in memory, so restarting clears that state and
	// ensures retries resume from the last committed offset.
	// Note: Since a static instance ID is used, this restart does not trigger a consumer group rebalance.
	reader, err := k.readerManager.RestartReader(readerID)
	if err != nil {
		return nil, fmt.Errorf("failed to restart reader %d: %s", readerID, err)
	}

	_, generationID := reader.GroupMetadata()
	logger.Infof("reader[%d]: restarted reader for consumption, generation id: %d", readerID, generationID)
	k.logReaderOffsetSnapshot(ctx, readerID, assignedPartitions, "before first poll")

	// track processing state
	lastMessages := make(map[types.PartitionKey]*kgo.Record)
	// offset captured by value when lastMessages is updated; used to detect franz-go record pointer reuse.
	lastCheckpointOffsets := make(map[types.PartitionKey]int64)
	// last offset that went through processFn (record.Data != nil).
	lastProcessedOffsets := make(map[types.PartitionKey]int64)
	// maintain completed partitions and observed partitions to track loop termination (for the current reader)
	completedPartitions := make(map[types.PartitionKey]struct{}) // completed partitions by the current reader
	observedPartitions := make(map[types.PartitionKey]struct{})  // cached partitions which are observed by the current reader

	logCheckpointOffsets := func(phase string) {
		for partitionKey, storedOffset := range lastCheckpointOffsets {
			processedOffset, hasProcessed := lastProcessedOffsets[partitionKey]
			if !hasProcessed {
				processedOffset = -1
			}
			message := lastMessages[partitionKey]
			pointerOffset := int64(-1)
			if message != nil {
				pointerOffset = message.Offset
			}
			logger.Infof(
				"reader[%d] (%s) %s: topic=%s partition=%d offset_when_stored=%d pointer_offset_now=%d last_processed_offset=%d",
				readerID, readerInstanceID, phase,
				partitionKey.Topic, partitionKey.Partition,
				storedOffset, pointerOffset, processedOffset,
			)
			if message != nil {
				logger.Infof(
					"reader[%d] (%s) %s lastMessages record: topic=%s partition=%d offset=%d key=%s value=%s",
					readerID, readerInstanceID, phase,
					message.Topic, message.Partition, message.Offset,
					string(message.Key), string(message.Value),
				)
			}
		}
	}

	defer func() {
		if len(lastMessages) > 0 {
			logCheckpointOffsets("checkpoint in defer")
			k.checkpointMessage.Store(readerID, lastMessages)
		}
	}()

	err = k.processKafkaMessages(ctx, readerID, reader, assignedPartitions, func(record types.KafkaRecord) (bool, error) {
		// get current partition metadata and key
		currentPartitionKey := types.PartitionKey{Topic: record.Message.Topic, Partition: record.Message.Partition}
		currentPartitionMeta, exists := k.readerManager.GetPartitionMeta(kafkapkg.PartitionMetadataKey(record.Message.Topic, record.Message.Partition))
		if !exists {
			return false, fmt.Errorf("missing partition Metadata for topic %s partition %d", record.Message.Topic, record.Message.Partition)
		}

		// process the change if data is present
		if record.Data != nil {
			err := processFn(ctx, abstract.CDCChange{
				Stream:    currentPartitionMeta.Stream,
				Timestamp: record.Message.Timestamp,
				Kind:      "create",
				Data:      record.Data,
			})
			if err != nil {
				return false, err
			}
		}

		lastMessages[currentPartitionKey] = record.Message
		lastCheckpointOffsets[currentPartitionKey] = record.Message.Offset
		if record.Data != nil {
			lastProcessedOffsets[currentPartitionKey] = record.Message.Offset
		}

		// check if partition is complete
		if record.Message.Offset >= currentPartitionMeta.EndOffset-1 {
			// mark current partition as completed
			completedPartitions[currentPartitionKey] = struct{}{}

			// check for all other assigned partitions to see if they are also completed
			shouldExit, err := k.checkPartitionCompletion(assignedPartitions, completedPartitions, observedPartitions)
			if err != nil || shouldExit {
				return shouldExit, err
			}
		}
		return false, nil
	})

	if err != nil {
		return nil, err
	}

	logCheckpointOffsets("checkpoint at loop exit")

	// Build per-stream recovery metadata.
	// Returns map[streamID]map[string]any with consumer_group_id and partition_N -> next consumable offset
	// (message.Offset + 1) for broker offset recovery.
	metadataByStream := make(map[string]any)
	for partitionKey, message := range lastMessages {
		partitionMeta, exists := k.readerManager.GetPartitionMeta(kafkapkg.PartitionMetadataKey(partitionKey.Topic, partitionKey.Partition))
		if !exists {
			return nil, fmt.Errorf("missing partition metadata for topic %s partition %d", partitionKey.Topic, partitionKey.Partition)
		}
		streamID := partitionMeta.Stream.ID()
		state, _ := metadataByStream[streamID].(map[string]any)
		if state == nil {
			state = map[string]any{
				"consumer_group_id": k.consumerGroupID,
			}
			metadataByStream[streamID] = state
		}

		state[fmt.Sprintf("partition_%d", partitionKey.Partition)] = message.Offset + 1
	}
	return metadataByStream, nil
}

func (k *Kafka) PostCDC(ctx context.Context, readerIdx int) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		readerID, _ := k.readerManager.GetReaderIDAndClientID(readerIdx)
		// Get accumulated messages for this reader
		lastMessagesMeta, hasMessages := k.checkpointMessage.Load(readerIdx)
		if !hasMessages {
			logger.Infof("reader %s has no accumulated offsets to commit", readerID)
			return nil
		}

		// Type assert and validate messages
		lastMessages, isValid := lastMessagesMeta.(map[types.PartitionKey]*kgo.Record)
		if !isValid {
			return fmt.Errorf("reader %s has invalid checkpoint message type %T", readerID, lastMessagesMeta)
		}

		// Prepare messages for commit and track affected streams
		messages := make([]*kgo.Record, 0, len(lastMessages))
		syncedStreams := make(map[string]types.StreamInterface)

		for partitionKey, message := range lastMessages {
			messages = append(messages, message)
			logger.Infof(
				"reader %s post cdc lastMessages record: topic=%s partition=%d offset=%d key=%s value=%s",
				readerID, message.Topic, message.Partition, message.Offset,
				string(message.Key), string(message.Value),
			)

			// Resolve stream for this partition
			partitionID := kafkapkg.PartitionMetadataKey(partitionKey.Topic, partitionKey.Partition)
			if partitionMeta, exists := k.readerManager.GetPartitionMeta(partitionID); exists && partitionMeta.Stream != nil {
				syncedStreams[partitionMeta.Stream.ID()] = partitionMeta.Stream
			}
		}

		// Commit messages if any exist
		if len(messages) > 0 {
			reader := k.readerManager.GetReader(readerIdx)
			if reader == nil {
				return fmt.Errorf("reader %s not found for commit", readerID)
			}

			_, generationID := reader.GroupMetadata()
			for _, message := range messages {
				logger.Infof(
					"reader %s post cdc commit (pointer offset): generation id=%d topic=%s partition=%d offset=%d",
					readerID, generationID, message.Topic, message.Partition, message.Offset,
				)
			}

			if err := reader.CommitRecords(ctx, messages...); err != nil {
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

		k.checkpointMessage.Delete(readerIdx)
		return nil
	}
}

// processKafkaMessages processes messages from a Kafka reader
// until stopProcessFn signals stop, a rebalance is detected, or the poll times out (reader caught up).
func (k *Kafka) processKafkaMessages(ctx context.Context, readerID int, reader *kgo.Client, assignedPartitions []types.PartitionKey, stopProcessFn func(record types.KafkaRecord) (bool, error)) error {
	var iter *kgo.FetchesRecordIter
	pollAttempt := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// checked before every poll and every record so a rebalance signal is never delayed by full batch processing.
			if stopProcessing, err := k.readerManager.FetchExitState(); stopProcessing {
				return err
			}

			// iter being nil triggers first poll.
			// iter.Done() being true triggers next polls when the current batch is fully consumed.
			if iter == nil || iter.Done() {
				pollAttempt++
				pollCtx, cancel := context.WithTimeout(ctx, kafkapkg.CDCPollTimeout)
				logger.Infof(
					"reader[%d]: poll attempt %d starting (timeout=%s)",
					readerID, pollAttempt, kafkapkg.CDCPollTimeout,
				)
				// poll for new messages
				fetches := reader.PollFetches(pollCtx)
				pollCtxErr := pollCtx.Err()
				cancel()

				recordCount := 0
				fetches.EachRecord(func(_ *kgo.Record) { recordCount++ })

				// no new messages for CDCPollTimeout means reader has caught up; exit cleanly without error.
				if errors.Is(pollCtxErr, context.DeadlineExceeded) {
					_, generationID := reader.GroupMetadata()
					logger.Warnf(
						"reader[%d]: poll attempt %d timed out after %s with %d records (generation id=%d); consumer sees no new data",
						readerID, pollAttempt, kafkapkg.CDCPollTimeout, recordCount, generationID,
					)
					k.logReaderOffsetSnapshot(ctx, readerID, assignedPartitions, "poll timeout")
					return nil
				}

				// any fetch error (including parent ctx cancellation) is non-retryable.
				// For more info, go through the documentation: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#Fetches.Errors
				if err := fetches.Err(); err != nil {
					return fmt.Errorf("%w: error reading message in Kafka CDC sync: %w", constants.ErrNonRetryable, err)
				}

				logger.Infof("reader[%d]: poll attempt %d returned %d records", readerID, pollAttempt, recordCount)

				// wrap batch into iterator
				iter = fetches.RecordIter()
				// check exit state again before processing the batch
				continue
			}

			message := iter.Next()
			data, key, err := k.parseKafkaData(message)
			if err != nil {
				logger.Warnf("failed to parse message of topic: %s, partition: %d, offset %d, error: %s", message.Topic, message.Partition, message.Offset, err)
			} else if data != nil {
				// data map will be nil (in cases like null and unparseable message values) so nil check is required
				data[Partition] = message.Partition
				data[Offset] = message.Offset
				data[Key] = key
				data[KafkaTimestamp], err = typeutils.ReformatDate(message.Timestamp, true)
				if err != nil {
					return fmt.Errorf("failed to reformat date: %s", err)
				}
			}

			stopProcessing, err := stopProcessFn(types.KafkaRecord{Data: data, Message: message})
			if err != nil {
				return err
			}
			if stopProcessing {
				return nil
			}
		}
	}
}

func (k *Kafka) parseKafkaData(message *kgo.Record) (map[string]interface{}, string, error) {
	// helper to parse data bytes (value or key)
	parseData := func(data []byte) (interface{}, error) {
		// if data is not in confluent wire format, it is assumed to be standard json currently
		if isConfluentWireFormat(data) {
			if k.schemaRegistryClient == nil {
				return decodeJSONMessage(data[5:])
			}

			// get schemaID
			schemaID := binary.BigEndian.Uint32(data[1:5])

			// fetch schema
			schema, err := k.schemaRegistryClient.FetchSchema(schemaID)
			if err != nil {
				return nil, fmt.Errorf("failed to fetch schema %d: %s", schemaID, err)
			}

			// decode data based on format
			switch schema.SchemaType {
			case types.SchemaTypeAvro:
				return decodeAvroMessage(data[5:], schema.Codec)
			case types.SchemaTypeJSON:
				return decodeJSONMessage(data[5:])
			default:
				return nil, fmt.Errorf("unsupported schema type: %s", schema.SchemaType)
			}
		}

		return decodeJSONMessage(data)
	}

	// 1. Parse Message Value
	var messageValue map[string]interface{}
	if message.Value != nil {
		valDecoded, err := parseData(message.Value)
		if err != nil {
			return nil, "", err
		}
		if vm, ok := valDecoded.(map[string]interface{}); ok {
			messageValue = vm
		} else {
			return nil, "", fmt.Errorf("expected format for message value is not supported, got %s of type %T", valDecoded, valDecoded)
		}
	}

	// 2. Parse Message Key
	var keyValue string
	if len(message.Key) > 0 {
		parsedKey, err := parseData(message.Key)
		if err != nil {
			// standard fallback: raw key as string
			logger.Warnf("failed to parse key for topic=%s partition=%d offset=%d: %s, using raw string", message.Topic, message.Partition, message.Offset, err)
			keyValue = string(message.Key)
		} else {
			switch v := parsedKey.(type) {
			case string:
				keyValue = v
			case []byte:
				keyValue = string(v)
			default:
				bytes, err := json.Marshal(v)
				if err != nil {
					logger.Warnf("failed to marshal decoded key at offset %d: %s, using raw string", message.Offset, err)
					keyValue = string(message.Key)
				} else {
					keyValue = string(bytes)
				}
			}
		}
	}

	return messageValue, keyValue, nil
}

// decode kafka json message
func decodeJSONMessage(value []byte) (map[string]interface{}, error) {
	var data map[string]interface{}

	decoder := json.NewDecoder(bytes.NewReader(value))
	// use json.Number to avoid automatic conversion of numbers to float64
	decoder.UseNumber()

	if err := decoder.Decode(&data); err != nil {
		return nil, err
	}
	return data, nil
}

// decode kafka avro binary message
func decodeAvroMessage(data []byte, codec *goavro.Codec) (interface{}, error) {
	nativeDatum, _, err := codec.NativeFromBinary(data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode Avro: %s", err)
	}

	if record, ok := nativeDatum.(map[string]interface{}); ok {
		return typeutils.ExtractAvroRecord(record), nil
	}
	return nativeDatum, nil
}

// syncCommittedOffsetsWithMetadata ensures consumer group offsets match destination metadata.
// Returns true if a recovery sync was performed for this reader.
func (k *Kafka) syncCommittedOffsetsWithMetadata(ctx context.Context, readerID int, reader *kgo.Client, metadataStates map[string]any, assignedPartitions []types.PartitionKey) (bool, error) {
	streamMetadata := make(map[string]map[string]any)
	var recordsToCommit []*kgo.Record

	// iterate over all assigned partitions for this reader
	for _, assignedPartition := range assignedPartitions {
		currentPartitionID := assignedPartition.Partition
		currentTopic := assignedPartition.Topic

		partitionMeta, ok := k.readerManager.GetPartitionMeta(kafkapkg.PartitionMetadataKey(currentTopic, currentPartitionID))
		if !ok {
			return false, fmt.Errorf("%w: assigned partition %s:%d missing from partition metadata", constants.ErrNonRetryable, currentTopic, currentPartitionID)
		}

		streamID := partitionMeta.Stream.ID()
		if _, loaded := streamMetadata[streamID]; !loaded {
			// Load destination metadata for this stream once (unmarshal or empty default) and cache it.
			rawMetadataStateValue := metadataStates[streamID]
			if rawMetadataStateValue == nil {
				// if metadata state is not present, create an empty metadata map.
				streamMetadata[streamID] = map[string]any{}
			} else {
				// if metadata state is present, unmarshal it and cache it.
				mtStateStr, ok := rawMetadataStateValue.(string)
				if !ok {
					return false, fmt.Errorf("stream[%s]: failed to typecast metadata state of type[%T] to string", streamID, rawMetadataStateValue)
				}

				var parsedMetadataStateValue map[string]any
				decoder := json.NewDecoder(bytes.NewReader([]byte(mtStateStr)))
				decoder.UseNumber()
				if err := decoder.Decode(&parsedMetadataStateValue); err != nil {
					return false, fmt.Errorf("stream[%s]: failed to unmarshal metadata state: %s", streamID, err)
				}

				// check if consumer group id mismatch
				metaConsumerGroupID, _ := parsedMetadataStateValue["consumer_group_id"].(string)
				if metaConsumerGroupID != "" && metaConsumerGroupID != k.consumerGroupID {
					return false, fmt.Errorf("%w: stream[%s]: consumer_group_id mismatch (destination metadata=%q, current=%q), run clear destination and restart", constants.ErrNonRetryable, streamID, metaConsumerGroupID, k.consumerGroupID)
				}

				// cache the parsed metadata state for this stream
				streamMetadata[streamID] = parsedMetadataStateValue
			}
		}

		// kafkaCommittedOffset = broker next offset, metaCommittedOffset = destination next offset.
		// partitionMeta is populated in PartitionsForStream only for partitions with unconsumed messages(committedOffset < EndOffset).
		kafkaCommittedOffset := partitionMeta.CommittedOffset
		partitionKey := fmt.Sprintf("partition_%d", currentPartitionID)
		offsetValue, hasMeta := streamMetadata[streamID][partitionKey]
		if !hasMeta {
			logger.Infof(
				"reader[%d] topic %s partition %d: no destination metadata cursor (broker_committed=%d end_offset=%d); consuming from broker offset",
				readerID, currentTopic, currentPartitionID, kafkaCommittedOffset, partitionMeta.EndOffset,
			)
			continue
		}

		metaCommittedOffset, err := typeutils.ReformatInt64(offsetValue)
		if err != nil {
			return false, fmt.Errorf("stream[%s] topic %s partition %d: invalid metadata offset: %s", streamID, currentTopic, currentPartitionID, err)
		}

		logger.Infof(
			"reader[%d] topic %s partition %d offset check: broker_committed=%d destination_next=%d end_offset=%d",
			readerID, currentTopic, currentPartitionID, kafkaCommittedOffset, metaCommittedOffset, partitionMeta.EndOffset,
		)

		// destination metadata offset must not exceed the current partition end offset.
		// this can happen when a topic is deleted and recreated with fewer messages.
		if metaCommittedOffset > partitionMeta.EndOffset {
			return false, fmt.Errorf("%w: stream[%s] topic %s partition %d metadata offset: %d exceeds partition end offset: %d, run clear destination and restart", constants.ErrNonRetryable, streamID, currentTopic, currentPartitionID, metaCommittedOffset, partitionMeta.EndOffset)
		}

		// kafkaCommittedOffset must not exceed metaCommittedOffset.
		if kafkaCommittedOffset >= 0 && kafkaCommittedOffset > metaCommittedOffset {
			return false, fmt.Errorf("%w: stream[%s] topic %s partition %d broker committed offset: %d is ahead of destination metadata offset: %d, run clear destination and restart", constants.ErrNonRetryable, streamID, currentTopic, currentPartitionID, kafkaCommittedOffset, metaCommittedOffset)
		}

		// Broker is behind destination (or has no committed offset yet): align broker to destination before consuming.
		if kafkaCommittedOffset < 0 || metaCommittedOffset > kafkaCommittedOffset {
			logger.Infof(
				"reader[%d] topic %s partition %d: broker behind destination, will commit offset %d to broker",
				readerID, currentTopic, currentPartitionID, metaCommittedOffset-1,
			)
			recordsToCommit = append(recordsToCommit, &kgo.Record{Topic: currentTopic, Partition: currentPartitionID, Offset: metaCommittedOffset - 1, LeaderEpoch: -1})
		}
	}

	if len(recordsToCommit) == 0 {
		logger.Infof("reader[%d]: no broker/destination offset recovery needed", readerID)
		return false, nil
	}

	logger.Infof("reader[%d]: crash-recovery detected, committing %d partitions to broker", readerID, len(recordsToCommit))
	if err := reader.CommitRecords(ctx, recordsToCommit...); err != nil {
		return false, fmt.Errorf("recovery offset commit failed, cannot continue (would cause duplicates): %s", err)
	}
	return true, nil
}

func (k *Kafka) logReaderOffsetSnapshot(ctx context.Context, readerID int, assignedPartitions []types.PartitionKey, phase string) {
	topicsSeen := make(map[string]struct{})
	for _, partition := range assignedPartitions {
		topicsSeen[partition.Topic] = struct{}{}
	}

	for topic := range topicsSeen {
		committedTopicOffsets, err := k.readerManager.FetchCommittedOffsets(ctx, topic)
		if err != nil {
			logger.Warnf("reader[%d] %s: failed to fetch broker offsets for topic %s: %s", readerID, phase, topic, err)
			continue
		}

		startOffsets, endOffsets, err := k.readerManager.ListTopicOffsets(ctx, topic)
		if err != nil {
			logger.Warnf("reader[%d] %s: failed to list topic offsets for %s: %s", readerID, phase, topic, err)
			continue
		}

		for _, partition := range assignedPartitions {
			if partition.Topic != topic {
				continue
			}
			_, endOffsetDetail, found := k.readerManager.GetPartitionOffsets(startOffsets, endOffsets, topic, partition.Partition)
			if !found {
				continue
			}
			brokerCommitted, hasCommitted := committedTopicOffsets[partition.Partition]
			if !hasCommitted {
				brokerCommitted = -1
			}
			partitionMeta, hasMeta := k.readerManager.GetPartitionMeta(kafkapkg.PartitionMetadataKey(topic, partition.Partition))
			cachedEnd := int64(-1)
			cachedCommitted := int64(-1)
			if hasMeta {
				cachedEnd = partitionMeta.EndOffset
				cachedCommitted = partitionMeta.CommittedOffset
			}
			logger.Infof(
				"reader[%d] %s snapshot topic=%s partition=%d broker_committed=%d end_offset=%d cached_committed=%d cached_end=%d",
				readerID, phase, topic, partition.Partition, brokerCommitted, endOffsetDetail.Offset, cachedCommitted, cachedEnd,
			)
		}
	}
}
