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
	"github.com/twmb/franz-go/pkg/kadm"
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
	// Sync recovery offsets on the OLD reader before restarting it.
	// The old reader has a valid consumer group session (member_id + generation_id) from PreCDC,
	// so CommitRecords succeeds. If we restarted first, the new client would have no session yet
	// and the broker would reject the commit with UNKNOWN_MEMBER_ID / FENCED_INSTANCE_ID.
	recovered, err := k.syncCommittedOffsetsWithMetadata(ctx, readerID, k.readerManager.GetReader(readerID), metadataStates)
	if err != nil {
		return nil, fmt.Errorf("reader[%d]: sync committed offsets with metadata failed: %s", readerID, err)
	}
	if recovered {
		logger.Infof("reader[%d]: crash-recovery detected", readerID)
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

	// track processing state
	lastMessages := make(map[types.PartitionKey]*kgo.Record)
	// maintain completed partitions and observed partitions to track loop termination (for the current reader)
	completedPartitions := make(map[types.PartitionKey]struct{}) // completed partitions by the current reader
	observedPartitions := make(map[types.PartitionKey]struct{})  // cached partitions which are observed by the current reader

	defer func() {
		if len(lastMessages) > 0 {
			k.checkpointMessage.Store(readerID, lastMessages)
		}
	}()

	err = k.processKafkaMessages(ctx, reader, func(record types.KafkaRecord) (bool, error) {
		// get current partition metadata and key
		currentPartitionKey := types.PartitionKey{Topic: record.Message.Topic, Partition: record.Message.Partition}
		currentPartitionMeta, exists := k.readerManager.GetPartitionIndex(kafkapkg.PartitionIndexKey(record.Message.Topic, record.Message.Partition))
		if !exists {
			return false, fmt.Errorf("missing partition index for topic %s partition %d", record.Message.Topic, record.Message.Partition)
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

		// check if partition is complete
		if record.Message.Offset >= currentPartitionMeta.EndOffset-1 {
			// mark current partition as completed
			completedPartitions[currentPartitionKey] = struct{}{}

			// check for all other assigned partitions to see if they are also completed
			shouldExit, err := k.checkPartitionCompletion(readerID, completedPartitions, observedPartitions)
			if err != nil || shouldExit {
				return shouldExit, err
			}
		}
		return false, nil
	})

	if err != nil {
		return nil, err
	}

	metadataByStream := make(map[string]any)
	for partitionKey, message := range lastMessages {
		partitionMeta, exists := k.readerManager.GetPartitionIndex(kafkapkg.PartitionIndexKey(partitionKey.Topic, partitionKey.Partition))
		if !exists {
			return nil, fmt.Errorf("missing partition index for topic %s partition %d", partitionKey.Topic, partitionKey.Partition)
		}
		streamID := partitionMeta.Stream.ID()
		offsets, _ := metadataByStream[streamID].(map[int32]int64)
		if offsets == nil {
			offsets = make(map[int32]int64)
			metadataByStream[streamID] = offsets
		}
		offsets[partitionKey.Partition] = message.Offset + 1
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
		if !isValid || len(lastMessages) == 0 {
			logger.Infof("reader %s has no accumulated offsets to commit", readerID)
			return nil
		}

		// Prepare messages for commit and track affected streams
		messages := make([]*kgo.Record, 0, len(lastMessages))
		syncedStreams := make(map[string]types.StreamInterface)

		for partitionKey, message := range lastMessages {
			messages = append(messages, message)

			// Resolve stream for this partition
			partitionID := kafkapkg.PartitionIndexKey(partitionKey.Topic, partitionKey.Partition)
			if partitionMeta, exists := k.readerManager.GetPartitionIndex(partitionID); exists && partitionMeta.Stream != nil {
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
			logger.Debugf("reader %s post cdc: generation id: %d", readerID, generationID)

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
func (k *Kafka) processKafkaMessages(ctx context.Context, reader *kgo.Client, stopProcessFn func(record types.KafkaRecord) (bool, error)) error {
	var iter *kgo.FetchesRecordIter

	for {
		// checked before every poll and every record so a rebalance signal is never delayed by full batch processing.
		if stopProcessing, err := k.readerManager.FetchExitState(); stopProcessing {
			return err
		}

		// iter being nil triggers first poll.
		// iter.Done() being true triggers next polls when the current batch is fully consumed.
		if iter == nil || iter.Done() {
			pollCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			// poll for new messages
			fetches := reader.PollFetches(pollCtx)
			pollCtxErr := pollCtx.Err()
			cancel()

			// no new messages for 10s means reader has caught up; exit cleanly without error.
			if errors.Is(pollCtxErr, context.DeadlineExceeded) {
				logger.Warnf("poll context deadline exceeded: %s", pollCtxErr)
				return nil
			}

			// any fetch error (including parent ctx cancellation) is non-retryable.
			// For more info, go through the documentation: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#Fetches.Errors
			if err := fetches.Err(); err != nil {
				return fmt.Errorf("%w: error reading message in Kafka CDC sync: %w", constants.ErrNonRetryable, err)
			}

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

// syncCommittedOffsetsWithMetadata forwards broker offsets to match destination metadata (next offset to consume).
// reader must be the client from PreCDC (before RestartReader) so CommitRecords has a valid group session.
// Returns true when a recovery commit was performed.
func (k *Kafka) syncCommittedOffsetsWithMetadata(ctx context.Context, readerID int, reader *kgo.Client, metadataStates map[string]any) (bool, error) {
	metaByStream := make(map[string]map[int32]int64)
	committedByTopic := make(map[string]map[int32]int64)
	topicStartOffsets := make(map[string]kadm.ListedOffsets)
	topicEndOffsets := make(map[string]kadm.ListedOffsets)
	var recordsToCommit []*kgo.Record

	for _, ap := range k.readerManager.ReaderPartitionMap[readerID] {
		partitionMeta, ok := k.readerManager.GetPartitionIndex(kafkapkg.PartitionIndexKey(ap.Topic, ap.Partition))
		if !ok {
			return false, fmt.Errorf("assigned partition %s:%d missing from partition index", ap.Topic, ap.Partition)
		}

		streamID := partitionMeta.Stream.ID()
		if _, loaded := metaByStream[streamID]; !loaded {
			raw := metadataStates[streamID]
			if raw == nil {
				metaByStream[streamID] = map[int32]int64{}
			} else {
				mtStateStr, ok := raw.(string)
				if !ok {
					return false, fmt.Errorf("failed to typecast metadata state of type[%T] to string", raw)
				}
				parsed := make(map[int32]int64)
				if err := json.Unmarshal([]byte(mtStateStr), &parsed); err != nil {
					return false, fmt.Errorf("stream[%s]: failed to unmarshal metadata state: %s", streamID, err)
				}
				metaByStream[streamID] = parsed
			}
		}

		if _, loaded := committedByTopic[ap.Topic]; !loaded {
			committed, err := k.readerManager.FetchCommittedOffsets(ctx, ap.Topic)
			if err != nil {
				return false, fmt.Errorf("failed to fetch committed offsets for topic %s: %s", ap.Topic, err)
			}
			committedByTopic[ap.Topic] = committed
		}
		lastCommitted := committedByTopic[ap.Topic][ap.Partition]

		if _, loaded := topicEndOffsets[ap.Topic]; !loaded {
			start, end, err := k.readerManager.ListTopicOffsets(ctx, ap.Topic)
			if err != nil {
				return false, fmt.Errorf("failed to list offsets for topic %s: %s", ap.Topic, err)
			}
			topicStartOffsets[ap.Topic] = start
			topicEndOffsets[ap.Topic] = end
		}

		startDetail, endDetail, boundsFound := k.readerManager.GetPartitionOffsets(
			topicStartOffsets[ap.Topic], topicEndOffsets[ap.Topic], ap.Topic, ap.Partition,
		)
		if !boundsFound {
			logger.Warnf("reader[%d]: stream[%s] topic %s partition %d missing offset bounds, skipping recovery commit", readerID, streamID, ap.Topic, ap.Partition)
			continue
		}

		logStart, logEnd := startDetail.Offset, endDetail.Offset
		invalidBroker := lastCommitted >= 0 && (lastCommitted > logEnd || lastCommitted < logStart)

		metaOffset, hasMeta := metaByStream[streamID][ap.Partition]
		brokerNext := logEnd
		if hasMeta {
			brokerNext = metaOffset
			if brokerNext < logStart {
				logger.Warnf("reader[%d]: stream[%s] topic %s partition %d metadata offset %d before log start %d, bumping to %d", readerID, streamID, ap.Topic, ap.Partition, brokerNext, logStart, logStart)
				brokerNext = logStart
			} else if brokerNext > logEnd {
				logger.Warnf("reader[%d]: stream[%s] topic %s partition %d metadata offset %d past log end %d, capping to %d", readerID, streamID, ap.Topic, ap.Partition, brokerNext, logEnd, logEnd)
				brokerNext = logEnd
			}
		}

		needsRecovery := invalidBroker || (hasMeta && (lastCommitted < 0 || brokerNext > lastCommitted))
		if !needsRecovery {
			continue
		}
		if invalidBroker {
			logger.Warnf("reader[%d]: stream[%s] topic %s partition %d broker offset %d outside log [start=%d end=%d), rewinding to %d", readerID, streamID, ap.Topic, ap.Partition, lastCommitted, logStart, logEnd, brokerNext)
		}

		recordsToCommit = append(recordsToCommit, &kgo.Record{
			Topic: ap.Topic, Partition: ap.Partition, Offset: brokerNext - 1,
			LeaderEpoch: -1,
		})
	}

	if len(recordsToCommit) == 0 {
		return false, nil
	}

	logger.Infof("reader[%d]: crash-recovery detected, committing %d partitions to broker", readerID, len(recordsToCommit))
	if err := reader.CommitRecords(ctx, recordsToCommit...); err != nil {
		return false, fmt.Errorf("recovery offset commit failed, cannot continue (would cause duplicates): %s", err)
	}
	return true, nil
}
