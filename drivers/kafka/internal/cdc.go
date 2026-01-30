package driver

import (
	"bytes"
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
	"github.com/linkedin/goavro/v2"
	"github.com/segmentio/kafka-go"
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
		BootstrapServers:            k.config.BootstrapServers,
		MaxThreads:                  k.config.MaxThreads,
		ConsumerGroupID:             k.consumerGroupID,
		Dialer:                      k.dialer,
		AdminClient:                 k.adminClient,
		ThreadsEqualTotalPartitions: k.config.ThreadsEqualTotalPartitions,
	})
	return k.readerManager.CreateReaders(ctx, streams, k.consumerGroupID)
}

func (k *Kafka) StreamChanges(ctx context.Context, readerID int, processFn abstract.CDCMsgFn) error {
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

	return k.processKafkaMessages(ctx, reader, func(record types.KafkaRecord) (bool, error) {
		if record.Data == nil {
			logger.Warnf("received nil message value at offset %d for topic %s, partition %d", record.Message.Offset, record.Message.Topic, record.Message.Partition)
			return false, nil
		}

		// get current partition metadata and key
		currentPartitionKey := types.PartitionKey{Topic: record.Message.Topic, Partition: record.Message.Partition}
		currentPartitionMeta, exists := k.readerManager.GetPartitionIndex(fmt.Sprintf("%s:%d", record.Message.Topic, record.Message.Partition))
		if !exists {
			return false, fmt.Errorf("missing partition index for topic %s partition %d", record.Message.Topic, record.Message.Partition)
		}

		// process the change
		err := processFn(ctx, abstract.CDCChange{
			Stream:    currentPartitionMeta.Stream,
			Timestamp: record.Message.Time,
			Kind:      "create",
			Data:      record.Data,
		})
		if err != nil {
			return false, err
		}

		lastMessages[currentPartitionKey] = record.Message

		// check if partition is complete
		if record.Message.Offset >= currentPartitionMeta.EndOffset-1 {
			// mark current partition as completed
			completedPartitions[currentPartitionKey] = struct{}{}

			// check for all other assigned partitions to see if they are also completed
			shouldExit, err := k.checkPartitionCompletion(ctx, readerID, completedPartitions, observedPartitions)
			if err != nil || shouldExit {
				return shouldExit, err
			}
		}
		return false, nil
	})
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
			reader := k.readerManager.GetReader(readerIdx)
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

		k.checkpointMessage.Delete(readerIdx)
		return nil
	}
}

// for processing messages from a Kafka reader.
func (k *Kafka) processKafkaMessages(ctx context.Context, reader *kafka.Reader, stopProcessFn func(record types.KafkaRecord) (bool, error)) error {
	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			return fmt.Errorf("error reading message in Kafka CDC sync: %s", err)
		}

		var data map[string]interface{}
		if message.Value != nil {
			var key string
			data, key, err = k.parseKafkaData(message)
			if err != nil {
				logger.Warnf("failed to parse message at offset %d: %s", message.Offset, err)
				continue
			}
			data[Partition] = message.Partition
			data[Offset] = message.Offset
			data[Key] = key
			data[KafkaTimestamp], err = typeutils.ReformatDate(message.Time, true)
			if err != nil {
				return fmt.Errorf("failed to reformat date: %s", err)
			}
		}

		stopProcessing, err := stopProcessFn(types.KafkaRecord{Data: data, Message: message})
		if err != nil {
			return err
		}
		if stopProcessing {
			break
		}
	}
	return nil
}

func (k *Kafka) parseKafkaData(message kafka.Message) (map[string]interface{}, string, error) {
	// helper to parse data bytes (value or key)
	parseData := func(data []byte) (interface{}, error) {
		// if data is not in confluent wire format, it is assumed to be standard json currently
		if isConfluentWireFormat(data) {
			if k.schemaRegistryClient == nil {
				return decodeJSONMessage(data[5:])
			}

			// get schemaID
			schemaID, err := extractSchemaID(data)
			if err != nil {
				return nil, err
			}

			// fetch schema
			schema, err := k.schemaRegistryClient.FetchSchema(schemaID)
			if err != nil {
				return nil, fmt.Errorf("failed to fetch schema %d: %s", schemaID, err)
			}

			// decode data based on format
			switch schema.SchemaType {
			case types.SchemaTypeAvro:
				decoded, err := decodeAvroMessage(data[5:], schema.Codec)
				if err != nil {
					return nil, err
				}
				if record, ok := decoded.(map[string]interface{}); ok {
					return record, nil
				}
				// For keys or non-record values, it might be primitive
				return decoded, nil
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
			return nil, "", fmt.Errorf("expected format for message value is not supported, got %s", valDecoded)
		}
	}

	// 2. Parse Message Key
	var keyValue string
	if len(message.Key) > 0 {
		parsedKey, err := parseData(message.Key)
		if err != nil {
			// standard fallback: raw key as string
			logger.Warnf("failed to parse key at offset %d: %s, using raw string", message.Offset, err)
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
		return typeutils.ConvertAvroRecord(record), nil
	}
	return nativeDatum, nil
}
