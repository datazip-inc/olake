package driver

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	kafkapkg "github.com/datazip-inc/olake/pkg/kafka"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/linkedin/goavro/v2"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const (
	partitionCount                = 5
	rebalanceBulkMessageCount     = 100_000
	rebalanceBulkPartition        = int32(0)
	rebalanceBulkBatchSize        = 500
	kafkaJSONIntegrationBroker    = "127.0.0.1:29092"
	rebalanceConsumerGroupID      = "kafka-Json-integration-test-group"
	kafkaJSONIntegrationStatsPath = "./testdata/json/stats.json"
	avroSchemaRegistryURL         = "http://127.0.0.1:8081"

	// Base Avro schema
	avroSchema = `{
		"type":"record",
		"name":"test",
		"fields":[
			{"name":"int32_value","type":"int"},
			{"name":"int64_value","type":"long"},
			{"name":"float32_value","type":"float"},
			{"name":"float64_value","type":"double"},
			{"name":"boolean","type":"boolean"},
			{"name":"timestamp_value","type":{"type":"long","logicalType":"timestamp-micros"}},
			{"name":"string_value","type":"string"},
			{"name":"int_value","type":"int"},
			{"name":"float_value","type":"float"},
			{"name":"col_excluded","type":"int"}
		]
	}`

	// Updated Avro schema
	updatedAvroSchema = `{
		"type":"record",
		"name":"test",
		"fields":[
			{"name":"int32_value","type":"int"},
			{"name":"int64_value","type":"long"},
			{"name":"float32_value","type":"float"},
			{"name":"float64_value","type":"double"},
			{"name":"boolean","type":"boolean"},
			{"name":"timestamp_value","type":{"type":"long","logicalType":"timestamp-micros"}},
			{"name":"string_value","type":"string"},
			{"name":"int_value","type":"long"},
			{"name":"float_value","type":"double"},
			{"name":"col_excluded","type":"int"},
			{"name":"col_included","type":"int","default": 102}
		]
	}`
)

var (
	// rebalance trigger
	rebalanceTriggerCancel context.CancelFunc
	rebalanceTriggerDone   chan struct{} // closed when the trigger goroutine has fully exited

	// JSON message keys must be valid JSON objects (driver parses keys as JSON before falling back to raw bytes).
	jsonKey          = []byte(`{"key":"json-key"}`)
	jsonValue        = []byte(`{"int_value": 100,"float_value": 99.99,"boolean": true,"timestamp_value": "2026-03-22T14:30:00Z","string_value": "test_string", "col_excluded": 101}`)
	jsonUpdatedValue = []byte(`{"int_value": 100,"float_value": 99.99,"boolean": true,"timestamp_value": "2026-03-22T14:30:00Z","string_value": "test_string", "col_excluded": 101, "col_included": 102}`)
	jsonFilterValue  = []byte(`{"string_value": "","float_value": 99.99,"col_excluded": 101}`)

	// Avro
	avroKey   = []byte(`{"key":"avro-key"}`)
	avroValue = map[string]interface{}{
		"int32_value":     int32(132),
		"int64_value":     int64(6400000000),
		"float32_value":   float32(32.5),
		"float64_value":   float64(64.6464),
		"boolean":         true,
		"timestamp_value": int64(time.Date(2026, 3, 22, 14, 30, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
		"string_value":    "test_string",
		"int_value":       int32(100),
		"float_value":     float32(64.6464),
		"col_excluded":    int32(101),
	}

	avroFilterValue = map[string]interface{}{
		"int32_value":     int32(132),
		"int64_value":     int64(6400000000),
		"float32_value":   float32(32.5),
		"float64_value":   float64(64.6464),
		"boolean":         true,
		"timestamp_value": int64(time.Date(2026, 3, 22, 14, 30, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
		"string_value":    "",
		"int_value":       int32(100),
		"float_value":     float32(64.6464),
		"col_excluded":    int32(101),
	}

	avroUpdatedValue = map[string]interface{}{
		"int32_value":     int32(132),
		"int64_value":     int64(6400000000),
		"float32_value":   float32(32.5),
		"float64_value":   float64(64.6464),
		"boolean":         true,
		"timestamp_value": int64(time.Date(2026, 3, 22, 14, 30, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
		"string_value":    "test_string",
		"int_value":       int64(100),
		"float_value":     float64(64.6464),
		"col_excluded":    int32(101),
		"col_included":    int32(102),
	}
)

// ExecuteQueryJSON executes Kafka queries for testing based on the operation type
func ExecuteQueryJSON(ctx context.Context, t *testing.T, streams []string, operation string, fileConfig bool) {
	t.Helper()

	var kafkaJSONBroker string
	if fileConfig {
		var config Config
		utils.UnmarshalFile("./testdata/source.json", &config, false)
		kafkaJSONBroker = config.BootstrapServers
	} else {
		kafkaJSONBroker = kafkaJSONIntegrationBroker
	}

	// kafka client
	client, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaJSONBroker),
		kgo.DefaultProduceTopic(streams[0]),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	require.NoError(t, err)
	defer client.Close()

	switch operation {
	case "create":
		createKafkaTopic(ctx, t, client, streams[0])

	case "clean":
		deleteKafkaTopic(ctx, t, client, streams[0])
		createKafkaTopic(ctx, t, client, streams[0])

	case "drop":
		deleteKafkaTopic(ctx, t, client, streams[0])

	case "add":
		// 5 messages inserted with different partitions
		for partition := range partitionCount {
			writeMessagesWithRetry(ctx, t, client, &kgo.Record{Key: jsonKey, Value: jsonValue, Partition: int32(partition)})
		}
		writeMessagesWithRetry(ctx, t, client, &kgo.Record{Key: jsonKey, Value: jsonFilterValue})
		t.Logf("Added 6 messages to topic '%s' (one per partition and one for filters)", streams[0])

	case "update":
		writeMessagesWithRetry(ctx, t, client, &kgo.Record{Key: jsonKey, Value: jsonUpdatedValue})
		t.Logf("Added 1 updated message to topic '%s'", streams[0])

	case "insert_2pc":
		// simulate 2PC failure after destination commit: consumer offset on partition 0 lags at 1
		commitConsumerGroupOffset(ctx, t, kafkaJSONBroker, rebalanceConsumerGroupID, streams[0], 0, 1)
		writeMessagesWithRetry(ctx, t, client, &kgo.Record{Key: jsonKey, Value: jsonValue, Partition: 0})
		// add a new partition with one message to simulate evolution of schema map in destination metadata
		addKafkaPartitions(ctx, t, client, streams[0], 1)
		writeMessagesWithRetry(ctx, t, client, &kgo.Record{Key: jsonKey, Value: jsonValue, Partition: 5})
		t.Logf("Rolled back partition %d, added partition %d with 1 message, and added 1 message on partition %d for topic '%s'", 0, 5, 0, streams[0])

	case "rebalance":
		msg := &kgo.Record{Key: jsonKey, Value: jsonValue, Partition: rebalanceBulkPartition}
		for written := 0; written < rebalanceBulkMessageCount; written += rebalanceBulkBatchSize {
			batchCount := min(rebalanceBulkBatchSize, rebalanceBulkMessageCount-written)
			records := make([]*kgo.Record, batchCount)
			for i := range records {
				records[i] = msg
			}
			for {
				err := client.ProduceSync(ctx, records...).FirstErr()
				if err == nil {
					break
				}
				if ctx.Err() != nil {
					require.NoError(t, err, "failed writing kafka rebalance bulk batch (batch_size=%d): %v", batchCount, err)
				}
				time.Sleep(200 * time.Millisecond)
			}
		}
		t.Logf("Added %d messages to topic '%s' on partition %d", rebalanceBulkMessageCount, streams[0], rebalanceBulkPartition)

		topic := streams[0]
		rebalanceCtx, cancel := context.WithCancel(ctx)
		rebalanceTriggerCancel = cancel
		go func() {
			waitForSyncProgress(rebalanceCtx, t, kafkaJSONIntegrationStatsPath)
			if rebalanceCtx.Err() != nil {
				return
			}
			startRebalanceTriggerConsumer(rebalanceCtx, t, kafkaJSONIntegrationBroker, rebalanceConsumerGroupID, topic)
			t.Logf("joined rebalance trigger consumer (group=%s topic=%s)", rebalanceConsumerGroupID, topic)
		}()

	case "stop_rebalance":
		stopRebalanceTrigger()
		t.Logf("stopped rebalance trigger consumer")

	default:
		t.Fatalf("unsupported operation: %s", operation)
	}
}

func waitForSyncProgress(ctx context.Context, t *testing.T, statsPath string) {
	t.Helper()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := os.Stat(statsPath); err == nil {
				var stats struct {
					SyncedRecords int64 `json:"Synced Records"`
				}
				if err := utils.UnmarshalFile(statsPath, &stats, false); err == nil && stats.SyncedRecords > 0 {
					t.Logf("sync started: %d records synced", stats.SyncedRecords)
					return
				}
			}
		}
	}
}

func startRebalanceTriggerConsumer(ctx context.Context, t *testing.T, broker, groupID, topic string) {
	t.Helper()

	instanceID := fmt.Sprintf("rebalance-trigger-%d", time.Now().UnixNano())

	client, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumerGroup(groupID),
		kgo.ClientID(instanceID),
		kgo.InstanceID(instanceID),
		kgo.ConsumeTopics(topic),
		kgo.Balancers(kafkapkg.NewCustomGroupBalancer(map[string]types.PartitionMetaData{
			kafkapkg.PartitionMetadataKey(topic, rebalanceBulkPartition): {PartitionID: rebalanceBulkPartition},
		})),
		kgo.FetchMinBytes(1),
		kgo.DisableAutoCommit(),
	)
	require.NoError(t, err)

	done := make(chan struct{})
	rebalanceTriggerDone = done

	go func() {
		defer func() {
			// kgo.InstanceID disables the automatic LeaveGroup in client.Close() for static members.
			// Send an explicit LeaveGroup so the broker removes this member immediately instead of
			// waiting for session.timeout.ms, which would cause a rebalance in the next sync.
			client.LeaveGroup()
			t.Logf("rebalance trigger consumer: sent LeaveGroup (group=%s instanceID=%s)", groupID, instanceID)
			client.Close()
			close(done)
			t.Logf("rebalance trigger consumer: goroutine exited (group=%s instanceID=%s)", groupID, instanceID)
		}()
		for {
			if ctx.Err() != nil {
				return
			}
			client.PollFetches(ctx)
		}
	}()
}

func stopRebalanceTrigger() {
	if rebalanceTriggerCancel != nil {
		rebalanceTriggerCancel()
		rebalanceTriggerCancel = nil
	}
	// Wait for the goroutine to fully exit and the explicit LeaveGroup to be sent.
	// Without this, the static member lingers in the broker (kgo.InstanceID skips LeaveGroup
	// on Close) and may still hold a partition assignment when the next sync starts.
	if rebalanceTriggerDone != nil {
		<-rebalanceTriggerDone
		rebalanceTriggerDone = nil
	}
}

// ExecuteQueryAvro executes Kafka queries for testing based on the operation type
func ExecuteQueryAvro(ctx context.Context, t *testing.T, streams []string, operation string, fileConfig bool) {
	t.Helper()

	var kafkaAvroBroker string
	if fileConfig {
		var config Config
		utils.UnmarshalFile("./testdata/source.json", &config, false)
		kafkaAvroBroker = config.BootstrapServers
	} else {
		kafkaAvroBroker = "127.0.0.1:29192"
	}
	// kafka client
	client, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaAvroBroker),
		kgo.DefaultProduceTopic(streams[0]),
		kgo.RecordPartitioner(kgo.RoundRobinPartitioner()),
	)
	require.NoError(t, err)
	defer client.Close()

	switch operation {
	case "create":
		createKafkaTopic(ctx, t, client, streams[0])

	case "clean":
		deleteKafkaTopic(ctx, t, client, streams[0])
		createKafkaTopic(ctx, t, client, streams[0])

	case "drop":
		deleteKafkaTopic(ctx, t, client, streams[0])

	case "add":
		// avro codec
		codec, err := goavro.NewCodec(avroSchema)
		require.NoError(t, err)
		schemaID := registerSchemaWithRetry(t, avroSchemaRegistryURL, streams[0], avroSchema)

		// avro messages written
		encodeAndWriteAvro(ctx, t, client, codec, schemaID, avroKey, avroValue, streams[0])
		encodeAndWriteAvro(ctx, t, client, codec, schemaID, avroKey, avroFilterValue, streams[0])
		t.Logf("Added 2 messages to topic '%s' (one valid for sync and one filtered out)", streams[0])

	case "update":
		codec, err := goavro.NewCodec(updatedAvroSchema)
		require.NoError(t, err)
		schemaID := registerSchemaWithRetry(t, avroSchemaRegistryURL, streams[0], updatedAvroSchema)

		// avro message written with new schema
		encodeAndWriteAvro(ctx, t, client, codec, schemaID, avroKey, avroUpdatedValue, streams[0])
		t.Logf("Added 1 updated message to topic '%s'", streams[0])

	default:
		t.Fatalf("unsupported operation: %s", operation)
	}
}

// deleteTopic deletes the topic and waits briefly so the broker can settle (matches prior test harness behavior).
func deleteKafkaTopic(ctx context.Context, t *testing.T, client *kgo.Client, topic string) {
	t.Helper()

	_, err := kadm.NewClient(client).DeleteTopics(ctx, topic)
	require.NoError(t, err, "failed to delete topic '%s'", topic)
	time.Sleep(5 * time.Second)
}

// createTopic creates the test topic with a fixed partition count and replication factor 1.
func createKafkaTopic(ctx context.Context, t *testing.T, client *kgo.Client, topic string) {
	t.Helper()

	res, err := kadm.NewClient(client).CreateTopics(ctx, int32(partitionCount), 1, nil, topic)
	if err == nil && res[topic].Err != nil && !errors.Is(res[topic].Err, kerr.TopicAlreadyExists) {
		err = res[topic].Err
	}
	require.NoError(t, err, "failed to create topic '%s'", topic)
}

// addKafkaPartitions adds Kafka partitions to a topic for 2PC recovery tests.
func addKafkaPartitions(ctx context.Context, t *testing.T, client *kgo.Client, topic string, add int) {
	t.Helper()

	res, err := kadm.NewClient(client).CreatePartitions(ctx, add, topic)
	require.NoError(t, err, "failed to add partitions to topic '%s'", topic)
	require.NoError(t, res.Error(), "partition expansion returned errors for topic '%s'", topic)
	topicRes, topicErr := res.On(topic, nil)
	require.NoError(t, topicErr, "no partition expansion response for topic '%s'", topic)
	require.NoError(t, topicRes.Err, "failed to add %d partition(s) to topic '%s'", add, topic)
	t.Logf("Added %d partition(s) to topic '%s'", add, topic)
}

// commitConsumerGroupOffset rolls back a consumer group offset for 2PC recovery tests.
// nextOffset is the next consumable offset (e.g. 1 means re-read from offset 1).
func commitConsumerGroupOffset(ctx context.Context, t *testing.T, broker, consumerGroupID, topic string, partition int32, nextOffset int64) {
	t.Helper()

	seed, err := kgo.NewClient(kgo.SeedBrokers(broker))
	require.NoError(t, err)
	defer seed.Close()

	adm := kadm.NewClient(seed)

	// Olake uses multiple static group members; force-leave any lingering members, then wait until empty.
	require.NoError(t, utils.RetryOnBackoff(ctx, 60, 2*time.Second, func(ctx context.Context) error {
		groups, describeErr := adm.DescribeGroups(ctx, consumerGroupID)
		if describeErr != nil {
			return describeErr
		}
		group := groups[consumerGroupID]
		if group.Err != nil && !errors.Is(group.Err, kerr.GroupIDNotFound) {
			return group.Err
		}
		if len(group.Members) == 0 {
			return nil
		}

		leaveReq := kmsg.NewPtrLeaveGroupRequest()
		leaveReq.Group = consumerGroupID
		for _, member := range group.Members {
			leaveReq.Members = append(leaveReq.Members, kmsg.LeaveGroupRequestMember{
				MemberID:   member.MemberID,
				InstanceID: member.InstanceID,
			})
		}
		leaveResp, leaveErr := leaveReq.RequestWith(ctx, seed)
		if leaveErr != nil {
			return fmt.Errorf("leave group %s: %w", consumerGroupID, leaveErr)
		}
		if leaveResp.ErrorCode != 0 {
			return fmt.Errorf("leave group %s error code %d", consumerGroupID, leaveResp.ErrorCode)
		}
		return fmt.Errorf("consumer group %s still has %d active member(s)", consumerGroupID, len(group.Members))
	}))

	fetched, err := adm.FetchOffsets(ctx, consumerGroupID)
	require.NoError(t, err)

	toCommit := fetched.Offsets()
	toCommit.Delete(topic, partition)
	toCommit.AddOffset(topic, partition, nextOffset, -1)

	// Delete and re-seed offsets admin-side; avoids joining Olake's multi-member consumer group.
	_, err = adm.DeleteGroup(ctx, consumerGroupID)
	require.NoError(t, err)

	committed, err := adm.CommitOffsets(ctx, consumerGroupID, toCommit)
	require.NoError(t, err)
	require.NoError(t, committed.Error())
	t.Logf("committed consumer group %s on %s:%d at offset %d", consumerGroupID, topic, partition, nextOffset)
}

// Writes a Kafka message with retries until success or context timeout.
func writeMessagesWithRetry(ctx context.Context, t *testing.T, writer *kgo.Client, msg *kgo.Record) {
	t.Helper()

	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	for {
		// write message
		err := writer.ProduceSync(ctx, msg).FirstErr()
		if err == nil {
			return
		}
		if ctx.Err() != nil {
			require.NoError(t, err, "timed out writing kafka message after retries (topic=%q partition=%d): %v", msg.Topic, msg.Partition, err)
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// Registers a schema with retries and returns its schema ID.
func registerSchemaWithRetry(t *testing.T, url, topic, schema string) uint32 {
	t.Helper()

	body, err := json.Marshal(map[string]string{"schema": schema})
	require.NoError(t, err)

	client := &http.Client{Timeout: 10 * time.Second}
	var schemaID uint32

	// retry for schema registration
	err = utils.RetryOnBackoff(context.Background(), 5, 2*time.Second, func(_ context.Context) error {
		// get schema response
		response, err := client.Post(
			fmt.Sprintf("%s/subjects/%s-value/versions", url, topic),
			"application/vnd.schemaregistry.v1+json",
			bytes.NewReader(body),
		)
		if err != nil {
			return err
		}
		defer response.Body.Close()

		if response.StatusCode != http.StatusOK {
			return fmt.Errorf("unexpected status: %d", response.StatusCode)
		}

		var schema struct {
			ID uint32 `json:"id"`
		}
		if err := json.NewDecoder(response.Body).Decode(&schema); err != nil {
			return err
		}

		schemaID = schema.ID
		return nil
	})

	require.NoError(t, err, "failed to register schema")
	return schemaID
}

// encodeAndWriteAvro encodes the Avro value and writes it to the Kafka topic
func encodeAndWriteAvro(ctx context.Context, t *testing.T, writer *kgo.Client, codec *goavro.Codec, schemaID uint32, key []byte, value map[string]interface{}, topic string) {
	t.Helper()
	binaryData, err := codec.BinaryFromNative(nil, value)
	require.NoError(t, err, "encode Avro value to binary (topic=%q, schema_id=%d)", topic, schemaID)

	// Confluent wire format: 1-byte magic (0x00) + 4-byte big-endian schema ID + Avro binary payload.
	msg := make([]byte, 5+len(binaryData))
	msg[0] = 0x00
	binary.BigEndian.PutUint32(msg[1:5], schemaID)
	copy(msg[5:], binaryData)

	// write message
	writeMessagesWithRetry(ctx, t, writer, &kgo.Record{Key: key, Value: msg})
}

// JSON data format resources
var ExpectedKafkaJSONData = map[string]interface{}{
	"int_value":       int64(100),
	"float_value":     float64(99.99),
	"boolean":         true,
	"timestamp_value": arrow.Timestamp(time.Date(2026, 3, 22, 14, 30, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"string_value":    "test_string",
}

var KafkaToDestinationJSONSchema = map[string]string{
	"int_value":       "bigint",
	"float_value":     "double",
	"boolean":         "boolean",
	"timestamp_value": "timestamp",
	"string_value":    "string",
}

var ExpectedKafkaUpdatedJSONData = map[string]interface{}{
	"int_value":       int64(100),
	"float_value":     float64(99.99),
	"boolean":         true,
	"timestamp_value": arrow.Timestamp(time.Date(2026, 3, 22, 14, 30, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"string_value":    "test_string",
	"col_included":    int64(102),
}

var UpdatedKafkaToDestinationJSONSchema = map[string]string{
	"int_value":       "bigint",
	"float_value":     "double",
	"boolean":         "boolean",
	"timestamp_value": "timestamp",
	"string_value":    "string",
	"col_included":    "bigint",
}

// AVRO data format resources
var ExpectedKafkaAvroData = map[string]interface{}{
	"int32_value":     int32(132),
	"int64_value":     int64(6400000000),
	"float32_value":   float32(32.5),
	"float64_value":   float64(64.6464),
	"boolean":         true,
	"timestamp_value": arrow.Timestamp(time.Date(2026, 3, 22, 14, 30, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"string_value":    "test_string",
	"int_value":       int32(100),
	"float_value":     float32(64.6464),
}

var ExpectedKafkaUpdatedAvroData = map[string]interface{}{
	"int32_value":     int32(132),
	"int64_value":     int64(6400000000),
	"float32_value":   float32(32.5),
	"float64_value":   float64(64.6464),
	"boolean":         true,
	"timestamp_value": arrow.Timestamp(time.Date(2026, 3, 22, 14, 30, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"string_value":    "test_string",
	"int_value":       int64(100),       // promoted from int → long
	"float_value":     float64(64.6464), // promoted from float → double
	"col_included":    int32(102),       // new field
}

var KafkaToDestinationAvroSchema = map[string]string{
	"int32_value":     "int",
	"int64_value":     "bigint",
	"float32_value":   "float",
	"float64_value":   "double",
	"boolean":         "boolean",
	"timestamp_value": "timestamp",
	"int_value":       "int",
	"float_value":     "float",
	"string_value":    "string",
}

var UpdatedKafkaToDestinationAvroSchema = map[string]string{
	"int32_value":     "int",
	"int64_value":     "bigint",
	"float32_value":   "float",
	"float64_value":   "double",
	"boolean":         "boolean",
	"timestamp_value": "timestamp",
	"string_value":    "string",
	"int_value":       "bigint",
	"float_value":     "double",
	"col_included":    "int",
}

var ExpectedKafkaDefaultCDCColumnsSchema = map[string]string{
	"_kafka_key":       "string",
	"_kafka_offset":    "bigint",
	"_kafka_partition": "int",
	"_kafka_timestamp": "timestamp",
	"_op_type":         "string",
	"_cdc_timestamp":   "timestamp",
	"_olake_id":        "string",
	"_olake_timestamp": "timestamp",
}
