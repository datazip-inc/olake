package driver

import (
	"errors"
	"strconv"

	"github.com/datazip-inc/olake/utils/errs"
	"github.com/twmb/franz-go/pkg/kerr"
)

// Codes for conditions this driver detects itself. All but the first cover state that no longer
// lines up with the broker: Kafka owns the position, OLake keeps its own copy in the destination
// metadata, and a mismatch is only visible by comparing the two.
const (
	codeBackfillUnsupported     = "kafka.backfill_unsupported"
	codeMetadataStateInvalid    = "kafka.metadata_state_invalid"
	codeConsumerGroupMismatch   = "kafka.consumer_group_mismatch"
	codeOffsetMismatch          = "kafka.offset_mismatch"
	codePartitionMetadataAbsent = "kafka.partition_metadata_absent"
)

// kerrCategories maps a Kafka protocol error code to a failure category. Comments give the
// protocol name as franz-go's own table spells it; that table ships with the dependency, and the
// tests assert the two agree.
//
//	github.com/twmb/franz-go@v1.21.1/pkg/kerr/kerr.go
//
// Only codes a *consumer* can receive are mapped. Produce- and transaction-side codes exist in
// the protocol but cannot reach this driver.
var kerrCategories = map[int16]errs.Category{
	// Authentication with the broker.
	58: errs.AuthFailed, // SASL_AUTHENTICATION_FAILED
	34: errs.AuthFailed, // ILLEGAL_SASL_STATE
	66: errs.AuthFailed, // DELEGATION_TOKEN_EXPIRED

	// ACLs. The principal is known and lacks a right on the topic, group or cluster.
	29: errs.PermissionDenied, // TOPIC_AUTHORIZATION_FAILED
	30: errs.PermissionDenied, // GROUP_AUTHORIZATION_FAILED
	31: errs.PermissionDenied, // CLUSTER_AUTHORIZATION_FAILED
	65: errs.PermissionDenied, // DELEGATION_TOKEN_AUTHORIZATION_FAILED

	// The topic or group does not exist.
	3:  errs.ObjectNotFound, // UNKNOWN_TOPIC_OR_PARTITION
	69: errs.ObjectNotFound, // GROUP_ID_NOT_FOUND

	// The position we asked to resume from is gone. This is the retention window having
	// passed the committed offset — a full re-read, not a retry.
	1: errs.CDCPositionLost, // OFFSET_OUT_OF_RANGE

	// The broker, leader or coordinator is not serving. All are transient and all look the
	// same from here: wait and retry.
	8:  errs.NetworkUnreachable, // BROKER_NOT_AVAILABLE
	13: errs.NetworkUnreachable, // NETWORK_EXCEPTION
	15: errs.NetworkUnreachable, // COORDINATOR_NOT_AVAILABLE
	16: errs.NetworkUnreachable, // NOT_COORDINATOR
	14: errs.NetworkUnreachable, // COORDINATOR_LOAD_IN_PROGRESS
	5:  errs.NetworkUnreachable, // LEADER_NOT_AVAILABLE
	6:  errs.NetworkUnreachable, // NOT_LEADER_FOR_PARTITION
	9:  errs.NetworkUnreachable, // REPLICA_NOT_AVAILABLE
	41: errs.NetworkUnreachable, // NOT_CONTROLLER
	72: errs.NetworkUnreachable, // LISTENER_NOT_FOUND

	// Group membership is being reshuffled and our claim on it is stale. Nothing ran out;
	// another member is taking over, and the run can proceed once the group settles.
	27: errs.ConcurrencyConflict, // REBALANCE_IN_PROGRESS
	22: errs.ConcurrencyConflict, // ILLEGAL_GENERATION
	25: errs.ConcurrencyConflict, // UNKNOWN_MEMBER_ID
	60: errs.ConcurrencyConflict, // REASSIGNMENT_IN_PROGRESS

	// The broker could not read its own log.
	56: errs.ResourceExhausted, // KAFKA_STORAGE_ERROR

	// The bytes on the partition are not what a consumer can read.
	2: errs.SourceReadError, // CORRUPT_MESSAGE
	4: errs.SourceReadError, // INVALID_FETCH_SIZE — a value this driver chose

	// Settings the broker rejected. Each is a field the user set.
	40: errs.ConfigInvalid, // INVALID_CONFIG
	44: errs.ConfigInvalid, // POLICY_VIOLATION
	24: errs.ConfigInvalid, // INVALID_GROUP_ID
	26: errs.ConfigInvalid, // INVALID_SESSION_TIMEOUT
	17: errs.ConfigInvalid, // INVALID_TOPIC_EXCEPTION
	33: errs.ConfigInvalid, // UNSUPPORTED_SASL_MECHANISM
	54: errs.ConfigInvalid, // SECURITY_DISABLED

	// The broker understood the request and will not serve it at this version.
	35: errs.UnsupportedFeature, // UNSUPPORTED_VERSION
	43: errs.UnsupportedFeature, // UNSUPPORTED_FOR_MESSAGE_FORMAT
	76: errs.UnsupportedFeature, // UNSUPPORTED_COMPRESSION_TYPE

	7: errs.Timeout, // REQUEST_TIMED_OUT
}

// Register so ReportFailure can classify without knowing which connector ran. Only Kafka protocol
// evidence is handled here — DNS, TLS, refused connections and deadlines look the same for
// every connector and belong to utils/errs.
func init() { errs.Register(classify) }

// classify reads Kafka's protocol error code. Returns nil for anything else, leaving it to the
// shared standard-library rules.
//
// The category comes from the error, never from the call site: one call can fail on a revoked
// grant, a missing object, contention or a dropped connection.
func classify(err error) *errs.Failure {
	var protocolErr *kerr.Error
	if !errors.As(err, &protocolErr) {
		return nil
	}

	// The broker answered with a protocol error code. It travels with the failure whether or
	// not we have mapped it, so an unmapped code is still identifiable.
	//
	// protocolErr.Retriable is not read: retryability is a property of the
	// category, decided once, and the repo drives retries through constants.ErrNonRetryable.
	f := errs.Failure{Code: strconv.FormatInt(int64(protocolErr.Code), 10)}
	if category, ok := kerrCategories[protocolErr.Code]; ok {
		f.Category = category
		f.ClassifiedBy = errs.ClassifiedByVendor
		return &f
	}
	// A real protocol code with no mapping yet. The code identifies it precisely, so the gap
	// is actionable without guessing at a category.
	f.Category = errs.Unclassified
	f.ClassifiedBy = errs.ClassifiedByDefault
	return &f
}
