package kafka

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils/compatibility"
)

// TestKafkaCompatibility pins the backward-compatibility contract on the JSON format, the same single
// format Test2PCIntegration uses: the suite varies only the binary, and avro would add a
// schema-registry axis to the comparison. See tests/testutils/compatibility.go.
func TestKafkaCompatibility(t *testing.T) {
	t.Parallel()
	compatibility.RunBackwardCompatibility(t, func() *compatibility.Test {
		cfg := &compatibility.Test{IntegrationTest: kafkaJSONBaseConfig()}
		// The compatibility floor and its story live in compatibility_rules.json's kafka block.
		// Kafka pipelines interfere across groups: discover enumerates the whole broker, so
		// concurrent groups scan (and race the deletion of) each other's topics.
		cfg.SerialGroups = true
		return cfg
	})
}
