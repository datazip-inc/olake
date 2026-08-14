PROBE.kafka = docker exec kafkaJson kafka-topics --bootstrap-server localhost:9092 --list && \
              docker exec kafkaAvro kafka-topics --bootstrap-server localhost:9092 --list && \
              curl -f http://localhost:8081/subjects

# Kafka-only: consumer-group rebalance recovery. CI covers it inside the kafka
# matrix job's `test.driver.kafka`; this target is the local one-shot.
.PHONY: test.kafka.rebalance
test.kafka.rebalance: prepare.kafka
	@$(call driver_test_setup,kafka)
	$(GO_ENV.kafka) cd tests && go test -v ./kafka/... -timeout 0 -count=1 -run 'Rebalance'

HELP_TARGETS += test.kafka.rebalance
HELP.test.kafka.rebalance = Kafka consumer-group rebalance recovery tests
