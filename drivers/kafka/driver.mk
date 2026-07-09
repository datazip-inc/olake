# kafka fragment for the root Makefile (see the driver-fragment contract there).

PROBE.kafka = docker exec kafkaJson kafka-topics --bootstrap-server localhost:9092 --list && docker exec kafkaAvro kafka-topics --bootstrap-server localhost:9092 --list && curl -f http://localhost:8081/subjects

# Kafka-only: consumer-group rebalance recovery (CI: kafka-rebalance-tests.yml).
.PHONY: test.kafka.rebalance
test.kafka.rebalance: db.kafka.start db.destination.all.start $(ICEBERG_JAR)
	go test -v ./drivers/kafka/internal/... -timeout 0 -count=1 -run 'Rebalance'
