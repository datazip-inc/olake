package io.debezium.server.iceberg;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.serde.DebeziumSerdes;
import io.debezium.server.iceberg.rpc.OlakeArrowIngester;
import io.debezium.server.iceberg.rpc.OlakeRowsIngester;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import jakarta.enterprise.context.Dependent;

/**
 * Shared-JVM entry point. Catalog config is parsed once at startup; per-stream
 * context (namespace, upsert, partition-fields, identifier-fields) is now carried
 * on every gRPC request, so a single JVM can serve all streams and chunks of an
 * OLake sync.
 */
@Dependent
public class OlakeRpcServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(OlakeRpcServer.class);

    protected static final Serde<JsonNode> valSerde = DebeziumSerdes.payloadJson(JsonNode.class);
    protected static final Serde<JsonNode> keySerde = DebeziumSerdes.payloadJson(JsonNode.class);
    final static Configuration hadoopConf = new Configuration();
    final static Map<String, String> icebergProperties = new ConcurrentHashMap<>();
    static Catalog icebergCatalog;
    static Deserializer<JsonNode> valDeserializer;
    static Deserializer<JsonNode> keyDeserializer;

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            LOGGER.error("Please provide a JSON config as an argument.");
            System.exit(1);
        }

        String jsonConfig = args[0];
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Object> configMap = objectMapper.readValue(jsonConfig, new TypeReference<Map<String, Object>>() {
        });

        LOGGER.info("Logs will be output to console only");

        // Only catalog/storage-level config is consumed here. Stream-level context
        // (namespace, upsert, partition-fields, identifier-fields) comes per-request.
        Map<String, String> stringConfigMap = new ConcurrentHashMap<>();
        configMap.forEach((key, value) -> {
            if (value != null) {
                stringConfigMap.put(key, value.toString());
            }
        });

        stringConfigMap.forEach(hadoopConf::set);
        icebergProperties.putAll(stringConfigMap);

        String catalogName = stringConfigMap.getOrDefault("catalog-name", "iceberg");

        icebergCatalog = CatalogUtil.buildIcebergCatalog(catalogName, icebergProperties, hadoopConf);

        valSerde.configure(Collections.emptyMap(), false);
        valDeserializer = valSerde.deserializer();
        keySerde.configure(Collections.emptyMap(), true);
        keyDeserializer = keySerde.deserializer();

        boolean arrowWriterEnabled = Boolean.parseBoolean(
            stringConfigMap.getOrDefault("arrow-writer-enabled", "false"));

        int port = Integer.parseInt(stringConfigMap.getOrDefault("port", "50051"));
        int maxMessageSize = Integer.parseInt(
            stringConfigMap.getOrDefault("max-message-size", "" + (1024 * 1024 * 1024)));

        ServerBuilder<?> serverBuilder = ServerBuilder.forPort(port)
                    .maxInboundMessageSize(maxMessageSize);

        if (arrowWriterEnabled) {
             OlakeArrowIngester oai = new OlakeArrowIngester(icebergCatalog);
             serverBuilder.addService(oai);
             LOGGER.info("Arrow writer enabled - registered OlakeArrowIngester service");
        }

        // Legacy ingester is always registered (Check, GET_OR_CREATE_TABLE, DROP_TABLE
        // and the default RECORDS path all flow through it).
        OlakeRowsIngester ori = new OlakeRowsIngester(icebergCatalog);
        serverBuilder.addService(ori);
        LOGGER.info("Legacy writer enabled - registered OlakeRowsIngester service");

        Server server = serverBuilder.build().start();

        // Graceful shutdown so the OS sees the gRPC port released cleanly.
        Runtime.getRuntime().addShutdownHook(new Thread(server::shutdown, "olake-grpc-shutdown"));

        LOGGER.info("Server started on port {} with max message size: {}MB",
                    port, (maxMessageSize / (1024 * 1024)));
        server.awaitTermination();
    }
}
