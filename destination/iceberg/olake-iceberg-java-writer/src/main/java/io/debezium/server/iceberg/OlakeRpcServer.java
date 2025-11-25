package io.debezium.server.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import io.debezium.serde.DebeziumSerdes;
import io.debezium.server.iceberg.rpc.OlakeRowsIngester;
import io.debezium.server.iceberg.rpc.OlakeArrowIngester;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import jakarta.enterprise.context.Dependent;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;

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
    static boolean upsert_records = true;
    static boolean createIdFields = true;
    // List to store partition fields and their transforms - preserves order and allows duplicates
    static List<Map<String, String>> partitionTransforms = new ArrayList<>();


    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            LOGGER.error("Please provide a JSON config as an argument.");
            System.exit(1);
        }

        

        String jsonConfig = args[0];
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Object> configMap = objectMapper.readValue(jsonConfig, new TypeReference<Map<String, Object>>() {
        });
        
        // Simplified logging setup - console only
        LOGGER.info("Logs will be output to console only");

        // Convert all config values to strings for hadoopConf
        Map<String, String> stringConfigMap = new ConcurrentHashMap<>();
        configMap.forEach((key, value) -> {
            if (value != null && !"partition-fields".equals(key)) {
                stringConfigMap.put(key, value.toString());
            }
        });
        
        stringConfigMap.forEach(hadoopConf::set);
        icebergProperties.putAll(stringConfigMap);
        String catalogName = "iceberg";
        if (stringConfigMap.get("catalog-name") != null) {
            catalogName = stringConfigMap.get("catalog-name");
        }

        if (stringConfigMap.get("table-namespace") == null) {
            throw new Exception("Iceberg table namespace not found");
        }

        if (stringConfigMap.get("upsert") != null) {
            upsert_records = Boolean.parseBoolean(stringConfigMap.get("upsert"));
        }       

        if (stringConfigMap.get("create-identifier-fields") != null) {
            createIdFields = Boolean.parseBoolean(stringConfigMap.get("create-identifier-fields"));
        }

        // Parse partition fields from array to preserve order
        if (configMap.containsKey("partition-fields")) {
            List<Map<String, String>> partitionFieldsList = (List<Map<String, String>>) configMap.get("partition-fields");
            if (partitionFieldsList != null) {
                for (Map<String, String> partitionField : partitionFieldsList) {
                    String field = partitionField.get("field");
                    String transform = partitionField.get("transform");
                    if (field != null && transform != null) {
                        partitionTransforms.add(partitionField);
                        LOGGER.info("Adding partition field: {} with transform: {}", field, transform);
                    }
                }
            }
        }

        icebergCatalog = CatalogUtil.buildIcebergCatalog(catalogName, icebergProperties, hadoopConf);

        // configure and set
        valSerde.configure(Collections.emptyMap(), false);
        valDeserializer = valSerde.deserializer();
        // configure and set
        keySerde.configure(Collections.emptyMap(), true);
        keyDeserializer = keySerde.deserializer();

        OlakeRowsIngester ori = new OlakeRowsIngester(upsert_records, stringConfigMap.get("table-namespace"), icebergCatalog, partitionTransforms);
        OlakeArrowIngester oai = new OlakeArrowIngester(upsert_records, stringConfigMap.get("table-namespace"), icebergCatalog);
        
        // Build the server to listen on port 50051
        int port = 50051; // Default port
        if (stringConfigMap.get("port") != null) {
            port = Integer.parseInt(stringConfigMap.get("port"));
        }
        
        // Get max message size from config or use a reasonable default 1GB
        int maxMessageSize =  1024 * 1024 * 1024;
        if (stringConfigMap.get("max-message-size") != null) {
            maxMessageSize = Integer.parseInt(stringConfigMap.get("max-message-size"));
        }
        
        Server server = ServerBuilder.forPort(port)
                .addService(ori)
                .addService(oai)
                .maxInboundMessageSize(maxMessageSize)
                .build()
                .start();

        // Log server startup without exposing potentially sensitive configuration details
        LOGGER.info("Server started on port {} with max message size: {}MB", 
                    port, (maxMessageSize / (1024 * 1024)));
        server.awaitTermination();
    }
}
