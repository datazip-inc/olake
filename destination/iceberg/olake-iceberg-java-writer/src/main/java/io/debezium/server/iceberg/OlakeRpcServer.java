package io.debezium.server.iceberg;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import io.debezium.server.iceberg.rpc.ArrowIngestRouter;
import io.debezium.server.iceberg.rpc.RowsIngestRouter;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import jakarta.enterprise.context.Dependent;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Dependent
public class OlakeRpcServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(OlakeRpcServer.class);

    final static Configuration hadoopConf = new Configuration();
    final static Map<String, String> icebergProperties = new ConcurrentHashMap<>();
    static Catalog icebergCatalog;

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

        // Convert all config values to strings for hadoopConf / iceberg catalog properties.
        // NOTE: table-namespace / upsert / partition-fields are no longer process-launch
        // config. A single process now serves many writer threads, so these per-writer
        // settings arrive in each request's metadata and are applied per thread_id by the
        // router services below.
        Map<String, String> stringConfigMap = new ConcurrentHashMap<>();
        configMap.forEach((key, value) -> {
            if (value != null) {
                stringConfigMap.put(key, value.toString());
            }
        });

        stringConfigMap.forEach(hadoopConf::set);
        icebergProperties.putAll(stringConfigMap);
        String catalogName = "iceberg";
        if (stringConfigMap.get("catalog-name") != null) {
            catalogName = stringConfigMap.get("catalog-name");
        }

        icebergCatalog = CatalogUtil.buildIcebergCatalog(catalogName, icebergProperties, hadoopConf);

        boolean arrowWriterEnabled = false;
        if (stringConfigMap.get("arrow-writer-enabled") != null) {
             arrowWriterEnabled = Boolean.parseBoolean(stringConfigMap.get("arrow-writer-enabled"));
        }

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
        
        ServerBuilder<?> serverBuilder = ServerBuilder.forPort(port)
                    .maxInboundMessageSize(maxMessageSize);

        // A single registered router service per type multiplexes all writer threads,
        // creating one per-thread worker (OlakeArrowIngester / OlakeRowsIngester) keyed
        // by thread_id. Only the Catalog is shared process-wide.
        if (arrowWriterEnabled) {
             serverBuilder.addService(new ArrowIngestRouter(icebergCatalog));
             LOGGER.info("Arrow writer enabled - registered ArrowIngestRouter (per-thread workers)");
        }

        // Check()/Setup() and the legacy write path always need the rows service.
        serverBuilder.addService(new RowsIngestRouter(icebergCatalog));
        LOGGER.info("Registered RowsIngestRouter (per-thread workers)");

        Server server = serverBuilder.build().start();

        // Log server startup without exposing potentially sensitive configuration details
        LOGGER.info("Server started on port {} with max message size: {}MB", 
                    port, (maxMessageSize / (1024 * 1024)));
        server.awaitTermination();
    }
}
