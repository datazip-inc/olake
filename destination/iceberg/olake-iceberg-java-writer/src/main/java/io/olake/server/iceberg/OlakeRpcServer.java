package io.olake.server.iceberg;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.olake.server.iceberg.service.CatalogServiceImpl;
import io.olake.server.iceberg.service.WriterServiceImpl;
import io.grpc.Server;
import io.grpc.ServerBuilder;

/**
 * Shared-JVM entry point. Catalog config is parsed once at startup; per-stream
 * context (namespace, upsert, partition-fields, identifier-fields) is now carried
 * on every gRPC request, so a single JVM can serve all streams and chunks of an
 * OLake sync.
 */
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

        int port = Integer.parseInt(stringConfigMap.getOrDefault("port", "50051"));
        int maxMessageSize = Integer.parseInt(
            stringConfigMap.getOrDefault("max-message-size", "" + (1024 * 1024 * 1024)));

        ServerBuilder<?> serverBuilder = ServerBuilder.forPort(port)
                    .maxInboundMessageSize(maxMessageSize);

        CatalogServiceImpl catalogService = new CatalogServiceImpl(icebergCatalog);
        serverBuilder.addService(catalogService);
        LOGGER.info("Registered CatalogServiceImpl service");

        WriterServiceImpl writerService = new WriterServiceImpl(icebergCatalog);
        serverBuilder.addService(writerService);
        LOGGER.info("Registered WriterServiceImpl service");

        Server server = serverBuilder.build().start();

        // Graceful shutdown so the OS sees the gRPC port released cleanly.
        Runtime.getRuntime().addShutdownHook(new Thread(server::shutdown, "olake-grpc-shutdown"));

        LOGGER.info("Server started on port {} with max message size: {}MB",
                    port, (maxMessageSize / (1024 * 1024)));
        server.awaitTermination();
    }
}
