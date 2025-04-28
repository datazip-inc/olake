/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.primitives.Ints;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.literal.NamedLiteral;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.ConfigValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.FORMAT_VERSION;


/**
 * @author Ismail Simsek
 */
public class IcebergUtil {
  protected static final Logger LOGGER = LoggerFactory.getLogger(IcebergUtil.class);
  protected static final ObjectMapper jsonObjectMapper = new ObjectMapper();
  protected static final DateTimeFormatter dtFormater = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC);


  public static Map<String, String> getConfigSubset(Config config, String prefix) {
    final Map<String, String> ret = new HashMap<>();

    for (String propName : config.getPropertyNames()) {
      if (propName.startsWith(prefix)) {
        final String newPropName = propName.substring(prefix.length());
        ret.put(newPropName, config.getValue(propName, String.class));
      }
    }

    return ret;
  }


  public static boolean configIncludesUnwrapSmt() {
    return configIncludesUnwrapSmt(ConfigProvider.getConfig());
  }

  //@TestingOnly
  static boolean configIncludesUnwrapSmt(Config config) {
    // first lets find the config value for debezium statements
    ConfigValue stms = config.getConfigValue("debezium.transforms");
    if (stms == null || stms.getValue() == null || stms.getValue().isEmpty() || stms.getValue().isBlank()) {
      return false;
    }

    String[] stmsList = stms.getValue().split(",");
    final String regexVal = "^io\\.debezium\\..*transforms\\.ExtractNew.*State$";
    // we have debezium statements configured! let's check if we have event flattening config is set.
    for (String stmName : stmsList) {
      ConfigValue stmVal = config.getConfigValue("debezium.transforms." + stmName + ".type");
      if (stmVal != null && stmVal.getValue() != null && !stmVal.getValue().isEmpty() && !stmVal.getValue().isBlank() && stmVal.getValue().matches(regexVal)) {
        return true;
      }
    }

    return false;
  }


  public static <T> T selectInstance(Instance<T> instances, String name) {

    Instance<T> instance = instances.select(NamedLiteral.of(name));
    if (instance.isAmbiguous()) {
      throw new RuntimeException("Multiple batch size wait class named '" + name + "' were found");
    } else if (instance.isUnsatisfied()) {
      throw new RuntimeException("No batch size wait class named '" + name + "' is available");
    }

    LOGGER.info("Using {}", instance.getClass().getName());
    return instance.get();
  }

  public static Table createIcebergTable(Catalog icebergCatalog, TableIdentifier tableIdentifier, Schema schema) {

    if (!((SupportsNamespaces) icebergCatalog).namespaceExists(tableIdentifier.namespace())) {
      ((SupportsNamespaces) icebergCatalog).createNamespace(tableIdentifier.namespace());
      LOGGER.warn("Created namespace:'{}'", tableIdentifier.namespace());
    }
    return icebergCatalog.createTable(tableIdentifier, schema);
  }

  public static Table createIcebergTable(Catalog icebergCatalog, TableIdentifier tableIdentifier,
                                         Schema schema, String writeFormat) {
    return createIcebergTable(icebergCatalog, tableIdentifier, schema, writeFormat, Collections.emptyMap());
  }

  public static Table createIcebergTable(Catalog icebergCatalog, TableIdentifier tableIdentifier,
                                         Schema schema, String writeFormat, Map<String, String> partitionTransforms) {

    LOGGER.warn("Creating table:'{}'\nschema:{}\nrowIdentifier:{}", tableIdentifier, schema,
        schema.identifierFieldNames());

    if (!((SupportsNamespaces) icebergCatalog).namespaceExists(tableIdentifier.namespace())) {
      ((SupportsNamespaces) icebergCatalog).createNamespace(tableIdentifier.namespace());
      LOGGER.warn("Created namespace:'{}'", tableIdentifier.namespace());
    }

    // If we have partition transforms, create a PartitionSpec
    if (partitionTransforms.isEmpty()) {
      // No partitioning - create a table as before
      return icebergCatalog.buildTable(tableIdentifier, schema)
          .withProperty(FORMAT_VERSION, "2")
          .withProperty(DEFAULT_FILE_FORMAT, writeFormat.toLowerCase(Locale.ENGLISH))
          .withSortOrder(IcebergUtil.getIdentifierFieldsAsSortOrder(schema))
          .create();
    } else {
      // Create a table with partitioning
      LOGGER.info("Creating table with partitioning: {}", partitionTransforms);
      
      // Start building the table
      PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(schema);
      
      // Apply each partition transform
      for (Map.Entry<String, String> entry : partitionTransforms.entrySet()) {
        String field = entry.getKey();
        String transform = entry.getValue().toLowerCase(Locale.ENGLISH);
        
        // Apply the appropriate transform based on the specified type
        switch (transform) {
          case "identity":
            specBuilder.identity(field);
            break;
          case "year":
            specBuilder.year(field);
            break;
          case "month":
            specBuilder.month(field);
            break;
          case "day":
            specBuilder.day(field);
            break;
          case "hour":
            specBuilder.hour(field);
            break;
          default:
            // Handle more complex transforms like bucket[N] or truncate[N]
            if (transform.startsWith("bucket[") && transform.endsWith("]")) {
              try {
                int numBuckets = Integer.parseInt(transform.substring(7, transform.length() - 1));
                specBuilder.bucket(field, numBuckets);
              } catch (NumberFormatException e) {
                LOGGER.warn("Invalid bucket transform: {}. Using identity transform instead.", transform);
                specBuilder.identity(field);
              }
            } else if (transform.startsWith("truncate[") && transform.endsWith("]")) {
              try {
                int width = Integer.parseInt(transform.substring(9, transform.length() - 1));
                specBuilder.truncate(field, width);
              } catch (NumberFormatException e) {
                LOGGER.warn("Invalid truncate transform: {}. Using identity transform instead.", transform);
                specBuilder.identity(field);
              }
            } else {
              LOGGER.warn("Unknown transform: {}. Using identity transform instead.", transform);
              specBuilder.identity(field);
            }
        }
      }
      
      // Create the table with the partition spec
      return icebergCatalog.buildTable(tableIdentifier, schema)
          .withProperty(FORMAT_VERSION, "2")
          .withProperty(DEFAULT_FILE_FORMAT, writeFormat.toLowerCase(Locale.ENGLISH))
          .withPartitionSpec(specBuilder.build())
          .withSortOrder(IcebergUtil.getIdentifierFieldsAsSortOrder(schema))
          .create();
    }
  }

  private static SortOrder getIdentifierFieldsAsSortOrder(Schema schema) {
    SortOrder.Builder sob = SortOrder.builderFor(schema);
    for (String fieldName : schema.identifierFieldNames()) {
      sob = sob.asc(fieldName);
    }

    return sob.build();
  }

  public static Optional<Table> loadIcebergTable(Catalog icebergCatalog, TableIdentifier tableId) {
    try {
      Table table = icebergCatalog.loadTable(tableId);
      return Optional.of(table);
    } catch (NoSuchTableException e) {
      LOGGER.debug("Table not found: {}", tableId.toString());
      return Optional.empty();
    }
  }

  public static FileFormat getTableFileFormat(Table icebergTable) {
    String formatAsString = icebergTable.properties().getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
    return FileFormat.valueOf(formatAsString.toUpperCase(Locale.ROOT));
  }

  public static GenericAppenderFactory getTableAppender(Table icebergTable) {
    final Set<Integer> identifierFieldIds = icebergTable.schema().identifierFieldIds();
    if (identifierFieldIds == null || identifierFieldIds.isEmpty()) {
      return new GenericAppenderFactory(
          icebergTable.schema(),
          icebergTable.spec(),
          null,
          null,
          null)
          .setAll(icebergTable.properties());
    } else {
      return new GenericAppenderFactory(
          icebergTable.schema(),
          icebergTable.spec(),
          Ints.toArray(identifierFieldIds),
          TypeUtil.select(icebergTable.schema(), Sets.newHashSet(identifierFieldIds)),
          null)
          .setAll(icebergTable.properties());
    }
  }

  public static OutputFileFactory getTableOutputFileFactory(Table icebergTable, FileFormat format) {
    return OutputFileFactory.builderFor(icebergTable,
            IcebergUtil.partitionId(), 1L)
        .defaultSpec(icebergTable.spec())
        .operationId(UUID.randomUUID().toString())
        .format(format)
        .build();
  }

  public static int partitionId() {
    return Integer.parseInt(dtFormater.format(Instant.now()));
  }

}
