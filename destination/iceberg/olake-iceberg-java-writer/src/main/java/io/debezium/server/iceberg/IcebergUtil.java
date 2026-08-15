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
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Locale;
import java.util.List;
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


  /** Creates the namespace if it does not already exist, ignoring transient catalog conflicts. */
  private static void ensureNamespace(Catalog icebergCatalog, TableIdentifier tableIdentifier) {
    if (((SupportsNamespaces) icebergCatalog).namespaceExists(tableIdentifier.namespace())) {
      return;
    }
    // multiple threads can try to create the namespace concurrently;
    // AlreadyExists means another thread won the race, which is fine.
    try {
      ((SupportsNamespaces) icebergCatalog).createNamespace(tableIdentifier.namespace());
      LOGGER.warn("Created namespace:'{}'", tableIdentifier.namespace());
    } catch (AlreadyExistsException e) {
      LOGGER.debug("Namespace '{}' already exists", tableIdentifier.namespace());
    } catch (software.amazon.awssdk.services.glue.model.ConcurrentModificationException e) {
      LOGGER.debug("Namespace '{}' is being created concurrently, proceeding", tableIdentifier.namespace());
    }
  }

  public static Table createIcebergTable(Catalog icebergCatalog, TableIdentifier tableIdentifier, Schema schema) {
    ensureNamespace(icebergCatalog, tableIdentifier);
    return icebergCatalog.createTable(tableIdentifier, schema);
  }

  public static Table createIcebergTable(Catalog icebergCatalog, TableIdentifier tableIdentifier,
                                         Schema schema, String writeFormat) {
    return createIcebergTable(icebergCatalog, tableIdentifier, schema, writeFormat, Collections.emptyList());
  }

  public static Table createIcebergTable(Catalog icebergCatalog, TableIdentifier tableIdentifier,
                                         Schema schema, String writeFormat, List<Map<String, String>> partitionTransforms) {
    return createIcebergTable(icebergCatalog, tableIdentifier, schema, writeFormat, partitionTransforms, 2);
  }

  /**
   * Creates the table at {@code formatVersion}. Deletion vectors are a v3 construct, so
   * a table that will receive them has to be created as v3 up front; everything else
   * stays on v2, which is what every reader understands.
   */
  public static Table createIcebergTable(Catalog icebergCatalog, TableIdentifier tableIdentifier,
                                         Schema schema, String writeFormat, List<Map<String, String>> partitionTransforms,
                                         int formatVersion) {

    LOGGER.warn("Creating table:'{}'\nschema:{}\nrowIdentifier:{}", tableIdentifier, schema,
        schema.identifierFieldNames());

    ensureNamespace(icebergCatalog, tableIdentifier);

    // If we have partition transforms, create a PartitionSpec
    if (partitionTransforms.isEmpty()) {
      // No partitioning - create a table as before
      return icebergCatalog.buildTable(tableIdentifier, schema)
              .withProperty(FORMAT_VERSION, String.valueOf(formatVersion))
              .withProperty(DEFAULT_FILE_FORMAT, writeFormat.toLowerCase(Locale.ENGLISH))
              .withSortOrder(IcebergUtil.getIdentifierFieldsAsSortOrder(schema))
              .create();
    } else {
      // Create a table with partitioning
      LOGGER.info("Creating table with partitioning: {}", partitionTransforms);
      
      // Start building the table
      PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(schema);
      
      // Apply each partition transform in order
      for (Map<String, String> partitionDef : partitionTransforms) {
        String field = partitionDef.get("field");
        String transform = partitionDef.get("transform").toLowerCase(Locale.ENGLISH);
        
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
              .withProperty(FORMAT_VERSION, String.valueOf(formatVersion))
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

  /**
   * Raises an existing table to {@code formatVersion} when it sits below it. Iceberg
   * only moves format versions forward, so this is one-way: a table upgraded for
   * deletion vectors cannot be read by anything that speaks only the older version.
   */
  public static void ensureFormatVersion(Table table, int formatVersion) {
    int current = ((org.apache.iceberg.BaseTable) table).operations().current().formatVersion();
    if (current >= formatVersion) {
      return;
    }
    LOGGER.warn("Upgrading {} from format version {} to {}; this cannot be undone",
        table.name(), current, formatVersion);
    table.updateProperties().set(FORMAT_VERSION, String.valueOf(formatVersion)).commit();
    table.refresh();
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
          .setAll(icebergTable.properties())
          .set("write.metadata.metrics.column.file_path", "full");
    } else {
      return new GenericAppenderFactory(
          icebergTable.schema(),
          icebergTable.spec(),
          Ints.toArray(identifierFieldIds),
          TypeUtil.select(icebergTable.schema(), Sets.newHashSet(identifierFieldIds)),
          null)
          .setAll(icebergTable.properties())
          .set("write.metadata.metrics.column.file_path", "full");
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

  public static boolean dropIcebergTable(String namespace, String tableName, Catalog icebergCatalog) {
    try{
      TableIdentifier tableID = TableIdentifier.of(namespace, tableName);
      // Check if table exists
      if (!icebergCatalog.tableExists(tableID)) {
        LOGGER.warn("Table not found: {}", tableID.toString());
        return false;
      }
      return icebergCatalog.dropTable(tableID, false);
    } catch(Exception e){
      LOGGER.error("Failed to drop table {}.{}: {}", namespace, tableName, e.getMessage());
      throw new RuntimeException("Failed to drop table: " + namespace + "." + tableName, e);
    }
  }

}
