/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.olake.server.iceberg.util;

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
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
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
  private static final Schema CONNECTION_CHECK_SCHEMA = new Schema(
      Types.NestedField.optional(1, "_olake_check", Types.StringType.get()));

  /** Creates a temp table to test catalog and storage permissions, then drops it. */
  public static void checkCatalogConnection(Catalog catalog, String namespace, String tableName) {
    TableIdentifier tableId = TableIdentifier.of(namespace, tableName);
    try {
      // we should remove and create table again on each connect
      if (catalog.tableExists(tableId)) {
        LOGGER.warn("Removing leftover connection-check table {}", tableId);
        catalog.dropTable(tableId, true);
      }

      // Creating a table inherently writes metadata.json to storage and updates the catalog
      createIcebergTable(catalog, tableId, CONNECTION_CHECK_SCHEMA, Collections.emptyList());
      LOGGER.info("Connection check table creation succeeded for {}", tableId);
    } finally {
      if (catalog.tableExists(tableId)) {
        catalog.dropTable(tableId, true);
        LOGGER.info("Connection check cleanup dropped {}", tableId);
      }
    }
  }

  public static Table loadOrCreateIcebergTable(Catalog catalog, TableIdentifier tableId, Schema schema,
                                               List<Map<String, String>> partitionTransforms) {
    return loadIcebergTable(catalog, tableId)
        .orElseGet(() -> createIcebergTable(catalog, tableId, schema, partitionTransforms));
  }

  public static Table createIcebergTable(Catalog catalog, TableIdentifier tableId, Schema schema,
                                         List<Map<String, String>> partitionTransforms) {
    LOGGER.warn("Creating table:'{}' schema:{} identifierFields:{}", tableId, schema, schema.identifierFieldNames());
    ensureNamespace(catalog, tableId);

    Catalog.TableBuilder tableBuilder = catalog.buildTable(tableId, schema)
        .withProperty(FORMAT_VERSION, "2")
        .withProperty(DEFAULT_FILE_FORMAT, "parquet")
        .withSortOrder(getIdentifierFieldsAsSortOrder(schema));

    if (!partitionTransforms.isEmpty()) {
      LOGGER.info("Creating table with partitioning: {}", partitionTransforms);
      tableBuilder.withPartitionSpec(buildPartitionSpec(schema, partitionTransforms));
    }

    try {
      return tableBuilder.create();
    } catch (AlreadyExistsException e) {
      LOGGER.warn("Table {} already exists: {}", tableId, e.getMessage());
      return catalog.loadTable(tableId);
    }
  }

  /** Creates the namespace if it does not already exist. */
  private static void ensureNamespace(Catalog catalog, TableIdentifier tableId) {
    if (((SupportsNamespaces) catalog).namespaceExists(tableId.namespace())) {
      return;
    }
    try {
      ((SupportsNamespaces) catalog).createNamespace(tableId.namespace());
      LOGGER.warn("Created namespace:'{}'", tableId.namespace());
    } catch (AlreadyExistsException e) {
      LOGGER.warn("Namespace '{}' already exists: {}", tableId.namespace(), e.getMessage());
    }// TODO: add exception error of glue
  }

  private static PartitionSpec buildPartitionSpec(Schema schema, List<Map<String, String>> partitionTransforms) {
    PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(schema);
    for (Map<String, String> partitionDef : partitionTransforms) {
      applyPartitionTransform(specBuilder, partitionDef.get("field"), partitionDef.get("transform"));
    }
    return specBuilder.build();
  }

  private static void applyPartitionTransform(PartitionSpec.Builder specBuilder, String field, String transform) {
    String normalized = transform.toLowerCase(Locale.ENGLISH);
    switch (normalized) {
      case "identity" -> specBuilder.identity(field);
      case "year" -> specBuilder.year(field);
      case "month" -> specBuilder.month(field);
      case "day" -> specBuilder.day(field);
      case "hour" -> specBuilder.hour(field);
      default -> {
        if (normalized.startsWith("bucket[") && normalized.endsWith("]")) {
          try {
            int numBuckets = Integer.parseInt(normalized.substring(7, normalized.length() - 1));
            specBuilder.bucket(field, numBuckets);
          } catch (NumberFormatException e) {
            LOGGER.warn("Invalid bucket transform: {}. Using identity transform instead.", transform);
            specBuilder.identity(field);
          }
        } else if (normalized.startsWith("truncate[") && normalized.endsWith("]")) {
          try {
            int width = Integer.parseInt(normalized.substring(9, normalized.length() - 1));
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
