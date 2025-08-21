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
import java.util.HashMap;
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







  public static Table createIcebergTable(Catalog icebergCatalog, TableIdentifier tableIdentifier, Schema schema) {

    if (!((SupportsNamespaces) icebergCatalog).namespaceExists(tableIdentifier.namespace())) {
      ((SupportsNamespaces) icebergCatalog).createNamespace(tableIdentifier.namespace());
      LOGGER.warn("Created namespace:'{}'", tableIdentifier.namespace());
    }
    return icebergCatalog.createTable(tableIdentifier, schema);
  }

  public static Table createIcebergTable(Catalog icebergCatalog, TableIdentifier tableIdentifier,
                                         Schema schema, String writeFormat) {
    return createIcebergTable(icebergCatalog, tableIdentifier, schema, writeFormat, Collections.emptyList());
  }

  public static Table createIcebergTable(Catalog icebergCatalog, TableIdentifier tableIdentifier,
                                         Schema schema, String writeFormat, List<Map<String, String>> partitionTransforms) {

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
