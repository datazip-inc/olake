/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.offset;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.server.iceberg.IcebergUtil;
import io.debezium.util.Strings;
import jakarta.enterprise.context.Dependent;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.*;
import org.apache.iceberg.types.Types;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.SafeObjectInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

/**
 * Implementation of OffsetBackingStore that saves data to Iceberg table.
 */
@Dependent
public class IcebergOffsetBackingStore extends MemoryOffsetBackingStore implements OffsetBackingStore {

  static final Schema OFFSET_STORAGE_TABLE_SCHEMA = new Schema(
      required(1, "id", Types.StringType.get()),
      optional(2, "offset_data", Types.StringType.get()),
      optional(3, "record_insert_ts", Types.TimestampType.withZone()
      )
  );
  protected static final ObjectMapper mapper = new ObjectMapper();
  public static String CONFIGURATION_FIELD_PREFIX_STRING = "offset.storage.";
  private static final Logger LOG = LoggerFactory.getLogger(IcebergOffsetBackingStore.class);
  protected Map<String, String> data = new HashMap<>();
  Catalog icebergCatalog;
  private String tableFullName;
  private TableIdentifier tableId;
  private Table offsetTable;
  IcebergOffsetBackingStoreConfig storageConfig;
  FileFormat format;
  GenericAppenderFactory appenderFactory;
  OutputFileFactory fileFactory;

  public IcebergOffsetBackingStore() {
  }

  @Override
  public void configure(WorkerConfig config) {
    super.configure(config);

    storageConfig = new IcebergOffsetBackingStoreConfig(Configuration.from(config.originalsStrings()), CONFIGURATION_FIELD_PREFIX_STRING);
    icebergCatalog = storageConfig.icebergCatalog();
    tableFullName = storageConfig.tableFullName();
    tableId = storageConfig.tableIdentifier();
  }

  @Override
  public synchronized void start() {
    super.start();
    LOG.info("Starting IcebergOffsetBackingStore table:{}", tableFullName);
    initializeTable();
    load();
  }

  @Override
  public synchronized void stop() {
    if (executor != null) {
      shutdownExecutorServiceQuietly(executor, 30, TimeUnit.SECONDS);
      executor = null;
    }
    LOG.info("Stopped IcebergOffsetBackingStore table:{}", tableFullName);
  }


  /**
   * Shuts down an executor service in two phases, first by calling shutdown to reject incoming tasks,
   * and then calling shutdownNow, if necessary, to cancel any lingering tasks.
   * After the timeout/on interrupt, the service is forcefully closed.
   * @param executorService The service to shut down.
   * @param timeout The timeout of the shutdown.
   * @param timeUnit The time unit of the shutdown timeout.
   */
  public static void shutdownExecutorServiceQuietly(ExecutorService executorService,
                                                    long timeout, TimeUnit timeUnit) {
    executorService.shutdown(); // Disable new tasks from being submitted
    try {
      // Wait a while for existing tasks to terminate
      if (!executorService.awaitTermination(timeout, timeUnit)) {
        executorService.shutdownNow(); // Cancel currently executing tasks
        // Wait a while for tasks to respond to being cancelled
        if (!executorService.awaitTermination(timeout, timeUnit)) {
          LOG.error("Executor {} did not terminate in time", executorService);
        }
      }
    } catch (InterruptedException e) {
      // (Re-)Cancel if current thread also interrupted
      executorService.shutdownNow();
      // Preserve interrupt status
      Thread.currentThread().interrupt();
    }
  }

  private void initializeTable() {
    if (icebergCatalog.tableExists(tableId)) {
      offsetTable = icebergCatalog.loadTable(tableId);
    } else {
      LOG.debug("Creating table {} to store offset", tableFullName);
      offsetTable = IcebergUtil.createIcebergTable(icebergCatalog, tableId, OFFSET_STORAGE_TABLE_SCHEMA);
      if (!icebergCatalog.tableExists(tableId)) {
        throw new DebeziumException("Failed to create table " + tableId + " to store offset");
      }

      if (!Strings.isNullOrEmpty(storageConfig.getMigrateOffsetFile().strip())) {
        LOG.warn("Migrating offset from file {}", storageConfig.getMigrateOffsetFile());
        this.loadFileOffset(new File(storageConfig.getMigrateOffsetFile()));
      }
    }

    format = IcebergUtil.getTableFileFormat(offsetTable);
    appenderFactory = IcebergUtil.getTableAppender(offsetTable);
    fileFactory = IcebergUtil.getTableOutputFileFactory(offsetTable, format);
  }

  private void loadFileOffset(File file) {
    try (SafeObjectInputStream is = new SafeObjectInputStream(Files.newInputStream(file.toPath()))) {
      Object obj = is.readObject();

      if (!(obj instanceof HashMap))
        throw new ConnectException("Expected HashMap but found " + obj.getClass());

      Map<byte[], byte[]> raw = (Map<byte[], byte[]>) obj;
      for (Map.Entry<byte[], byte[]> mapEntry : raw.entrySet()) {
        ByteBuffer key = (mapEntry.getKey() != null) ? ByteBuffer.wrap(mapEntry.getKey()) : null;
        ByteBuffer value = (mapEntry.getValue() != null) ? ByteBuffer.wrap(mapEntry.getValue()) : null;
        data.put(fromByteBuffer(key), fromByteBuffer(value));
      }
    } catch (IOException | ClassNotFoundException e) {
      throw new DebeziumException("Failed migrating offset from file", e);
    }

    LOG.warn("Loaded file offset, saving it to iceberg offset storage");
    save();
  }

  protected void save() {
    LOG.debug("Saving offset data to iceberg table...");
    try {
      String dataJson = mapper.writeValueAsString(data);
      LOG.debug("Saving offset data {}", dataJson);
      OffsetDateTime currentTs = OffsetDateTime.now(ZoneOffset.UTC);

      GenericRecord record = GenericRecord.create(OFFSET_STORAGE_TABLE_SCHEMA);
      Record row = record.copy(
          "id", UUID.randomUUID().toString(),
          "offset_data", dataJson,
          "record_insert_ts", currentTs);

      try (BaseTaskWriter<Record> writer = new UnpartitionedWriter<>(
          offsetTable.spec(), format, appenderFactory, fileFactory, offsetTable.io(), Long.MAX_VALUE)) {
        writer.write(row);
        writer.close();
        WriteResult files = writer.complete();

        Transaction t = offsetTable.newTransaction();
        t.newDelete().deleteFromRowFilter(Expressions.alwaysTrue()).commit();
        Arrays.stream(files.dataFiles()).forEach(f -> t.newAppend().appendFile(f).commit());
        t.commitTransaction();
        LOG.debug("Successfully saved offset data to iceberg table");
      }

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void load() {
    try {
      String dataJsonString = null;

      int rowNum = 0;
      try (CloseableIterable<Record> rs = IcebergGenerics.read(offsetTable)
          .build()) {
        for (Record row : rs) {
          dataJsonString = (String) row.getField("offset_data");
          rowNum++;
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      if (rowNum > 1) {
        throw new DebeziumException("Failed recover offset data from iceberg, Found multiple offset row!");
      }

      if (dataJsonString != null) {
        this.data = mapper.readValue(dataJsonString, new TypeReference<>() {
        });
        LOG.debug("Loaded offset data {}", dataJsonString);
      }
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      throw new DebeziumException("Failed recover offset data from iceberg", e);
    }
  }

  @Override
  public Future<Void> set(final Map<ByteBuffer, ByteBuffer> values,
                          final Callback<Void> callback) {
    return executor.submit(() -> {
      for (Map.Entry<ByteBuffer, ByteBuffer> entry : values.entrySet()) {
        if (entry.getKey() == null) {
          continue;
        }
        data.put(fromByteBuffer(entry.getKey()), fromByteBuffer(entry.getValue()));
      }
      save();
      if (callback != null) {
        callback.onCompletion(null, null);
      }
      return null;
    });
  }

  @Override
  public Future<Map<ByteBuffer, ByteBuffer>> get(final Collection<ByteBuffer> keys) {
    return executor.submit(() -> {
      Map<ByteBuffer, ByteBuffer> result = new HashMap<>();
      for (ByteBuffer key : keys) {
        result.put(key, toByteBuffer(data.get(fromByteBuffer(key))));
      }
      return result;
    });
  }

  public static String fromByteBuffer(ByteBuffer data) {
    return (data != null) ? String.valueOf(StandardCharsets.UTF_8.decode(data.asReadOnlyBuffer())) : null;
  }

  public static ByteBuffer toByteBuffer(String data) {
    return (data != null) ? ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8)) : null;
  }

  public Set<Map<String, Object>> connectorPartitions(String connectorName) {
    return null;
  }


}
