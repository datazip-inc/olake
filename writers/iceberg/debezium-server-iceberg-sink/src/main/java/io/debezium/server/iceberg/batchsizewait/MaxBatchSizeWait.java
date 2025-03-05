/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.batchsizewait;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.server.DebeziumMetrics;

import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Optimizes batch size around 85%-90% of max,batch.size using dynamically calculated sleep(ms)
 *
 * @author Ismail Simsek
 */
@Dependent
@Named("MaxBatchSizeWait")
public class MaxBatchSizeWait implements InterfaceBatchSizeWait {
  protected static final Logger LOGGER = LoggerFactory.getLogger(MaxBatchSizeWait.class);

  @ConfigProperty(name = "debezium.source.max.queue.size", defaultValue = CommonConnectorConfig.DEFAULT_MAX_QUEUE_SIZE + "")
  int maxQueueSize;
  @ConfigProperty(name = "debezium.source.max.batch.size", defaultValue = CommonConnectorConfig.DEFAULT_MAX_BATCH_SIZE + "")
  int maxBatchSize;
  @ConfigProperty(name = "debezium.sink.batch.batch-size-wait.max-wait-ms", defaultValue = "300000")
  int maxWaitMs;
  @ConfigProperty(name = "debezium.sink.batch.batch-size-wait.wait-interval-ms", defaultValue = "10000")
  int waitIntervalMs;

  @Inject
  DebeziumMetrics dbzMetrics;

  @Override
  public void initizalize() throws DebeziumException {
    assert waitIntervalMs < maxWaitMs : "`wait-interval-ms` cannot be bigger than `max-wait-ms`";
  }

  @Override
  public void waitMs(Integer numRecordsProcessed, Integer processingTimeMs) throws InterruptedException {

    // don't wait if snapshot process is running
    if (dbzMetrics.snapshotRunning()) {
      return;
    }

    LOGGER.debug("Processed {}, QueueCurrentSize:{}, QueueTotalCapacity:{}, SecondsBehindSource:{}, SnapshotCompleted:{}",
        numRecordsProcessed,
        dbzMetrics.streamingQueueCurrentSize(),
        maxQueueSize,
        (int) (dbzMetrics.streamingMilliSecondsBehindSource() / 1000),
        dbzMetrics.snapshotCompleted()
    );

    int totalWaitMs = 0;
    while (totalWaitMs < maxWaitMs && dbzMetrics.streamingQueueCurrentSize() < maxBatchSize) {
      totalWaitMs += waitIntervalMs;
      LOGGER.debug("Sleeping {} Milliseconds, QueueCurrentSize:{} < maxBatchSize:{}",
          waitIntervalMs, dbzMetrics.streamingQueueCurrentSize(), maxBatchSize);

      Thread.sleep(waitIntervalMs);
    }

    LOGGER.debug("Total wait {} Milliseconds, QueueCurrentSize:{} < maxBatchSize:{}",
        totalWaitMs, dbzMetrics.streamingQueueCurrentSize(), maxBatchSize);

  }

}
