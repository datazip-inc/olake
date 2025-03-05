package io.debezium.server.iceberg.history;

import io.debezium.config.Configuration;
import io.debezium.server.iceberg.storage.BaseIcebergStorageConfig;


public class IcebergSchemaHistoryConfig extends BaseIcebergStorageConfig {
  public IcebergSchemaHistoryConfig(Configuration config, String configuration_field_prefix) {
    super(config, configuration_field_prefix);
  }

  @Override
  public String tableName() {
    return this.config.getProperty("table-name", "debezium_database_history_storage");
  }
  public String getMigrateHistoryFile() {
    return config.getProperty("migrate-history-file", "");
  }
}
