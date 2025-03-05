package io.debezium.server.iceberg.storage;

import com.google.common.collect.Maps;
import io.debezium.config.Configuration;
import io.debezium.server.iceberg.IcebergUtil;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.eclipse.microprofile.config.ConfigProvider;

import java.util.Map;
import java.util.Properties;


public abstract class BaseIcebergStorageConfig {
  private static final String PROP_SINK_PREFIX = "debezium.sink.";
  public Properties config = new Properties();

  public BaseIcebergStorageConfig(Configuration config, String configuration_field_prefix) {
    Configuration confIcebergSubset = config.subset(configuration_field_prefix + "iceberg.", true);
    confIcebergSubset.forEach(this.config::put);

    // debezium is doing config filtering before passing it down to this class!
    // so we are taking additional config using ConfigProvider with this we take full iceberg config
    Map<String, String> icebergConf = IcebergUtil.getConfigSubset(ConfigProvider.getConfig(), PROP_SINK_PREFIX + "iceberg.");
    icebergConf.forEach(this.config::putIfAbsent);
  }

  public String catalogName() {
    return this.config.getProperty("catalog-name", "default");
  }

  public String tableNamespace() {
    return this.config.getProperty("table-namespace", "default");
  }

  abstract public String tableName();

  public org.apache.hadoop.conf.Configuration hadoopConfig() {
    final org.apache.hadoop.conf.Configuration hadoopConfig = new org.apache.hadoop.conf.Configuration();
    config.forEach((key, value) -> hadoopConfig.set((String) key, (String) value));
    return hadoopConfig;
  }

  public Map<String, String> icebergProperties() {
    return Maps.fromProperties(config);
  }

  public Catalog icebergCatalog() {
    return CatalogUtil.buildIcebergCatalog(this.catalogName(),
        this.icebergProperties(), this.hadoopConfig());
  }

  public String tableFullName() {
    return String.format("%s.%s", this.tableNamespace(), this.tableName());
  }

  public TableIdentifier tableIdentifier() {
    return TableIdentifier.of(Namespace.of(this.tableNamespace()), this.tableName());
  }
}
