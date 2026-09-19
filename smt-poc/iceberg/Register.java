// Registers an existing Parquet file (written by arrow-go with a VARIANT column)
// into an Iceberg v3 table through the REST catalog — the same "register a file
// we already wrote" path OLake's arrow writer uses (REGISTER_AND_COMMIT).
import java.util.Map;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.rest.RESTCatalog;

public class Register {
  public static void main(String[] a) throws Exception {
    String path = a[0];
    RESTCatalog cat = new RESTCatalog();
    cat.initialize("rest", Map.of(
        "uri", System.getenv().getOrDefault("CATALOG_URI", "http://localhost:8181/catalog"),
        "warehouse", "warehouse",
        "io-impl", "org.apache.iceberg.aws.s3.S3FileIO",
        "s3.endpoint", System.getenv().getOrDefault("S3_ENDPOINT", "http://localhost:9000"),
        "s3.path-style-access", "true",
        "s3.access-key-id", "admin",
        "s3.secret-access-key", "password",
        "client.region", "us-east-1"));
    Table t = cat.loadTable(TableIdentifier.of("demo", System.getenv().getOrDefault("TABLE", "arrow_variant")));
    System.out.println("table schema: " + t.schema());
    System.out.println("format-version: " + ((BaseTable) t).operations().current().formatVersion());

    InputFile in = t.io().newInputFile(path);
    Metrics m = null;
    try {
      m = ParquetUtil.fileMetrics(in, MetricsConfig.forTable(t));
      System.out.println("metrics ok: rows=" + m.recordCount() + " cols-with-stats=" + m.lowerBounds().keySet());
    } catch (Exception e) {
      System.out.println("metrics FAILED (" + e + ") — registering with record count only");
    }
    DataFiles.Builder b = DataFiles.builder(t.spec()).withPath(path).withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(in.getLength());
    if (m != null) b.withMetrics(m); else b.withRecordCount(Long.parseLong(a[1]));
    t.newAppend().appendFile(b.build()).commit();
    System.out.println("committed snapshot " + t.currentSnapshot().snapshotId());
    cat.close();
  }
}
