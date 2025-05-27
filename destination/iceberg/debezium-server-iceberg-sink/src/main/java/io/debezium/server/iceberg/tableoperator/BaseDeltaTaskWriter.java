package io.debezium.server.iceberg.tableoperator;

import com.google.common.collect.Sets;
import org.apache.iceberg.*;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.types.TypeUtil;

import java.io.IOException;
import java.util.Set;

abstract class BaseDeltaTaskWriter extends BaseTaskWriter<Record> {

  private final Schema schema;
  private final Schema deleteSchema;
  private final InternalRecordWrapper wrapper;
  private final InternalRecordWrapper keyWrapper;
  private final boolean keepDeletes;
  private final RecordProjection keyProjection;

  BaseDeltaTaskWriter(PartitionSpec spec,
                      FileFormat format,
                      FileAppenderFactory<Record> appenderFactory,
                      OutputFileFactory fileFactory,
                      FileIO io,
                      long targetFileSize,
                      Schema schema,
                      Set<Integer> identifierFieldIds,
                      boolean keepDeletes) {
    super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
    this.schema = schema;
    this.deleteSchema = TypeUtil.select(schema, Sets.newHashSet(identifierFieldIds));
    this.wrapper = new InternalRecordWrapper(schema.asStruct());
    this.keyWrapper = new InternalRecordWrapper(deleteSchema.asStruct());
    this.keyProjection = RecordProjection.create(schema, deleteSchema);
    this.keepDeletes = keepDeletes;
  }

  abstract RowDataDeltaWriter route(Record row);

  InternalRecordWrapper wrapper() {
    return wrapper;
  }

  @Override/**/
  public void write(Record row) throws IOException {
    RowDataDeltaWriter writer = route(row);
    Operation rowOperation = ((RecordWrapper) row).op();
    if (rowOperation == Operation.INSERT) {
      // new row
      writer.write(row);
    } else if (rowOperation == Operation.DELETE && !keepDeletes) {
      // deletes. doing hard delete. when keepDeletes = FALSE we dont keep deleted record
      writer.deleteKey(keyProjection.wrap(row));
    } else {
      writer.deleteKey(keyProjection.wrap(row));
      writer.write(row);
    }
  }

  public class RowDataDeltaWriter extends BaseEqualityDeltaWriter {
    RowDataDeltaWriter(PartitionKey partition) {
      super(partition, schema, deleteSchema);
    }

    @Override
    protected StructLike asStructLike(Record data) {
      return wrapper.wrap(data);
    }

    @Override
    protected StructLike asStructLikeKey(Record data) {
      return keyWrapper.wrap(data);
    }
  }
}
