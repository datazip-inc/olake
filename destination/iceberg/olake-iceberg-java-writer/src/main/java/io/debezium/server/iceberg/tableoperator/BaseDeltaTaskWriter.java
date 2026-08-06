package io.debezium.server.iceberg.tableoperator;

import com.google.common.collect.Sets;
import org.apache.iceberg.*;
import org.apache.iceberg.deletes.DeleteGranularity;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.types.TypeUtil;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Set;

abstract class BaseDeltaTaskWriter extends BaseTaskWriter<Record> implements PositionTrackableWriter {

  private final Schema schema;
  private final Schema deleteSchema;
  private final InternalRecordWrapper wrapper;
  private final InternalRecordWrapper keyWrapper;
  private final boolean keepDeletes;
  private final boolean usePositionalDeletes;
  private final RecordProjection keyProjection;

  BaseDeltaTaskWriter(PartitionSpec spec,
                      FileFormat format,
                      FileAppenderFactory<Record> appenderFactory,
                      OutputFileFactory fileFactory,
                      FileIO io,
                      long targetFileSize,
                      Schema schema,
                      Set<Integer> identifierFieldIds,
                      boolean keepDeletes,
                      boolean usePositionalDeletes) {
    super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
    this.schema = schema;
    this.deleteSchema = TypeUtil.select(schema, Sets.newHashSet(identifierFieldIds));
    this.wrapper = new InternalRecordWrapper(schema.asStruct());
    this.keyWrapper = new InternalRecordWrapper(deleteSchema.asStruct());
    this.keyProjection = RecordProjection.create(schema, deleteSchema);
    this.keepDeletes = keepDeletes;
    this.usePositionalDeletes = usePositionalDeletes;
  }

  abstract RowDataDeltaWriter route(Record row);

  public CharSequence currentPath(Record record) {
      return route(record).currentPath();
  }

  public long currentRows(Record record) {
      return route(record).currentRows();
  }

  InternalRecordWrapper wrapper() {
    return wrapper;
  }

  @Override
  public void write(Record row) throws IOException {
    RowDataDeltaWriter writer = route(row);
    RecordWrapper wrapped = (RecordWrapper) row;
    Operation rowOperation = wrapped.op();

    if (usePositionalDeletes) {
      if (wrapped.hasPositionalDelete()) {
        writer.deletePosition(wrapped.deleteFilePath(), wrapped.deletePosition());
      }

      if (rowOperation == Operation.DELETE && !keepDeletes) {
        // Hard delete: positional delete only, no tombstone row.
        return;
      }

      // Iceberg's write() already pos-deletes a prior insert of the same
      // equality key within this writer session (insertedRowMap).
      writer.write(wrapped);
      return;
    }

    if (rowOperation == Operation.DELETE && !keepDeletes) {
      // deletes. doing hard delete. when keepDeletes = FALSE we dont keep deleted record
      writer.deleteKey(keyProjection.wrap(row));
    } else if (rowOperation == Operation.CREATE) {
      // Steady-state CDC insert: no prior committed row exists for this key, skip equality delete.
      writer.write(row);
    } else {
      // Phantom read possible: equality-delete before write to evict any prior committed version.
      // _op_type normalisation ("i" -> "c") is done upstream in IcebergTableOperator
      // for all writer types before reaching here.
      writer.deleteKey(keyProjection.wrap(row));
      writer.write(row);
    }
  }

  public class RowDataDeltaWriter extends BaseEqualityDeltaWriter {
    private final RollingFileWriter cachedDataWriter;
    private final FileWriter<PositionDelete<Record>, ?> posDeleteWriter;
    private final PositionDelete<Record> positionDelete = PositionDelete.create();

    @SuppressWarnings("unchecked")
    RowDataDeltaWriter(PartitionKey partition) {
      // create one positional delete file per referenced data file,
      super(partition, schema, deleteSchema, DeleteGranularity.FILE);

      try {
        Field dataField = BaseEqualityDeltaWriter.class.getDeclaredField("dataWriter");
        dataField.setAccessible(true);
        cachedDataWriter = (RollingFileWriter) dataField.get(this);

        // Iceberg 1.10 keeps writePosDelete private; write through the same
        // pos-delete writer the equality path uses so files join the close/commit.
        Field posField = BaseEqualityDeltaWriter.class.getDeclaredField("posDeleteWriter");
        posField.setAccessible(true);
        posDeleteWriter = (FileWriter<PositionDelete<Record>, ?>) posField.get(this);
      } catch (Exception e) {
        throw new RuntimeException("Failed to access underlying equality delta writer fields", e);
      }
    }

    public CharSequence currentPath() {
        return cachedDataWriter.currentPath();
    }

    public long currentRows() {
        return cachedDataWriter.currentRows();
    }

    /** Emit a positional delete for a previously committed (or indexed) row. */
    void deletePosition(String filePath, long position) throws IOException {
      posDeleteWriter.write(positionDelete.set(filePath, position, null));
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
