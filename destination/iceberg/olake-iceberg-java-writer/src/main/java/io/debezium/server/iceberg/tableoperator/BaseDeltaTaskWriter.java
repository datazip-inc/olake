package io.debezium.server.iceberg.tableoperator;

import com.google.common.collect.Sets;
import org.apache.iceberg.*;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.DeleteGranularity;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.deletes.SortingPositionOnlyDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.*;
import org.apache.iceberg.types.TypeUtil;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

abstract class BaseDeltaTaskWriter extends BaseTaskWriter<Record> implements PositionTrackableWriter {

  private final Schema schema;
  private final Schema deleteSchema;
  private final InternalRecordWrapper wrapper;
  private final InternalRecordWrapper keyWrapper;
  private final boolean keepDeletes;
  private final boolean usePositionalDeletes;
  private final RecordProjection keyProjection;

  private final FileAppenderFactory<Record> appenderFactory;
  private final OutputFileFactory fileFactory;
  private final FileIO io;
  private final long targetFileSize;
  private final FileWriterFactory<Record> fileWriterFactory;
  private final List<DeleteFile> extraDeleteFiles = new ArrayList<>();
  private final Set<CharSequence> extraReferencedDataFiles = new HashSet<>();

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
    this.appenderFactory = appenderFactory;
    this.fileFactory = fileFactory;
    this.io = io;
    this.targetFileSize = targetFileSize;

    this.fileWriterFactory = new FileWriterFactory<>() {
      @Override
      public DataWriter<Record> newDataWriter(EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {
        return appenderFactory.newDataWriter(file, format, partition);
      }

      @Override
      public EqualityDeleteWriter<Record> newEqualityDeleteWriter(EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {
        return appenderFactory.newEqDeleteWriter(file, format, partition);
      }

      @Override
      public PositionDeleteWriter<Record> newPositionDeleteWriter(EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {
        return appenderFactory.newPosDeleteWriter(file, format, partition);
      }
    };
  }

  EncryptedOutputFile newOutputFile(StructLike partition) {
    if (spec().isUnpartitioned() || partition == null) {
      return fileFactory.newOutputFile();
    }
    return fileFactory.newOutputFile(spec(), partition);
  }

  synchronized void addExtraDeleteResult(DeleteWriteResult result) {
    if (result != null) {
      if (result.deleteFiles() != null && !result.deleteFiles().isEmpty()) {
        extraDeleteFiles.addAll(result.deleteFiles());
      }
      if (result.referencedDataFiles() != null && !result.referencedDataFiles().isEmpty()) {
        extraReferencedDataFiles.addAll(result.referencedDataFiles());
      }
    }
  }

  @Override
  public WriteResult complete() throws IOException {
    WriteResult result = super.complete();
    if (!extraDeleteFiles.isEmpty()) {
      return WriteResult.builder()
          .add(result)
          .addDeleteFiles(extraDeleteFiles)
          .addReferencedDataFiles(extraReferencedDataFiles)
          .build();
    }
    return result;
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

  public class RowDataDeltaWriter implements Closeable {
    private final RollingFileWriter dataWriter;
    private final BaseEqualityDeltaWriter eqDeltaWriter;
    private final FileWriter<PositionDelete<Record>, DeleteWriteResult> posDeleteWriter;
    private final PositionDelete<Record> positionDelete = PositionDelete.create();

    RowDataDeltaWriter(PartitionKey partition) {
      if (usePositionalDeletes) {
        this.dataWriter = new RollingFileWriter(partition);
        this.eqDeltaWriter = null;
        this.posDeleteWriter = new SortingPositionOnlyDeleteWriter<>(
            () -> new RollingPositionDeleteWriter<>(
                fileWriterFactory,
                fileFactory,
                io,
                targetFileSize,
                spec(),
                partition
            ),
            DeleteGranularity.PARTITION
        );
      } else {
        this.dataWriter = null;
        this.posDeleteWriter = null;
        this.eqDeltaWriter = new BaseEqualityDeltaWriter(partition, schema, deleteSchema, DeleteGranularity.FILE) {
          @Override
          protected StructLike asStructLike(Record data) {
            return wrapper.wrap(data);
          }

          @Override
          protected StructLike asStructLikeKey(Record data) {
            return keyWrapper.wrap(data);
          }
        };
      }
    }

    public CharSequence currentPath() {
      return dataWriter != null ? dataWriter.currentPath() : null;
    }

    public long currentRows() {
      return dataWriter != null ? dataWriter.currentRows() : 0L;
    }

    public void write(Record record) throws IOException {
      if (usePositionalDeletes) {
        dataWriter.write(record);
      } else {
        eqDeltaWriter.write(record);
      }
    }

    public void deleteKey(Record key) throws IOException {
      if (eqDeltaWriter != null) {
        eqDeltaWriter.deleteKey(key);
      }
    }

    public void deletePosition(String filePath, long position) throws IOException {
      if (posDeleteWriter != null) {
        posDeleteWriter.write(positionDelete.set(filePath, position, null));
      }
    }

    private boolean closed = false;

    @Override
    public void close() throws IOException {
      if (closed) {
        return;
      }
      closed = true;
      if (usePositionalDeletes) {
        if (dataWriter != null) {
          dataWriter.close();
        }
        if (posDeleteWriter != null) {
          posDeleteWriter.close();
          DeleteWriteResult result = posDeleteWriter.result();
          addExtraDeleteResult(result);
        }
      } else if (eqDeltaWriter != null) {
        eqDeltaWriter.close();
      }
    }
  }
}
