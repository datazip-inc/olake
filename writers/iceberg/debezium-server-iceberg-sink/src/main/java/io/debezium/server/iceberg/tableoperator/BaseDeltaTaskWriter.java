package io.debezium.server.iceberg.tableoperator;

import com.google.common.collect.Sets;
import io.debezium.server.iceberg.typeconversion.TypeConverter;
import io.debezium.server.iceberg.typeconversion.TypeConversionException;
import org.apache.iceberg.*;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
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
    
    // Perform type conversion if needed before writing
    Record convertedRow = convertTypes(row);
    
    if (rowOperation == Operation.INSERT) {
      // new row
      writer.write(convertedRow);
    } else if (rowOperation == Operation.DELETE && !keepDeletes) {
      // deletes. doing hard delete. when keepDeletes = FALSE we dont keep deleted record
      writer.deleteKey(keyProjection.wrap(convertedRow));
    } else {
      writer.deleteKey(keyProjection.wrap(convertedRow));
      writer.write(convertedRow);
    }
  }

  /**
   * Converts field types in the record if needed based on schema requirements
   */
  private Record convertTypes(Record row) {
    // Create a copy of the record to modify
    Record convertedRow = row.copy();
    
    // Iterate through all fields and check if type conversion is needed
    for (Types.NestedField field : schema.columns()) {
      String fieldName = field.name();
      Type targetType = field.type();
      
      try {
        Object value = convertedRow.getField(fieldName);
        if (value != null) {
          // Infer source type from the value's Java class
          Type sourceType = inferSourceType(value);
          if (!sourceType.equals(targetType)) {
            Object convertedValue = TypeConverter.convert(value, sourceType, targetType);
            convertedRow.setField(fieldName, convertedValue);
          }
        }
      } catch (TypeConversionException e) {
        throw new RuntimeException("Failed to convert field " + fieldName + ": " + e.getMessage(), e);
      }
    }
    return convertedRow;
  }

  /**
   * Infers the Iceberg Type from a Java object's class
   */
  private Type inferSourceType(Object value) {
    if (value instanceof Integer) {
      return Types.IntegerType.get();
    } else if (value instanceof Long) {
      return Types.LongType.get();
    } else if (value instanceof Double) {
      return Types.DoubleType.get();
    } else if (value instanceof Float) {
      return Types.FloatType.get();
    } else if (value instanceof Boolean) {
      return Types.BooleanType.get();
    } else if (value instanceof String) {
      return Types.StringType.get();
    } else {
      throw new TypeConversionException("Unsupported value type: " + value.getClass().getName());
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



