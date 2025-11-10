package io.debezium.server.iceberg;

import java.util.Map;

import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;

import  io.debezium.server.iceberg.rpc.RecordIngest;
import io.debezium.server.iceberg.rpc.RecordIngest.IcebergPayload.IceRecord;
import io.debezium.server.iceberg.rpc.RecordIngest.IcebergPayload.IceRecord.FieldValue;
import io.debezium.server.iceberg.tableoperator.Operation;
import io.debezium.server.iceberg.tableoperator.RecordWrapper;

public class SchemaConvertor {
  private final List<RecordIngest.IcebergPayload.SchemaField> schemaMetadata;
  private final String identifierField;
  protected static final Logger LOGGER = LoggerFactory.getLogger(SchemaConvertor.class);

  public static final List<String> TS_MS_FIELDS = List.of("_olake_timestamp", "_cdc_timestamp");

  public SchemaConvertor(String pk, List<RecordIngest.IcebergPayload.SchemaField> schema) {
    schemaMetadata = schema;
    identifierField = pk;
  }

  // currently implemented for primitive fields only
  public Schema convertToIcebergSchema() {
    RecordSchemaData schemaData = new RecordSchemaData();
    for(RecordIngest.IcebergPayload.SchemaField rawField :  schemaMetadata){
      String fieldName = rawField.getKey(); // field name 
      String fieldType = rawField.getIceType();
      Boolean isPkField = (fieldName.equals(identifierField));
      final Types.NestedField field = Types.NestedField.of(schemaData.nextFieldId().getAndIncrement(), !isPkField, fieldName, icebergPrimitiveField(fieldName, fieldType));
      schemaData.fields().add(field);
      if (isPkField) schemaData.identifierFieldIds().add(field.fieldId());
    }
    return new Schema(schemaData.fields(), schemaData.identifierFieldIds());
  }

  public List<RecordWrapper> convert(Boolean upsert, Schema tableSchema, List<IceRecord> records) {
      // Pre-compute schema information once
      Map<String, Integer> fieldNameToIndexMap = new HashMap<>();
      for (int index = 0; index < schemaMetadata.size(); index++) {
          RecordIngest.IcebergPayload.SchemaField rawField = schemaMetadata.get(index);
          String fieldName = rawField.getKey(); // or whatever method gives the field name
          fieldNameToIndexMap.put(fieldName, index); // Map field name → index
      }
      
      return records.parallelStream().map(data -> convertRecord(upsert, fieldNameToIndexMap, data, tableSchema.asStruct())).toList();
  }

  private RecordWrapper convertRecord(Boolean upsert, Map<String, Integer> fieldNameToIndexMap, IceRecord data, StructType tableFields) {
      // Create record using first field's struct (optimization)
      GenericRecord genericRow = GenericRecord.create(tableFields);
      List<Types.NestedField> fields = tableFields.fields();
     
      // Process all fields 
      for (Types.NestedField field : fields) {
          String fieldName = field.name();
          // Get field value - single map lookup
          Integer idx = fieldNameToIndexMap.get(fieldName);
          if (idx == null) {
            genericRow.setField(fieldName, null);
            continue;
          }
          FieldValue fieldValue = data.getFields(idx);
          if (fieldValue == null) {
              genericRow.setField(fieldName, null);
              continue;
          }
          try {
              Object convertedValue = fieldValuetoIceType(field, fieldValue);
              genericRow.setField(fieldName, convertedValue);
          } catch (RuntimeException e) {
              throw new RuntimeException("Failed to parse JSON string for field "+ fieldName +" value "+ fieldValue + " exceptipn: " + e);
          }
      }
      // check if it is append only or upsert
      if(!upsert) { 
        // TODO: need a discussion previously Operation.Insert was being used
        return new RecordWrapper(genericRow, Operation.READ);
      }
      return new RecordWrapper(genericRow, cdcOpValue(data.getRecordType()));
  }

  private static Object fieldValuetoIceType(Types.NestedField field, FieldValue value) {
      LOGGER.debug("Processing Field:{} Type:{} RawValue:{}", field.name(), field.type(), value);
      if (value == null) {
          return null;
      }
      // NOTE: for conversion related json code (How we do previously for UUID, Map, Struct etc.) check: olake code on or before version v0.1.11
      try {
          switch (field.type().typeId()) {
              case INTEGER:
                  if (value.hasIntValue())  return value.getIntValue();
                  return null;
              case LONG:
                  if (value.hasLongValue())  return value.getLongValue();
                  return null;
              case FLOAT:
                  if (value.hasFloatValue()) return value.getFloatValue();
                  return null;
              case DOUBLE:
                  if (value.hasDoubleValue()) return value.getDoubleValue();
                  return null;
              case BOOLEAN:
                  if (value.hasBoolValue()) return value.getBoolValue();
                  return null;
              case STRING,UUID:
                  if (value.hasStringValue()) return value.getStringValue();
                  return null;
              case TIMESTAMP:
                  if (value.hasLongValue()) return OffsetDateTime.ofInstant(Instant.ofEpochMilli(value.getLongValue()), ZoneOffset.UTC);
                  return null;
              default:
                  return value;
          }
      } catch (Exception e) {
          throw new RuntimeException("Failed to parse value for field " + field.name() +
                                    " as type " + field.type() +
                                    ", raw value: " + value, e);
      }
  }

  public Operation cdcOpValue(String cdcOpField) {
    return switch (cdcOpField) {
      case "u" -> Operation.UPDATE;
      case "d" -> Operation.DELETE;
      case "r" -> Operation.READ;
      case "c" -> Operation.INSERT;
      case "i" -> Operation.INSERT;
      default ->
          throw new RuntimeException("Unexpected `" + cdcOpField + "` operation value received, expecting one of ['u','d','r','c', 'i']");
    };
  }

  private static Type.PrimitiveType icebergPrimitiveField(String fieldName, String fieldType) {
    switch (fieldType) {
      case "int": // int 4 bytes
        return Types.IntegerType.get();
      case "long": // long 8 bytes
        if (TS_MS_FIELDS.contains(fieldName)) {
          return Types.TimestampType.withZone();
        } else {
          return Types.LongType.get();
        }
      case "float": // float is represented in 32 bits,
        return Types.FloatType.get();
      case "double": // double is represented in 64 bits
        return Types.DoubleType.get();
      case "boolean":
        return Types.BooleanType.get();
      case "string":
        return Types.StringType.get();
      case "uuid":
        return Types.UUIDType.get();
      case "binary":
        return Types.BinaryType.get();
      case "timestamp":
          return Types.TimestampType.withoutZone();
      case "timestamptz":
          return Types.TimestampType.withZone();
      default:
        // default to String type
        return Types.StringType.get();
    }
  }
}
