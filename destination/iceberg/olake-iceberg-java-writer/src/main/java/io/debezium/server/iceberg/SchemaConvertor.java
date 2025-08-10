package io.debezium.server.iceberg;

import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Value;
import  io.debezium.server.iceberg.rpc.RecordIngest;
import io.debezium.server.iceberg.rpc.RecordIngest.IcebergPayload.IceRecord;
import io.debezium.server.iceberg.tableoperator.Operation;
import io.debezium.server.iceberg.tableoperator.RecordWrapper;

public class SchemaConvertor {
  static List<RecordIngest.IcebergPayload.SchemaField> schemaMetadata;
  static String primaryKey;
  protected static final Logger LOGGER = LoggerFactory.getLogger(SchemaConvertor.class);

  public static final List<String> TS_MS_FIELDS = List.of("_olake_timestamp", "_cdc_timestamp");

  public SchemaConvertor(String pk, List<RecordIngest.IcebergPayload.SchemaField> schema) {
    schemaMetadata = schema;
    primaryKey = pk;
  }

  // currently implemented for primitive fields only
  public Schema convertToIcebergSchema() {
    RecordSchemaData schemaData = new RecordSchemaData();
    for(RecordIngest.IcebergPayload.SchemaField rawField :  schemaMetadata){
      String fieldName = rawField.getKey(); // field name 
      String fieldType = rawField.getIceType();
      Boolean isPkField = (fieldName.equals(primaryKey));
      final Types.NestedField field = Types.NestedField.of(schemaData.nextFieldId().getAndIncrement(), !isPkField, fieldName, icebergPrimitiveField(fieldName, fieldType));
      schemaData.fields().add(field);
      if (isPkField) schemaData.identifierFieldIds().add(field.fieldId());
    }
    return new Schema(schemaData.fields(), schemaData.identifierFieldIds());
  }

  public List<RecordWrapper> convert(Boolean upsert, Schema tableSchema, List<IceRecord> records) {
      // Pre-compute schema information once
      StructType tableFields = tableSchema.asStruct();
      Map<String, Integer> fieldNameToIndexMap = new HashMap<>();
      for (int index = 0; index < schemaMetadata.size(); index++) {
          RecordIngest.IcebergPayload.SchemaField rawField = schemaMetadata.get(index);
          String fieldName = rawField.getKey(); // or whatever method gives the field name
          fieldNameToIndexMap.put(fieldName, index); // Map field name → index
      }

      // Use parallel stream with optimal chunk sizing
      return records.stream()
          .map(data -> convertRecord(upsert,fieldNameToIndexMap, data, tableFields))
          .collect(Collectors.toList());
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
          Value fieldValue = data.getFields(idx);
          if (fieldValue == null || !fieldValue.hasStringValue() || fieldValue.getStringValue() == "") {
              genericRow.setField(fieldName, null);
              continue;
          }
          String stringValue = fieldValue.getStringValue();
          try {
              Object convertedValue = stringValToIcebergVal(field, stringValue);
              genericRow.setField(fieldName, convertedValue);
          } catch (RuntimeException e) {
              throw new RuntimeException("Failed to parse JSON string for field "+ fieldName +" value "+ stringValue + " exceptipn: " + e);
          }
      }
      // check if it is append only or upsert
      if(!upsert) { 
        // TODO: need a discussion previously Operation.Insert was being used
        return new RecordWrapper(genericRow, Operation.READ);
      }
      return new RecordWrapper(genericRow, cdcOpValue(data.getRecordType()));
  }

  private static Object stringValToIcebergVal(Types.NestedField field, String value) {
      LOGGER.debug("Processing Field:{} Type:{} RawValue:{}", field.name(), field.type(), value);
      if (value == null || value.trim().isEmpty()) {
          return null;
      }
      try {
          switch (field.type().typeId()) {
              case INTEGER:
                  return Integer.parseInt(value.trim());
              case LONG:
                  return Long.parseLong(value.trim());
              case FLOAT:
                  return Float.parseFloat(value.trim());
              case DOUBLE:
                  return Double.parseDouble(value.trim());
              case BOOLEAN:
                  return Boolean.parseBoolean(value.trim());
              case STRING:
                  return value;
              case UUID:
                  return UUID.fromString(value.trim());
              case TIMESTAMP:
                  return OffsetDateTime.parse(value.trim());
              default:
                  return value;
          }
      } catch (Exception e) {
          throw new RuntimeException("Failed to parse value for field " + field.name() +
                                    " as type " + field.type() +
                                    ", raw value: " + value, e);
      }
  }

  // private static Object jsonValToIcebergVal(Types.NestedField field, JsonNode node) {
  //   LOGGER.debug("Processing Field:{} Type:{}", field.name(), field.type());
  //   final Object val;
  //   switch (field.type().typeId()) {
  //     case INTEGER: // int 4 bytes
  //       val = node.isNull() ? null : node.asInt();
  //       break;
  //     case LONG: // long 8 bytes
  //       val = node.isNull() ? null : node.asLong();
  //       break;
  //     case FLOAT: // float is represented in 32 bits,
  //       val = node.isNull() ? null : node.floatValue();
  //       break;
  //     case DOUBLE: // double is represented in 64 bits
  //       val = node.isNull() ? null : node.asDouble();
  //       break;
  //     case BOOLEAN:
  //       val = node.isNull() ? null : node.asBoolean();
  //       break;
  //     case STRING:
  //       // if the node is not a value node (method isValueNode returns false), convert it to string.
  //       val = node.isValueNode() ? node.asText(null) : node.toString();
  //       break;
  //     case UUID:
  //       val = node.isValueNode() ? UUID.fromString(node.asText(null)) : UUID.fromString(node.toString());
  //       break;
  //     case TIMESTAMP:
  //       if ((node.isLong() || node.isNumber()) && TS_MS_FIELDS.contains(field.name())) {
  //         val = node.isNull() ? null : OffsetDateTime.ofInstant(Instant.ofEpochMilli(node.longValue()), ZoneOffset.UTC);
  //       } else if (node.isTextual()) {
  //         val = node.isNull() ? null : OffsetDateTime.parse(node.asText());
  //       } else {
  //         throw new RuntimeException("Failed to convert timestamp value, field: " + field.name() + " value: " + node);
  //       }
  //       break;
  //     case BINARY:
  //       try {
  //         val = node.isNull() ? null : ByteBuffer.wrap(node.binaryValue());
  //       } catch (IOException e) {
  //         throw new RuntimeException("Failed to convert binary value to iceberg value, field: " + field.name(), e);
  //       }
  //       break;
  //     default:
  //       // default to String type
  //       // if the node is not a value node (method isValueNode returns false), convert it to string.
  //       val = node.isValueNode() ? node.asText(null) : node.toString();
  //       break;
  //   }

  //   return val;
  // }


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
