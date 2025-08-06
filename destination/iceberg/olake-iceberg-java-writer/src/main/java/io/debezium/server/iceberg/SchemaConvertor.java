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
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Value;
import  io.debezium.server.iceberg.rpc.RecordIngest;
import io.debezium.server.iceberg.rpc.RecordIngest.IcebergPayload.IceRecord;
import io.debezium.server.iceberg.tableoperator.Operation;
import io.debezium.server.iceberg.tableoperator.RecordWrapper;
import io.debezium.util.Strings;

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

  public List<RecordWrapper> convert(Boolean upsert,Schema tableSchema, List<IceRecord> records) {
      // Pre-compute schema information once
      StructType tableFields = tableSchema.asStruct();
      // Use parallel stream with optimal chunk sizing
      return records.parallelStream()
          .map(data -> convertRecord(upsert, data, tableFields))
          .collect(Collectors.toList());
  }

  private RecordWrapper convertRecord(Boolean upsert, IceRecord data, StructType tableFields) {
      // Create record using first field's struct (optimization)
      GenericRecord genericRow = GenericRecord.create(tableFields);
      Map<String, Value> fieldsMap = data.getFieldsMap();
      List<Types.NestedField> fields = tableFields.fields();

      // Process all fields 
      for (Types.NestedField field : fields) {
          String fieldName = field.name();
          // Get field value - single map lookup
          Value fieldValue = fieldsMap.get(fieldName);
          if (fieldValue == null || !fieldValue.hasStringValue() || fieldValue.getStringValue() == "") {
              genericRow.setField(fieldName, null);
              continue;
          }
          try {
              String stringValue = fieldValue.getStringValue();
              ObjectMapper objectMapper = new ObjectMapper();
              JsonNode node = objectMapper.readTree(stringValue);
              Object convertedValue = jsonValToIcebergVal(field, node);
              genericRow.setField(fieldName, convertedValue);
          } catch (IOException e) {
              throw new RuntimeException("Failed to parse JSON string for field "+fieldName, e);
          }
      }
      // check if it is append only or upsert
      if(!upsert) { 
        // TODO: need a discussion previously Operation.Insert was being used
        return new RecordWrapper(genericRow, Operation.READ);
      }
      return new RecordWrapper(genericRow, cdcOpValue(data.getRecordType()));
  }

  private static Object jsonValToIcebergVal(Types.NestedField field, JsonNode node) {
    LOGGER.debug("Processing Field:{} Type:{}", field.name(), field.type());
    final Object val;
    switch (field.type().typeId()) {
      case INTEGER: // int 4 bytes
        val = node.isNull() ? null : node.asInt();
        break;
      case LONG: // long 8 bytes
        val = node.isNull() ? null : node.asLong();
        break;
      case FLOAT: // float is represented in 32 bits,
        val = node.isNull() ? null : node.floatValue();
        break;
      case DOUBLE: // double is represented in 64 bits
        val = node.isNull() ? null : node.asDouble();
        break;
      case BOOLEAN:
        val = node.isNull() ? null : node.asBoolean();
        break;
      case STRING:
        // if the node is not a value node (method isValueNode returns false), convert it to string.
        val = node.isValueNode() ? node.asText(null) : node.toString();
        break;
      case UUID:
        val = node.isValueNode() ? UUID.fromString(node.asText(null)) : UUID.fromString(node.toString());
        break;
      case TIMESTAMP:
        if ((node.isLong() || node.isNumber()) && TS_MS_FIELDS.contains(field.name())) {
          val = OffsetDateTime.ofInstant(Instant.ofEpochMilli(node.longValue()), ZoneOffset.UTC);
        } else if (node.isTextual()) {
          val = OffsetDateTime.parse(node.asText());
        } else {
          throw new RuntimeException("Failed to convert timestamp value, field: " + field.name() + " value: " + node);
        }
        break;
      case BINARY:
        try {
          val = node.isNull() ? null : ByteBuffer.wrap(node.binaryValue());
        } catch (IOException e) {
          throw new RuntimeException("Failed to convert binary value to iceberg value, field: " + field.name(), e);
        }
        break;
      default:
        // default to String type
        // if the node is not a value node (method isValueNode returns false), convert it to string.
        val = node.isValueNode() ? node.asText(null) : node.toString();
        break;
    }

    return val;
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
