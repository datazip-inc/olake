package io.debezium.server.iceberg;

import java.util.Map;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.Descriptors;

import io.debezium.server.iceberg.rpc.RecordIngest;

public class SchemaConvertor {
  static List<RecordIngest.IcebergPayload.SchemaField> schemaMetadata;
  static String primaryKey;
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
      Boolean isPkField = (fieldName==primaryKey);
      final Types.NestedField field = Types.NestedField.of(schemaData.nextFieldId().getAndIncrement(), !isPkField, fieldName, icebergPrimitiveField(fieldName, fieldType));
      schemaData.fields().add(field);
      if (isPkField) schemaData.identifierFieldIds().add(field.fieldId());
    }
    System.out.println("here i got the identifier field: " + schemaData.identifierFieldIds());
    return new Schema(schemaData.fields(), schemaData.identifierFieldIds());
  }

  private static Type.PrimitiveType icebergPrimitiveField(String fieldName, String fieldType) {
    switch (fieldType) {
      case "int8":
      case "int16":
      case "int32": // int 4 bytes
        return Types.IntegerType.get();
      case "int64": // long 8 bytes
        if (TS_MS_FIELDS.contains(fieldName)) {
          return Types.TimestampType.withZone();
        } else {
          return Types.LongType.get();
        }
      case "float8":
      case "float16":
      case "float32": // float is represented in 32 bits,
        return Types.FloatType.get();
      case "double":
      case "float64": // double is represented in 64 bits
        return Types.DoubleType.get();
      case "boolean":
        return Types.BooleanType.get();
      case "string":
        return Types.StringType.get();
      case "uuid":
        return Types.UUIDType.get();
      case "bytes":
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
