package io.olake.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.util.List;

import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import io.olake.iceberg.rpc.RecordIngest.ArrowPayload.FileMetadata.PartitionValue;
import io.olake.iceberg.rpc.RecordIngest.IcebergPayload.IceRecord;
import io.olake.iceberg.rpc.RecordIngest.IcebergPayload.IceRecord.FieldValue;
import io.olake.iceberg.rpc.RecordIngest.IcebergPayload.SchemaField;
import io.olake.iceberg.tableoperator.IcebergTableOperator;

class DecimalConversionTest {

    private static final SchemaConvertor CONVERTOR = new SchemaConvertor(null,
            List.of(SchemaField.newBuilder().setKey("price").setIceType("decimal(10, 2)").build()));

    private static Object convert(String text) {
        IceRecord record = IceRecord.newBuilder()
                .addFields(FieldValue.newBuilder().setStringValue(text))
                .setRecordType("r")
                .build();
        return CONVERTOR.convert(false, CONVERTOR.convertToIcebergSchema(), List.of(record)).get(0).getField("price");
    }

    @Test
    void decimalTypeParsesFromItsIcebergName() {
        assertEquals(Types.DecimalType.of(10, 2), CONVERTOR.convertToIcebergSchema().findField("price").type());
    }

    @Test
    void decimalValueTakesTheColumnScale() {
        assertEquals(new BigDecimal("1.50"), convert("1.5"));
    }

    @Test
    void decimalValueThatWouldRoundIsRejected() {
        assertThrows(RuntimeException.class, () -> convert("1.234"));
    }

    @Test
    void decimalPartitionValueIsABigDecimal() {
        Schema schema = CONVERTOR.convertToIcebergSchema();
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("price").build();
        PartitionData partition = new IcebergTableOperator(false).partitionDataFromTypedValues(spec,
                List.of(PartitionValue.newBuilder().setDecimalValue("7.00").build()));
        assertEquals(new BigDecimal("7.00"), partition.get(0, BigDecimal.class));
    }
}
