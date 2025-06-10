package io.debezium.server.iceberg;

import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

record RecordSchemaData(List<Types.NestedField> fields, Set<Integer> identifierFieldIds,
                        AtomicInteger nextFieldId) {


  public RecordSchemaData(Integer nextFieldId) {
    this(new ArrayList<>(), new HashSet<>(), new AtomicInteger(nextFieldId));
  }

  public RecordSchemaData() {
    this(new ArrayList<>(), new HashSet<>(), new AtomicInteger(1));
  }

  public RecordSchemaData copyKeepIdentifierFieldIdsAndNextFieldId() {
    return new RecordSchemaData(new ArrayList<>(), this.identifierFieldIds, this.nextFieldId);
  }


}
