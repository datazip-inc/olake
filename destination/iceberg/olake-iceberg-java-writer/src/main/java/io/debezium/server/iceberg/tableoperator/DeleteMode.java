package io.debezium.server.iceberg.tableoperator;

import io.debezium.server.iceberg.rpc.RecordIngest.IcebergPayload;

/** How a writer represents the removal of a row that a later version supersedes. */
public enum DeleteMode {
  /** Equality delete files keyed on the table's identifier fields. */
  EQUALITY("eq"),
  /** Positional delete files addressing (data file, row offset). */
  POSITION("pos"),
  /** Format v3 deletion vectors: one Puffin bitmap per data file. */
  DELETION_VECTOR("dv");

  private final String wireName;

  DeleteMode(String wireName) {
    this.wireName = wireName;
  }

  public String wireName() {
    return wireName;
  }

  /** True when the writer addresses rows by position, which needs the caller's table index. */
  public boolean addressesPositions() {
    return this == POSITION || this == DELETION_VECTOR;
  }

  /** Deletion vectors are a v3 construct; everything else works on v2. */
  public int minimumFormatVersion() {
    return this == DELETION_VECTOR ? 3 : 2;
  }

  /**
   * Maps the wire enum onto this one. UNSPECIFIED is rejected rather than defaulted:
   * it means the sender did not set delete_mode, and guessing on its behalf would
   * silently write a representation the caller never asked for - and, for a table
   * created under that guess, at the wrong format version. Every caller sets it,
   * including the destination check, which sends EQUALITY for its throwaway table.
   * UNRECOGNIZED means the sender knows a mode this build does not.
   */
  public static DeleteMode resolve(IcebergPayload.DeleteMode deleteMode) {
    switch (deleteMode) {
      case DELETE_MODE_EQUALITY:
        return EQUALITY;
      case DELETE_MODE_POSITION:
        return POSITION;
      case DELETE_MODE_DELETION_VECTOR:
        return DELETION_VECTOR;
      default:
        throw new IllegalArgumentException(String.format(
            "delete mode %s cannot be used; the caller must set delete_mode on "
                + "GET_OR_CREATE_TABLE to one of eq, pos or dv",
            deleteMode));
    }
  }
}
