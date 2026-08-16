package io.debezium.server.iceberg.tableoperator;

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

  /** True when the writer addresses rows by position, which needs the caller's row index. */
  public boolean addressesPositions() {
    return this == POSITION || this == DELETION_VECTOR;
  }

  /** Deletion vectors are a v3 construct; everything else works on v2. */
  public int minimumFormatVersion() {
    return this == DELETION_VECTOR ? 3 : 2;
  }

  /**
   * Resolves the mode a request asked for. {@code deleteMode} wins when present;
   * otherwise the older boolean is honoured so callers predating this field keep the
   * behaviour they had.
   */
  public static DeleteMode resolve(String deleteMode, boolean usePositionalDeletes) {
    if (deleteMode != null && !deleteMode.isBlank()) {
      for (DeleteMode mode : values()) {
        if (mode.wireName.equalsIgnoreCase(deleteMode.trim())) {
          return mode;
        }
      }
      throw new IllegalArgumentException("unknown delete mode: " + deleteMode);
    }
    return usePositionalDeletes ? POSITION : EQUALITY;
  }
}
