package com.onevizion.scmdb.model;

import java.util.Objects;

/**
 * A {@code *_LABEL_ID} column that may resolve to either a system-defined label (LABEL_SYSTEM,
 * non-negative id) or a program-defined label (LABEL_PROGRAM, negative id). Reads must consult both
 * tables; writes always target {@code writeTarget}.
 */
public record LabelReferenceMetadata(String systemTable,
                                     String systemIdColumn,
                                     String programTable,
                                     String programIdColumn,
                                     String languageColumn,
                                     String programColumn,
                                     LabelWriteDestination writeTarget) {
    public LabelReferenceMetadata {
        Objects.requireNonNull(systemTable, "systemTable");
        Objects.requireNonNull(systemIdColumn, "systemIdColumn");
        Objects.requireNonNull(programTable, "programTable");
        Objects.requireNonNull(programIdColumn, "programIdColumn");
        Objects.requireNonNull(languageColumn, "languageColumn");
        Objects.requireNonNull(writeTarget, "writeTarget");
    }
}
