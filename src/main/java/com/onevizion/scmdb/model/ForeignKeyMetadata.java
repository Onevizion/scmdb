package com.onevizion.scmdb.model;

import java.util.List;
import java.util.Objects;

public record ForeignKeyMetadata(String constraintName,
                                 String sourceTable,
                                 List<String> sourceColumns,
                                 String targetTable,
                                 List<String> targetColumns,
                                 boolean composite,
                                 String uniqueIndex) {
    public ForeignKeyMetadata {
        Objects.requireNonNull(constraintName, "constraintName");
        Objects.requireNonNull(sourceTable, "sourceTable");
        sourceColumns = List.copyOf(sourceColumns);
        Objects.requireNonNull(targetTable, "targetTable");
        targetColumns = List.copyOf(targetColumns);
    }
}