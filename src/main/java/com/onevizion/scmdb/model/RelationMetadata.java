package com.onevizion.scmdb.model;

import java.util.List;
import java.util.Objects;

public record RelationMetadata(String sourceTable,
                               List<String> sourceColumns,
                               String targetTable,
                               List<String> targetColumns,
                               String constraintName,
                               boolean containment,
                               boolean cycle) {
    public RelationMetadata {
        Objects.requireNonNull(sourceTable, "sourceTable");
        sourceColumns = List.copyOf(sourceColumns);
        Objects.requireNonNull(targetTable, "targetTable");
        targetColumns = List.copyOf(targetColumns);
        Objects.requireNonNull(constraintName, "constraintName");
    }
}