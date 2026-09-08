package com.onevizion.scmdb.model;

import java.util.List;
import java.util.Objects;

public record TableMetadata(String name,
                            String description,
                            List<ColumnMetadata> columns,
                            List<String> primaryKey,
                            List<ForeignKeyMetadata> foreignKeys,
                            List<CheckConstraintMetadata> checks,
                            ReferenceMetadata referenceData) {
    public TableMetadata {
        Objects.requireNonNull(name, "name");
        columns = List.copyOf(columns);
        primaryKey = List.copyOf(primaryKey);
        foreignKeys = List.copyOf(foreignKeys);
        checks = List.copyOf(checks);
    }

    public ColumnMetadata column(String columnName) {
        return columns.stream()
                      .filter(column -> column.name().equals(columnName))
                      .findFirst()
                      .orElse(null);
    }
}