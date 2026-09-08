package com.onevizion.scmdb.model;

import java.util.List;
import java.util.Objects;

public record ComponentMetadata(Integer componentId,
                                String componentName,
                                String mainTable,
                                List<TableMetadata> tables,
                                ComponentHierarchyNode hierarchy) {
    public ComponentMetadata {
        Objects.requireNonNull(componentId, "componentId");
        Objects.requireNonNull(componentName, "componentName");
        Objects.requireNonNull(mainTable, "mainTable");
        tables = List.copyOf(tables);
        Objects.requireNonNull(hierarchy, "hierarchy");
    }

    public TableMetadata table(String tableName) {
        return tables.stream()
                     .filter(table -> table.name().equals(tableName))
                     .findFirst()
                     .orElse(null);
    }
}