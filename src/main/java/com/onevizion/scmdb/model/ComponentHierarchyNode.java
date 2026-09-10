package com.onevizion.scmdb.model;

import java.util.List;
import java.util.Objects;

public record ComponentHierarchyNode(String tableName,
                                     boolean mainTable,
                                     boolean cycle,
                                     RelationMetadata parentRelation,
                                     List<ComponentHierarchyNode> children) {
    public ComponentHierarchyNode {
        Objects.requireNonNull(tableName, "tableName");
        children = List.copyOf(children);
    }
}