package com.onevizion.scmdb.model;

import java.util.List;
import java.util.Objects;

public record ReferenceMetadata(ReferenceKind kind,
                                String targetTable,
                                List<String> targetColumns,
                                List<String> lookupColumns,
                                List<StaticValueMetadata> staticValues) {
    public ReferenceMetadata {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(targetTable, "targetTable");
        targetColumns = List.copyOf(targetColumns);
        lookupColumns = List.copyOf(lookupColumns);
        staticValues = List.copyOf(staticValues);
    }
}