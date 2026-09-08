package com.onevizion.scmdb.model;

import java.util.List;

public record ConstraintMetadata(String minimum,
                                 String maximum,
                                 String notEqual,
                                 String pattern,
                                 String format,
                                 List<String> allowedValues) {
    public ConstraintMetadata {
        allowedValues = allowedValues == null ? List.of() : List.copyOf(allowedValues);
    }

    public static ConstraintMetadata empty() {
        return new ConstraintMetadata(null, null, null, null, null, List.of());
    }
}