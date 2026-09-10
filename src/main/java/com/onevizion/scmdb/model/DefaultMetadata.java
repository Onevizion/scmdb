package com.onevizion.scmdb.model;

import java.util.Objects;

public record DefaultMetadata(String rawExpression, String normalizedValue, DefaultKind kind) {
    public DefaultMetadata {
        Objects.requireNonNull(rawExpression, "rawExpression");
        Objects.requireNonNull(kind, "kind");
    }
}