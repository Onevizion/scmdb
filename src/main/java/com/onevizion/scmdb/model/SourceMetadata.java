package com.onevizion.scmdb.model;

import java.util.Objects;

public record SourceMetadata(String description, boolean generated, boolean environmentSpecific) {
    public SourceMetadata {
        Objects.requireNonNull(description, "description");
    }
}