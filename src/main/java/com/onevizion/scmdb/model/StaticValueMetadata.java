package com.onevizion.scmdb.model;

import java.util.Objects;

public record StaticValueMetadata(String id, String name) {
    public StaticValueMetadata {
        Objects.requireNonNull(id, "id");
    }
}