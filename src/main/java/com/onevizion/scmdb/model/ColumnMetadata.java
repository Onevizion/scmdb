package com.onevizion.scmdb.model;

import java.util.Objects;

public record ColumnMetadata(String name,
                             String oracleType,
                             GraphqlScalar graphqlScalar,
                             boolean nullable,
                             Integer precision,
                             Integer scale,
                             Integer maxLength,
                             String description,
                             DefaultMetadata defaultValue,
                             SourceMetadata source,
                             boolean readOnly,
                             String readOnlyReason,
                             ConstraintMetadata constraints,
                             ReferenceMetadata reference) {
    public ColumnMetadata {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(oracleType, "oracleType");
        Objects.requireNonNull(graphqlScalar, "graphqlScalar");
        constraints = constraints == null ? ConstraintMetadata.empty() : constraints;
    }
}