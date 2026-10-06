package com.onevizion.scmdb.model;

import java.util.List;
import java.util.Objects;

public record CheckConstraintMetadata(String name,
                                      String expression,
                                      List<String> referencedColumns,
                                      List<DerivedColumnConstraint> derivedColumnConstraints) {
    public CheckConstraintMetadata {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(expression, "expression");
        referencedColumns = List.copyOf(referencedColumns);
        derivedColumnConstraints = List.copyOf(derivedColumnConstraints);
    }

    public record DerivedColumnConstraint(String columnName,
                                          String minimum,
                                          String maximum,
                                          String notEqual,
                                          List<String> allowedValues) {
        public DerivedColumnConstraint {
            Objects.requireNonNull(columnName, "columnName");
            allowedValues = allowedValues == null ? List.of() : List.copyOf(allowedValues);
        }
    }
}