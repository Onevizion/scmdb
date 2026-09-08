package com.onevizion.scmdb.model;

public enum GraphqlScalar {
    BIG_INT("BigInt"),
    BOOLEAN("Boolean"),
    DATE("Date"),
    DATE_TIME("DateTime"),
    DECIMAL("Decimal"),
    ID("ID"),
    INT("Int"),
    STRING("String");

    private final String typeName;

    GraphqlScalar(String typeName) {
        this.typeName = typeName;
    }

    public String typeName() {
        return typeName;
    }
}