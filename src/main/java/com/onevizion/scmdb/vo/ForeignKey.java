package com.onevizion.scmdb.vo;

public record ForeignKey(String constraintName,
                         String fromTable,
                         String fromColumn,
                         String toTable,
                         String toColumn) {
}
