package com.onevizion.scmdb.vo;

public record TableData(String tableName,
                        boolean mainTable,
                        Integer bpdItemTypeId,
                        String bpdItemType) {
}
