package com.onevizion.scmdb.vo;

public record ComponentRow(Integer componentId,
                           String component,
                           String mainTable,
                           Integer supportBpl,
                           Integer supportAudit,
                           String componentNameColumn,
                           Integer componentTableId,
                           String tableName,
                           Integer bpdItemTypeId,
                           String bpdItemType) {
}
