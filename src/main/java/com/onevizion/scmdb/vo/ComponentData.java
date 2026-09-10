package com.onevizion.scmdb.vo;

import java.util.List;

public record ComponentData(Integer componentId,
                            String componentName,
                            String mainTable,
                            boolean supportBpl,
                            boolean supportAudit,
                            String componentNameColumn,
                            Integer bpdItemTypeId,
                            String bpdItemType,
                            List<TableData> tables) {
}
