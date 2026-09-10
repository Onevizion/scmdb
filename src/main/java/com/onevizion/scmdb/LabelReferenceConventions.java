package com.onevizion.scmdb;

import com.onevizion.scmdb.model.ColumnMetadata;
import com.onevizion.scmdb.model.LabelReferenceMetadata;
import com.onevizion.scmdb.model.LabelWriteDestination;
import com.onevizion.scmdb.model.TableMetadata;

import java.util.List;

/**
 * Platform naming convention (not a real FK): any {@code *_LABEL_ID} column without a declared foreign
 * key may resolve to LABEL_SYSTEM (non-negative id, system-defined) or LABEL_PROGRAM (negative id,
 * program-defined). Kept separate from {@link OracleDdlParser} so canonical DDL parsing stays free of
 * platform-specific guesses; a real FK declared in DDL always takes priority over this convention.
 */
final class LabelReferenceConventions {
    private static final String SYSTEM_TABLE = "LABEL_SYSTEM";
    private static final String SYSTEM_ID_COLUMN = "LABEL_SYSTEM_ID";
    private static final String PROGRAM_TABLE = "LABEL_PROGRAM";
    private static final String PROGRAM_ID_COLUMN = "LABEL_PROGRAM_ID";
    private static final String LANGUAGE_COLUMN = "APP_LANG_ID";
    private static final String PROGRAM_COLUMN = "PROGRAM_ID";
    private static final String LABEL_ID_SUFFIX = "_LABEL_ID";

    private LabelReferenceConventions() {
    }

    static TableMetadata apply(TableMetadata table) {
        List<ColumnMetadata> columns = table.columns().stream()
                .map(column -> applyToColumn(table, column))
                .toList();
        return new TableMetadata(table.name(), table.description(), columns, table.primaryKey(),
                table.foreignKeys(), table.checks(), table.referenceData());
    }

    private static ColumnMetadata applyToColumn(TableMetadata table, ColumnMetadata column) {
        boolean hasForeignKey = table.foreignKeys().stream()
                .anyMatch(key -> key.sourceColumns().contains(column.name()));
        if (hasForeignKey || column.reference() != null || !column.name().endsWith(LABEL_ID_SUFFIX)) {
            return column;
        }
        LabelReferenceMetadata labelReference = new LabelReferenceMetadata(SYSTEM_TABLE, SYSTEM_ID_COLUMN,
                PROGRAM_TABLE, PROGRAM_ID_COLUMN, LANGUAGE_COLUMN, PROGRAM_COLUMN, LabelWriteDestination.PROGRAM);
        return new ColumnMetadata(column.name(), column.oracleType(), column.graphqlScalar(), column.nullable(),
                column.precision(), column.scale(), column.maxLength(), column.description(), column.defaultValue(),
                column.source(), column.readOnly(), column.readOnlyReason(), column.constraints(), null,
                labelReference);
    }
}
