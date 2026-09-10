package com.onevizion.scmdb;

import com.onevizion.scmdb.model.ColumnMetadata;
import com.onevizion.scmdb.model.ConstraintMetadata;
import com.onevizion.scmdb.model.ForeignKeyMetadata;
import com.onevizion.scmdb.model.GraphqlScalar;
import com.onevizion.scmdb.model.LabelReferenceMetadata;
import com.onevizion.scmdb.model.LabelWriteDestination;
import com.onevizion.scmdb.model.TableMetadata;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class LabelReferenceConventionsTest {
    @Test
    void appliesSystemProgramLabelConventionToPlainLabelIdColumn() {
        TableMetadata table = new TableMetadata("TEST_ROOT", null, List.of(
                idColumn(), column("DISPLAY_LABEL_ID")),
                List.of("TEST_ROOT_ID"), List.of(), List.of(), null);

        TableMetadata enriched = LabelReferenceConventions.apply(table);

        LabelReferenceMetadata labelReference = enriched.column("DISPLAY_LABEL_ID").labelReference();
        assertEquals("LABEL_SYSTEM", labelReference.systemTable());
        assertEquals("LABEL_SYSTEM_ID", labelReference.systemIdColumn());
        assertEquals("LABEL_PROGRAM", labelReference.programTable());
        assertEquals("LABEL_PROGRAM_ID", labelReference.programIdColumn());
        assertEquals("APP_LANG_ID", labelReference.languageColumn());
        assertEquals("PROGRAM_ID", labelReference.programColumn());
        assertEquals(LabelWriteDestination.PROGRAM, labelReference.writeTarget());
        assertNull(enriched.column("DISPLAY_LABEL_ID").reference());
    }

    @Test
    void realForeignKeyOnLabelIdColumnTakesPriorityOverConvention() {
        ForeignKeyMetadata realForeignKey = new ForeignKeyMetadata("FK_OWNER_LABEL", "TEST_ROOT",
                List.of("OWNER_LABEL_ID"), "TEST_LOOKUP", List.of("TEST_LOOKUP_ID"), false, null);
        TableMetadata table = new TableMetadata("TEST_ROOT", null, List.of(
                idColumn(), column("OWNER_LABEL_ID")),
                List.of("TEST_ROOT_ID"), List.of(realForeignKey), List.of(), null);

        TableMetadata enriched = LabelReferenceConventions.apply(table);

        assertNull(enriched.column("OWNER_LABEL_ID").labelReference());
    }

    @Test
    void leavesColumnsNotEndingInLabelIdUnchanged() {
        TableMetadata table = new TableMetadata("TEST_ROOT", null, List.of(
                idColumn(), column("TEST_ROOT_NAME")),
                List.of("TEST_ROOT_ID"), List.of(), List.of(), null);

        TableMetadata enriched = LabelReferenceConventions.apply(table);

        assertNull(enriched.column("TEST_ROOT_NAME").labelReference());
    }

    private static ColumnMetadata idColumn() {
        return new ColumnMetadata("TEST_ROOT_ID", "NUMBER", GraphqlScalar.ID, false, null, null, null, null,
                null, null, false, null, ConstraintMetadata.empty(), null, null);
    }

    private static ColumnMetadata column(String name) {
        GraphqlScalar scalar = name.endsWith("_ID") ? GraphqlScalar.ID : GraphqlScalar.STRING;
        return new ColumnMetadata(name, scalar == GraphqlScalar.ID ? "NUMBER" : "VARCHAR2", scalar, true,
                null, null, null, null, null, null, false, null, ConstraintMetadata.empty(), null, null);
    }
}
