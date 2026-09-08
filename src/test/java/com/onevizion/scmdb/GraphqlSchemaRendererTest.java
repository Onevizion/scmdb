package com.onevizion.scmdb;

import com.onevizion.scmdb.model.ColumnMetadata;
import com.onevizion.scmdb.model.ComponentHierarchyNode;
import com.onevizion.scmdb.model.ComponentMetadata;
import com.onevizion.scmdb.model.ConstraintMetadata;
import com.onevizion.scmdb.model.ForeignKeyMetadata;
import com.onevizion.scmdb.model.GraphqlScalar;
import com.onevizion.scmdb.model.ReferenceKind;
import com.onevizion.scmdb.model.ReferenceMetadata;
import com.onevizion.scmdb.model.RelationMetadata;
import com.onevizion.scmdb.model.StaticValueMetadata;
import com.onevizion.scmdb.model.TableMetadata;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphqlSchemaRendererTest {
    private final GraphqlNamingService naming = new GraphqlNamingService();

    @Test
    void rendersReferencesNestedInputsCompositeKeysAndCycles() {
        ReferenceMetadata dynamicConfig = new ReferenceMetadata(ReferenceKind.DYNAMIC, "CONFIG_FIELD",
                List.of("CONFIG_FIELD_ID"), List.of("CONFIG_FIELD_NAME"), List.of());
        ReferenceMetadata staticKind = new ReferenceMetadata(ReferenceKind.STATIC, "WIDGET_KIND",
                List.of("WIDGET_KIND_ID"), List.of("WIDGET_KIND"),
                List.of(new StaticValueMetadata("1", "Text")));
        ReferenceMetadata label = new ReferenceMetadata(ReferenceKind.DYNAMIC, "LABEL_PROGRAM",
                List.of("LABEL_PROGRAM_ID", "APP_LANG_ID"), List.of(), List.of());
        ForeignKeyMetadata configKey = foreignKey("FK_CONFIG", "WIDGET", "CONFIG_FIELD_ID", "CONFIG_FIELD", "CONFIG_FIELD_ID");
        ForeignKeyMetadata kindKey = foreignKey("FK_KIND", "WIDGET", "WIDGET_KIND_ID", "WIDGET_KIND", "WIDGET_KIND_ID");
        ForeignKeyMetadata labelKey = new ForeignKeyMetadata("SYNTHETIC_WIDGET_DISPLAY_LABEL_ID", "WIDGET",
                List.of("DISPLAY_LABEL_ID"), "LABEL_PROGRAM", List.of("LABEL_PROGRAM_ID", "APP_LANG_ID"),
                true, "UN1_LABEL_PROGRAM");
        TableMetadata widget = table("WIDGET", "Widget definition", List.of(
                column("WIDGET_ID", GraphqlScalar.ID, false, null),
                column("WIDGET_NAME", GraphqlScalar.STRING, false, null),
                column("CONFIG_FIELD_ID", GraphqlScalar.ID, false, dynamicConfig),
                column("WIDGET_KIND_ID", GraphqlScalar.ID, false, staticKind),
                column("DISPLAY_LABEL_ID", GraphqlScalar.ID, true, label)),
                List.of(configKey, kindKey, labelKey));
        TableMetadata parameter = table("WIDGET_PARAM", "Widget parameter", List.of(
                column("WIDGET_PARAM_ID", GraphqlScalar.ID, false, null),
                column("WIDGET_ID", GraphqlScalar.ID, false,
                        new ReferenceMetadata(ReferenceKind.DYNAMIC, "WIDGET", List.of("WIDGET_ID"), List.of(), List.of())),
                column("PARAM_NAME", GraphqlScalar.STRING, false, null)),
                List.of(foreignKey("FK_WIDGET_PARAM", "WIDGET_PARAM", "WIDGET_ID", "WIDGET", "WIDGET_ID")));
        TableMetadata config = table("CONFIG_FIELD", "Configured field", List.of(
                column("CONFIG_FIELD_ID", GraphqlScalar.ID, false, null),
                column("CONFIG_FIELD_NAME", GraphqlScalar.STRING, false, null)), List.of());
        RelationMetadata relation = new RelationMetadata("WIDGET_PARAM", List.of("WIDGET_ID"),
                "WIDGET", List.of("WIDGET_ID"), "FK_WIDGET_PARAM", true, false);
        ComponentHierarchyNode cycle = new ComponentHierarchyNode("WIDGET", true, true,
                new RelationMetadata("WIDGET", List.of("WIDGET_ID"), "WIDGET_PARAM",
                        List.of("WIDGET_PARAM_ID"), "FK_CYCLE", true, true), List.of());
        ComponentHierarchyNode child = new ComponentHierarchyNode("WIDGET_PARAM", false, false, relation, List.of(cycle));
        ComponentMetadata component = new ComponentMetadata(57, "Widget", "WIDGET",
                List.of(widget, parameter, config),
                new ComponentHierarchyNode("WIDGET", true, false, null, List.of(child)));

        GraphqlSchemaRenderer renderer = new GraphqlSchemaRenderer(component, naming);
        String sdl = renderer.render();

        assertTrue(sdl.contains("scalar BigInt"));
        assertTrue(sdl.contains("directive @reference("));
        assertTrue(sdl.contains("type Widget @table(name: \"WIDGET\")"));
        assertTrue(sdl.contains("widgetParam: [WidgetParam!] @relation(fromTable: \"WIDGET_PARAM\""));
        assertTrue(sdl.contains("configFieldId: ID! @column(name: \"CONFIG_FIELD_ID\") @reference(table: \"CONFIG_FIELD\", column: [\"CONFIG_FIELD_ID\"], lookup: [\"CONFIG_FIELD_NAME\"], kind: DYNAMIC)"));
        assertTrue(sdl.contains("configFieldName: String @referenceLookup(field: \"configFieldId\")"));
        assertTrue(sdl.contains("widgetKind: WidgetWidgetKind @referenceLookup(field: \"widgetKindId\")"));
        assertTrue(sdl.contains("TEXT @dbValue(id: \"1\")"));
        assertTrue(sdl.contains("displayLabelId: ID @column(name: \"DISPLAY_LABEL_ID\") @reference(table: \"LABEL_PROGRAM\", column: [\"LABEL_PROGRAM_ID\", \"APP_LANG_ID\"], lookup: [], kind: DYNAMIC, compositeKey: true, uniqueIndex: \"UN1_LABEL_PROGRAM\")"));
        assertTrue(sdl.contains("input ConfigFieldReferenceInput"));
        assertFalse(sdl.contains("widget: [Widget!]"));
    }

    @Test
    void usesMetaValueForPhysicalLookupNameCollisionsAndNeverDuplicatesFields() {
        ReferenceMetadata dataType = new ReferenceMetadata(ReferenceKind.STATIC, "DATA_TYPE_REF",
                List.of("DATA_TYPE_ID"), List.of("DATA_TYPE"), List.of(new StaticValueMetadata("1", "Text")));
        ReferenceMetadata caseReference = new ReferenceMetadata(ReferenceKind.STATIC, "REPORT_PARAMS_CASE",
                List.of("REPORT_PARAMS_CASE_ID"), List.of("CASE_TYPE"), List.of(new StaticValueMetadata("1", "Upper")));
        TableMetadata report = table("REPORT_PARAMS", "Report parameters", List.of(
                column("DATA_TYPE", GraphqlScalar.BIG_INT, true, dataType),
                column("CASE", GraphqlScalar.BIG_INT, true, caseReference)), List.of(
                foreignKey("FK_DATA_TYPE", "REPORT_PARAMS", "DATA_TYPE", "DATA_TYPE_REF", "DATA_TYPE_ID"),
                foreignKey("FK_CASE", "REPORT_PARAMS", "CASE", "REPORT_PARAMS_CASE", "REPORT_PARAMS_CASE_ID")));
        ComponentMetadata component = component(report);

        String sdl = new GraphqlSchemaRenderer(component, naming).render();

        assertTrue(sdl.contains("dataTypeMetaValue: ReportParamsDataType"));
        assertTrue(sdl.contains("caseMetaValue: ReportParamsCase"));
        assertEquals(2, sdl.lines().filter(line -> line.startsWith("  dataType: ")).count());
        assertEquals(2, sdl.lines().filter(line -> line.startsWith("  case: ")).count());
    }

    @Test
    void appliesDedicatedPhysicalNamingRules() {
        assertEquals("Widget", naming.typeName("WIDGET"));
        assertEquals("WidgetParam", naming.typeName("WIDGET_PARAM"));
        assertEquals("widgetParam", naming.fieldName("WIDGET_PARAM"));
        assertEquals("programId", naming.fieldName("PROGRAM_ID"));
        assertEquals("programName", naming.fieldName("PROGRAM_NAME"));
    }

    private static ComponentMetadata component(TableMetadata table) {
        return new ComponentMetadata(1, table.name(), table.name(), List.of(table),
                new ComponentHierarchyNode(table.name(), true, false, null, List.of()));
    }

    private static TableMetadata table(String name, String description, List<ColumnMetadata> columns,
                                       List<ForeignKeyMetadata> foreignKeys) {
        return new TableMetadata(name, description, columns, List.of(columns.get(0).name()), foreignKeys, List.of(), null);
    }

    private static ColumnMetadata column(String name, GraphqlScalar scalar, boolean nullable,
                                         ReferenceMetadata reference) {
        return new ColumnMetadata(name, scalar == GraphqlScalar.STRING ? "VARCHAR2" : "NUMBER", scalar,
                nullable, null, null, null, null, null, null, false, null, ConstraintMetadata.empty(), reference);
    }

    private static ForeignKeyMetadata foreignKey(String name, String sourceTable, String sourceColumn,
                                                 String targetTable, String targetColumn) {
        return new ForeignKeyMetadata(name, sourceTable, List.of(sourceColumn), targetTable,
                List.of(targetColumn), false, null);
    }
}
