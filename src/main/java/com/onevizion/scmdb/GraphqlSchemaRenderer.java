package com.onevizion.scmdb;

import com.onevizion.scmdb.model.CheckConstraintMetadata;
import com.onevizion.scmdb.model.ColumnMetadata;
import com.onevizion.scmdb.model.ComponentHierarchyNode;
import com.onevizion.scmdb.model.ComponentMetadata;
import com.onevizion.scmdb.model.ConstraintMetadata;
import com.onevizion.scmdb.model.ForeignKeyMetadata;
import com.onevizion.scmdb.model.ReferenceKind;
import com.onevizion.scmdb.model.ReferenceMetadata;
import com.onevizion.scmdb.model.RelationMetadata;
import com.onevizion.scmdb.model.StaticValueMetadata;
import com.onevizion.scmdb.model.TableMetadata;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class GraphqlSchemaRenderer {
    private static final String PRELUDE = """
            scalar BigInt
            scalar Decimal
            scalar Date
            scalar DateTime

            enum ReferenceKind {
              STATIC
              DYNAMIC
            }

            directive @table(name: String!) on OBJECT | INPUT_OBJECT
            directive @column(name: String!) on FIELD_DEFINITION | INPUT_FIELD_DEFINITION
            directive @source(value: String!) on FIELD_DEFINITION | INPUT_FIELD_DEFINITION
            directive @readOnly(reason: String) on FIELD_DEFINITION
            directive @reference(table: String!, column: [String!]!, lookup: [String!]!, kind: ReferenceKind, compositeKey: Boolean = false, uniqueIndex: String) on FIELD_DEFINITION | INPUT_FIELD_DEFINITION
            directive @referenceLookup(field: String!) on FIELD_DEFINITION | INPUT_FIELD_DEFINITION
            directive @relation(fromTable: String!, fromColumn: String!, toTable: String!, toColumn: String!, constraint: String!) on FIELD_DEFINITION | INPUT_FIELD_DEFINITION
            directive @check(name: String!, expression: String!, columns: [String!]!) repeatable on OBJECT | INPUT_OBJECT | FIELD_DEFINITION | INPUT_FIELD_DEFINITION
            directive @constraint(maxLength: Int, minimum: String, maximum: String, pattern: String, format: String, notEqual: String, precision: Int, scale: Int) on FIELD_DEFINITION | INPUT_FIELD_DEFINITION
            directive @defaultValue(value: String!) on FIELD_DEFINITION | INPUT_FIELD_DEFINITION
            directive @dbValue(id: String!) on ENUM_VALUE
            """;

    private final ComponentMetadata component;
    private final GraphqlNamingService naming;
    private final Map<String, TableMetadata> renderedTables = new LinkedHashMap<>();
    private final Map<String, List<StaticValueMetadata>> enums = new LinkedHashMap<>();
    private final Map<String, String> columnEnums = new LinkedHashMap<>();
    private final Map<String, Map<String, ReferenceInputField>> referenceInputs = new LinkedHashMap<>();

    public GraphqlSchemaRenderer(ComponentMetadata component, GraphqlNamingService naming) {
        this.component = Objects.requireNonNull(component, "component");
        this.naming = Objects.requireNonNull(naming, "naming");
    }

    public static String prelude() {
        return PRELUDE;
    }

    public String render() {
        collect(component.hierarchy());
        List<String> sections = new ArrayList<>();
        sections.add("# Generated component: " + component.componentName() + " (id=" + component.componentId() + ")\n\n" + PRELUDE);
        enums.forEach((name, values) -> sections.add(renderEnum(name, values)));
        renderedTables.values().forEach(table -> sections.add(renderObject(table, false)));
        renderedTables.values().forEach(table -> sections.add(renderObject(table, true)));
        referenceInputs.forEach((name, fields) -> sections.add(renderReferenceInput(name, fields)));
        return String.join("\n\n", sections) + "\n";
    }

    private void collect(ComponentHierarchyNode node) {
        if (node.cycle()) {
            return;
        }
        TableMetadata table = requireTable(node.tableName());
        if (renderedTables.putIfAbsent(table.name(), table) != null) {
            return;
        }
        registerEnums(table);
        node.children().forEach(this::collect);
    }

    private void registerEnums(TableMetadata table) {
        for (ColumnMetadata column : table.columns()) {
            ReferenceMetadata reference = column.reference();
            if (reference == null || reference.kind() != ReferenceKind.STATIC) {
                continue;
            }
            List<StaticValueMetadata> values = reference.staticValues().stream()
                    .filter(value -> value.name() != null)
                    .filter(value -> column.constraints().allowedValues().isEmpty()
                            || column.constraints().allowedValues().contains(value.id())).toList();
            if (values.isEmpty()) {
                continue;
            }
            String base = naming.enumTypeName(table.name(), column.name());
            String enumName = base;
            int suffix = 2;
            while (enums.containsKey(enumName) && !enums.get(enumName).equals(values)) {
                enumName = base + suffix++;
            }
            enums.putIfAbsent(enumName, values);
            columnEnums.put(table.name() + ":" + column.name(), enumName);
        }
    }

    private String renderObject(TableMetadata table, boolean input) {
        List<String> lines = new ArrayList<>();
        description(lines, table.description(), "");
        StringBuilder declaration = new StringBuilder((input ? "input " : "type ") + naming.typeName(table.name())
                + (input ? "Input" : "") + " @table(name: " + quote(table.name()) + ")");
        for (CheckConstraintMetadata check : table.checks()) {
            declaration.append("\n  ").append(checkDirective(check));
        }
        lines.add(declaration + " {");
        Set<String> usedNames = physicalFieldNames(table);
        Set<String> parentColumns = parentSourceColumns(table.name());
        for (ColumnMetadata column : table.columns()) {
            if (input && ((column.source() != null && column.source().generated())
                    || parentColumns.contains(column.name()))) {
                continue;
            }
            boolean requiredInput = !column.nullable() && column.defaultValue() == null
                    && !column.readOnly() && !hasDynamicReference(column);
            boolean nonNull = input ? requiredInput : !column.nullable();
            addField(lines, naming.fieldName(column.name()),
                    column.graphqlScalar().typeName() + (nonNull ? "!" : ""),
                    column.description(), directives(table, column, input));
            addReferenceFields(lines, table, column, input, usedNames);
        }
        ComponentHierarchyNode hierarchy = hierarchyNode(component.hierarchy(), table.name());
        if (hierarchy != null) {
            for (ComponentHierarchyNode child : hierarchy.children()) {
                if (!child.cycle()) {
                    String name = naming.relationFieldName(child.tableName(), usedNames);
                    String type = "[" + naming.typeName(child.tableName()) + (input ? "Input" : "") + "!]";
                    addField(lines, name, type, null, List.of(relationDirective(child.parentRelation())));
                }
            }
        }
        lines.add("}");
        return String.join("\n", lines);
    }

    private void addReferenceFields(List<String> lines, TableMetadata table, ColumnMetadata column,
                                    boolean input, Set<String> usedNames) {
        ReferenceMetadata reference = column.reference();
        if (reference == null) {
            return;
        }
        String sourceField = naming.fieldName(column.name());
        if (reference.kind() == ReferenceKind.STATIC && !reference.staticValues().isEmpty()) {
            String name = naming.lookupFieldName(column.name(), usedNames);
            String enumType = columnEnums.getOrDefault(table.name() + ":" + column.name(), "String");
            addField(lines, name, enumType, null, List.of(referenceLookup(sourceField)));
        } else if (input && reference.kind() == ReferenceKind.DYNAMIC && !reference.lookupColumns().isEmpty()) {
            String name = sourceField.endsWith("Id") ? sourceField.substring(0, sourceField.length() - 2)
                                                     : sourceField + "Reference";
            if (!usedNames.add(name)) {
                name = naming.lookupFieldName(column.name(), usedNames);
            }
            String inputName = naming.typeName(reference.targetTable()) + "ReferenceInput";
            registerReferenceInput(inputName, reference);
            addField(lines, name, inputName, column.description(), List.of(referenceDirective(table, column)));
        } else if (!input && !reference.lookupColumns().isEmpty()) {
            TableMetadata target = component.table(reference.targetTable());
            for (String lookupColumn : reference.lookupColumns()) {
                String name = naming.fieldName(lookupColumn);
                if (usedNames.add(name)) {
                    ColumnMetadata lookup = target == null ? null : target.column(lookupColumn);
                    addField(lines, name, lookup == null ? "String" : lookup.graphqlScalar().typeName(),
                             lookup == null ? null : lookup.description(), List.of(referenceLookup(sourceField)));
                }
            }
        } else if (reference.lookupColumns().isEmpty()) {
            if (!renderedTables.containsKey(reference.targetTable())) {
                return;
            }
            String name = sourceField.endsWith("Id") ? sourceField.substring(0, sourceField.length() - 2)
                                                     : naming.fieldName(reference.targetTable());
            if (usedNames.add(name)) {
                addField(lines, name, naming.typeName(reference.targetTable()) + (input ? "Input" : ""),
                         null, List.of(referenceLookup(sourceField)));
            }
        }
    }

    private void registerReferenceInput(String inputName, ReferenceMetadata reference) {
        Map<String, ReferenceInputField> fields = referenceInputs.computeIfAbsent(inputName,
                ignored -> new LinkedHashMap<>());
        for (String column : reference.targetColumns()) {
            fields.putIfAbsent(naming.fieldName(column), new ReferenceInputField("ID", "Database identifier"));
        }
        TableMetadata target = component.table(reference.targetTable());
        for (String column : reference.lookupColumns()) {
            ColumnMetadata metadata = target == null ? null : target.column(column);
            fields.putIfAbsent(naming.fieldName(column), new ReferenceInputField(
                    metadata == null ? "String" : metadata.graphqlScalar().typeName(),
                    metadata == null ? null : metadata.description()));
        }
    }

    private List<String> directives(TableMetadata table, ColumnMetadata column, boolean input) {
        List<String> result = new ArrayList<>();
        result.add("@column(name: " + quote(column.name()) + ")");
        if (column.source() != null) result.add("@source(value: " + quote(column.source().description()) + ")");
        if (!input && column.readOnly()) result.add("@readOnly(reason: " + quote(column.readOnlyReason()) + ")");
        if (column.reference() != null) result.add(referenceDirective(table, column));
        String constraint = constraintDirective(column);
        if (constraint != null) result.add(constraint);
        if (column.defaultValue() != null && column.defaultValue().normalizedValue() != null) {
            result.add("@defaultValue(value: " + quote(column.defaultValue().normalizedValue()) + ")");
        }
        return result;
    }

    private String referenceDirective(TableMetadata table, ColumnMetadata column) {
        ReferenceMetadata reference = column.reference();
        ForeignKeyMetadata key = table.foreignKeys()
                                      .stream()
                                      .filter(candidate -> candidate.sourceColumns().contains(column.name()))
                                      .findFirst().orElse(null);
        List<String> arguments = new ArrayList<>();
        arguments.add("table: " + quote(reference.targetTable()));
        arguments.add("column: " + quotedList(reference.targetColumns()));
        arguments.add("lookup: " + quotedList(reference.lookupColumns()));
        arguments.add("kind: " + reference.kind().name());
        if (key != null && key.composite()) arguments.add("compositeKey: true");
        if (key != null && key.uniqueIndex() != null) arguments.add("uniqueIndex: " + quote(key.uniqueIndex()));
        return "@reference(" + String.join(", ", arguments) + ")";
    }

    private static String constraintDirective(ColumnMetadata column) {
        ConstraintMetadata constraint = column.constraints();
        List<String> values = new ArrayList<>();
        if (column.maxLength() != null) values.add("maxLength: " + column.maxLength());
        if (constraint.minimum() != null) values.add("minimum: " + quote(constraint.minimum()));
        if (constraint.maximum() != null) values.add("maximum: " + quote(constraint.maximum()));
        if (constraint.notEqual() != null) values.add("notEqual: " + quote(constraint.notEqual()));
        if (constraint.pattern() != null) values.add("pattern: " + quote(constraint.pattern()));
        if (constraint.format() != null) values.add("format: " + quote(constraint.format()));
        if (column.precision() != null) values.add("precision: " + column.precision());
        if (column.scale() != null) values.add("scale: " + column.scale());
        return values.isEmpty() ? null : "@constraint(" + String.join(", ", values) + ")";
    }

    private String renderEnum(String name, List<StaticValueMetadata> values) {
        List<String> lines = new ArrayList<>();
        lines.add("enum " + name + " {");
        Set<String> usedNames = new LinkedHashSet<>();
        for (StaticValueMetadata value : values) {
            lines.add("  " + naming.enumValueName(value.name(), value.id(), usedNames) + " @dbValue(id: " + quote(value.id()) + ")");
        }
        lines.add("}");
        return String.join("\n", lines);
    }

    private static String renderReferenceInput(String name, Map<String, ReferenceInputField> fields) {
        List<String> lines = new ArrayList<>();
        lines.add("input " + name + " {");
        fields.forEach((field, metadata) -> addField(lines, field, metadata.type(), metadata.description(), List.of()));
        lines.add("}");
        return String.join("\n", lines);
    }

    private Set<String> physicalFieldNames(TableMetadata table) {
        Set<String> result = new LinkedHashSet<>();
        table.columns().forEach(column -> result.add(naming.fieldName(column.name())));
        return result;
    }

    private Set<String> parentSourceColumns(String tableName) {
        ComponentHierarchyNode node = hierarchyNode(component.hierarchy(), tableName);
        return node == null || node.parentRelation() == null ? Set.of()
                                                             : new LinkedHashSet<>(node.parentRelation().sourceColumns());
    }

    private static ComponentHierarchyNode hierarchyNode(ComponentHierarchyNode node, String tableName) {
        ComponentHierarchyNode result = node.tableName().equals(tableName) && !node.cycle() ? node : null;
        for (ComponentHierarchyNode child : node.children()) {
            if (result == null) result = hierarchyNode(child, tableName);
        }
        return result;
    }

    private TableMetadata requireTable(String tableName) {
        TableMetadata table = component.table(tableName);
        if (table == null) throw new IllegalStateException("Component table metadata was not found: " + tableName);
        return table;
    }

    private static boolean hasDynamicReference(ColumnMetadata column) {
        return column.reference() != null && column.reference().kind() == ReferenceKind.DYNAMIC
                && !column.reference().lookupColumns().isEmpty();
    }

    private static String referenceLookup(String field) {
        return "@referenceLookup(field: " + quote(field) + ")";
    }

    private static String relationDirective(RelationMetadata relation) {
        return "@relation(fromTable: " + quote(relation.sourceTable())
                + ", fromColumn: " + quote(relation.sourceColumns().get(0))
                + ", toTable: " + quote(relation.targetTable())
                + ", toColumn: " + quote(relation.targetColumns().get(0))
                + ", constraint: " + quote(relation.constraintName()) + ")";
    }

    private static String checkDirective(CheckConstraintMetadata check) {
        return "@check(name: " + quote(check.name()) + ", expression: " + quote(check.expression())
                + ", columns: " + quotedList(check.referencedColumns()) + ")";
    }

    private static void addField(List<String> lines, String name, String type,
                                 String description, List<String> directives) {
        description(lines, description, "  ");
        lines.add("  " + name + ": " + type
                + (directives.isEmpty() ? "" : " " + String.join(" ", directives)));
    }

    private static void description(List<String> lines, String value, String indent) {
        if (value != null && !value.isEmpty()) lines.add(indent + quote(value));
    }

    private static String quotedList(List<String> values) {
        return "[" + String.join(", ", values.stream().map(GraphqlSchemaRenderer::quote).toList()) + "]";
    }

    private static String quote(String value) {
        String safeValue = value == null ? "" : value;
        return '"' + safeValue.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\n", "\\n").replace("\r", "\\r") + '"';
    }

    private record ReferenceInputField(String type, String description) { }
}
