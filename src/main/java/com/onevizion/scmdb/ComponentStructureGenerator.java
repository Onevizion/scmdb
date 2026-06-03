package com.onevizion.scmdb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.onevizion.scmdb.dao.DdlDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static com.onevizion.scmdb.ColorLogger.Color.GREEN;

@Component
public class ComponentStructureGenerator {

    @Autowired
    private AppArguments appArguments;

    @Autowired
    private DdlDao ddlDao;

    @Autowired
    private ColorLogger logger;

    private final ObjectMapper mapper = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);

    public void generate() {
        Map<Integer, ComponentData> components = loadComponents();
        for (ComponentData component : components.values()) {
            ObjectNode structure = buildComponentStructure(component);
            writeJson(componentFile(component), structure);
        }
        logger.info("Generated component structure files: {}", GREEN, components.size());
    }

    private Map<Integer, ComponentData> loadComponents() {
        Map<Integer, ComponentData> components = new LinkedHashMap<>();
        for (Map<String, Object> row : ddlDao.loadComponentStructureRows()) {
            Integer componentId = asInteger(row.get("component_id"));
            if (componentId == null) {
                continue;
            }

            ComponentData component = components.computeIfAbsent(componentId, id -> new ComponentData(row));
            if (row.get("component_table_id") != null) {
                component.tables.add(new TableData(
                        asString(row.get("table_name")),
                        Objects.equals(asString(row.get("table_name")), component.mainTable)
                ));
            }
        }

        for (ComponentData component : components.values()) {
            component.tables.sort(Comparator.comparing((TableData table) -> !table.mainTable)
                                            .thenComparing(table -> table.tableName));
        }
        return components;
    }

    private ObjectNode buildComponentStructure(ComponentData component) {
        List<TableData> tables = component.tables;
        Set<String> tableNames = tables.stream()
                                       .map(table -> table.tableName)
                                       .collect(Collectors.toCollection(LinkedHashSet::new));
        Map<String, List<RelationshipData>> relationships = loadRelationships(tableNames);

        ObjectNode structure = mapper.createObjectNode();
        structure.set("component", componentNode(component));
        structure.set("tables", tablesNode(tables));
        structure.set("relationships", relationshipsNode(relationships));
        structure.set("hierarchy", hierarchyNode(component.mainTable, tables, relationships, new LinkedHashSet<>()));
        return structure;
    }

    private Map<String, List<RelationshipData>> loadRelationships(Set<String> tableNames) {
        Map<String, List<RelationshipData>> relationships = new LinkedHashMap<>();
        for (String tableName : tableNames) {
            List<RelationshipData> tableRelationships = ddlDao.getTableForeignKeys(tableName)
                                                              .stream()
                                                              .filter(row -> tableNames.contains(asString(row.get("to_table"))))
                                                              .map(RelationshipData::new)
                                                              .toList();
            if (!tableRelationships.isEmpty()) {
                relationships.put(tableName, tableRelationships);
            }
        }
        return relationships;
    }

    private ObjectNode componentNode(ComponentData component) {
        ObjectNode node = mapper.createObjectNode();
        putValue(node, "component_id", component.componentId);
        putValue(node, "component_name", component.componentName);
        putValue(node, "main_table", component.mainTable);
        putValue(node, "support_bpl", component.supportBpl);
        putValue(node, "support_audit", component.supportAudit);

        BpdData bpdData = BpdData.BY_COMPONENT_ID.get(component.componentId);
        if (bpdData != null) {
            putValue(node, "bpd_item_type_id", bpdData.itemTypeId);
            putValue(node, "bpd_item_type", bpdData.itemType);
        }
        return node;
    }

    private ArrayNode tablesNode(List<TableData> tables) {
        ArrayNode nodes = mapper.createArrayNode();
        for (TableData table : tables) {
            ObjectNode node = nodes.addObject();
            putValue(node, "table_name", table.tableName);
            putValue(node, "is_main_table", table.mainTable);
            putValue(node, "schema_file", schemaFileName(table.tableName));
        }
        return nodes;
    }

    private ObjectNode relationshipsNode(Map<String, List<RelationshipData>> relationships) {
        ObjectNode node = mapper.createObjectNode();
        for (Map.Entry<String, List<RelationshipData>> entry : relationships.entrySet()) {
            ArrayNode relationshipNodes = node.putArray(entry.getKey());
            for (RelationshipData relationship : entry.getValue()) {
                relationshipNodes.add(relationshipNode(relationship));
            }
        }
        return node;
    }

    private ObjectNode hierarchyNode(String tableName, List<TableData> tables, Map<String, List<RelationshipData>> relationships,
                                     Set<String> visited) {
        if (visited.contains(tableName)) {
            ObjectNode circular = mapper.createObjectNode();
            putValue(circular, "table_name", tableName);
            putValue(circular, "circular_reference", true);
            return circular;
        }

        TableData table = tables.stream()
                                .filter(candidate -> Objects.equals(candidate.tableName, tableName))
                                .findFirst()
                                .orElse(null);
        if (table == null) {
            return mapper.createObjectNode();
        }

        Set<String> nextVisited = new LinkedHashSet<>(visited);
        nextVisited.add(tableName);

        ObjectNode node = mapper.createObjectNode();
        putValue(node, "table_name", table.tableName);
        putValue(node, "schema_file", schemaFileName(table.tableName));
        putValue(node, "is_main_table", table.mainTable);
        ArrayNode children = node.putArray("children");

        for (Map.Entry<String, List<RelationshipData>> entry : relationships.entrySet()) {
            String childTableName = entry.getKey();
            for (RelationshipData relationship : entry.getValue()) {
                if (Objects.equals(relationship.toTable, tableName)) {
                    ObjectNode child = hierarchyNode(childTableName, tables, relationships, nextVisited);
                    child.set("relationship", relationshipNode(relationship));
                    children.add(child);
                }
            }
        }
        return node;
    }

    private ObjectNode relationshipNode(RelationshipData relationship) {
        ObjectNode node = mapper.createObjectNode();
        putValue(node, "type", "foreign_key");
        putValue(node, "from_column", relationship.fromColumn);
        putValue(node, "to_table", relationship.toTable);
        putValue(node, "to_column", relationship.toColumn);
        putValue(node, "constraint_name", relationship.constraintName);
        return node;
    }

    private File componentFile(ComponentData component) {
        return new File(appArguments.getComponentStructuresDirectory(),
                        "component_" + component.componentId + "_" + component.mainTable.toLowerCase(Locale.ROOT) + ".json");
    }

    private String schemaFileName(String tableName) {
        return tableName.toLowerCase(Locale.ROOT) + ".schema.json";
    }

    private void putValue(ObjectNode node, String fieldName, Object value) {
        node.set(fieldName, mapper.valueToTree(value));
    }

    private Integer asInteger(Object value) {
        if (value instanceof Number number) {
            return number.intValue();
        }
        return value == null ? null : Integer.valueOf(String.valueOf(value));
    }

    private String asString(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private void writeJson(File file, ObjectNode value) {
        try {
            mapper.writeValue(file, value);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write component structure file: " + file.getAbsolutePath(), e);
        }
    }

    private class ComponentData {
        private final Integer componentId;
        private final String componentName;
        private final String mainTable;
        private final boolean supportBpl;
        private final boolean supportAudit;
        private final List<TableData> tables = new ArrayList<>();

        private ComponentData(Map<String, Object> row) {
            componentId = asInteger(row.get("component_id"));
            componentName = asString(row.get("component"));
            mainTable = asString(row.get("main_table"));
            supportBpl = Objects.equals(asInteger(row.get("support_bpl")), 1);
            supportAudit = Objects.equals(asInteger(row.get("support_audit")), 1);
        }
    }

    private record TableData(String tableName, boolean mainTable) {
    }

    private class RelationshipData {
        private final String constraintName;
        private final String fromColumn;
        private final String toTable;
        private final String toColumn;

        private RelationshipData(Map<String, Object> row) {
            constraintName = asString(row.get("constraint_name"));
            fromColumn = asString(row.get("from_column"));
            toTable = asString(row.get("to_table"));
            toColumn = asString(row.get("to_column"));
        }
    }

    private record BpdData(Integer itemTypeId, String itemType) {
        private static final Map<Integer, BpdData> BY_COMPONENT_ID = Map.ofEntries(
                Map.entry(1, new BpdData(6, "Import")),
                Map.entry(3, new BpdData(4, "Report")),
                Map.entry(4, new BpdData(9, "Rule")),
                Map.entry(5, new BpdData(31, "Config Field")),
                Map.entry(6, new BpdData(32, "Config V_Table")),
                Map.entry(10, new BpdData(46, "Trackor Tour")),
                Map.entry(11, new BpdData(34, "Trackor Type")),
                Map.entry(12, new BpdData(35, "Trackor Tree")),
                Map.entry(13, new BpdData(8, "Security Role")),
                Map.entry(15, new BpdData(33, "Trackor Class")),
                Map.entry(16, new BpdData(32, "Config V_Table")),
                Map.entry(17, new BpdData(37, "WorkFlow")),
                Map.entry(20, new BpdData(39, "Portal View")),
                Map.entry(22, new BpdData(40, "Notification")),
                Map.entry(26, new BpdData(38, "Trackor Form")),
                Map.entry(27, new BpdData(42, "DB Package")),
                Map.entry(41, new BpdData(49, "Dashboard")),
                Map.entry(48, new BpdData(34, "Trackor Type")),
                Map.entry(56, new BpdData(50, "Widget Panel")),
                Map.entry(57, new BpdData(51, "Widget"))
        );
    }
}
