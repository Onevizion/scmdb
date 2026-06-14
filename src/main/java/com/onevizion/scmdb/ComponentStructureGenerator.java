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
                        Objects.equals(asString(row.get("table_name")), component.mainTable),
                        asInteger(row.get("bpd_item_type_id")),
                        asString(row.get("bpd_item_type"))
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
        putValue(node, "component_name_column", component.componentNameColumn);
        if (component.bpdItemTypeId != null) {
            putValue(node, "bpd_item_type_id", component.bpdItemTypeId);
            putValue(node, "bpd_item_type", component.bpdItemType);
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
            putBpdData(node, table);
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
        putBpdData(node, table);
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

    private void putBpdData(ObjectNode node, TableData table) {
        if (!table.mainTable || table.bpdItemTypeId == null) {
            return;
        }

        putValue(node, "bpd_item_type_id", table.bpdItemTypeId);
        putValue(node, "bpd_item_type", table.bpdItemType);
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
        private final String componentNameColumn;
        private final Integer bpdItemTypeId;
        private final String bpdItemType;
        private final List<TableData> tables = new ArrayList<>();

        private ComponentData(Map<String, Object> row) {
            componentId = asInteger(row.get("component_id"));
            componentName = asString(row.get("component"));
            mainTable = asString(row.get("main_table"));
            supportBpl = Objects.equals(asInteger(row.get("support_bpl")), 1);
            supportAudit = Objects.equals(asInteger(row.get("support_audit")), 1);
            componentNameColumn = asString(row.get("component_name_column"));
            bpdItemTypeId = asInteger(row.get("bpd_item_type_id"));
            bpdItemType = asString(row.get("bpd_item_type"));
        }
    }

    private record TableData(String tableName, boolean mainTable, Integer bpdItemTypeId, String bpdItemType) {
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

}
