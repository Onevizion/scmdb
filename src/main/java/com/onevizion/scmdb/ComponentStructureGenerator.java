package com.onevizion.scmdb;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.onevizion.scmdb.dao.DdlDao;
import com.onevizion.scmdb.vo.ComponentData;
import com.onevizion.scmdb.vo.ComponentRow;
import com.onevizion.scmdb.vo.ForeignKey;
import com.onevizion.scmdb.vo.TableData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.onevizion.scmdb.ColorLogger.Color.GREEN;

@Component
public class ComponentStructureGenerator {

    private static final String FIELD_COMPONENT = "component";
    private static final String FIELD_COMPONENT_ID = "component_id";
    private static final String FIELD_COMPONENT_NAME = "component_name";
    private static final String FIELD_MAIN_TABLE = "main_table";
    private static final String FIELD_SUPPORT_BPL = "support_bpl";
    private static final String FIELD_SUPPORT_AUDIT = "support_audit";
    private static final String FIELD_COMPONENT_NAME_COLUMN = "component_name_column";
    private static final String FIELD_BPD_ITEM_TYPE_ID = "bpd_item_type_id";
    private static final String FIELD_BPD_ITEM_TYPE = "bpd_item_type";
    private static final String FIELD_TABLES = "tables";
    private static final String FIELD_TABLE_NAME = "table_name";
    private static final String FIELD_IS_MAIN_TABLE = "is_main_table";
    private static final String FIELD_SCHEMA_FILE = "schema_file";
    private static final String FIELD_RELATIONSHIPS = "relationships";
    private static final String FIELD_RELATIONSHIP = "relationship";
    private static final String FIELD_HIERARCHY = "hierarchy";
    private static final String FIELD_CHILDREN = "children";
    private static final String FIELD_CIRCULAR_REFERENCE = "circular_reference";
    private static final String FIELD_TYPE = "type";
    private static final String FIELD_FROM_COLUMN = "from_column";
    private static final String FIELD_TO_TABLE = "to_table";
    private static final String FIELD_TO_COLUMN = "to_column";
    private static final String FIELD_CONSTRAINT_NAME = "constraint_name";
    private static final String FOREIGN_KEY_TYPE = "foreign_key";
    private static final String SCHEMA_FILE_SUFFIX = ".schema.json";

    private final JsonMapper mapper = JsonMapper.builder()
                                                .enable(SerializationFeature.INDENT_OUTPUT)
                                                .build();

    private final AppArguments appArguments;
    private final DdlDao ddlDao;
    private final ColorLogger logger;

    @Autowired
    public ComponentStructureGenerator(AppArguments appArguments, DdlDao ddlDao, ColorLogger logger) {
        this.appArguments = appArguments;
        this.ddlDao = ddlDao;
        this.logger = logger;
    }

    public void generate() {
        List<ComponentData> components = loadComponents();
        for (ComponentData component : components) {
            ObjectNode structure = buildComponentStructure(component);
            writeJson(componentFile(component), structure);
        }
        logger.info("Generated component structure files: {}", GREEN, components.size());
    }

    private List<ComponentData> loadComponents() {
        Map<Integer, List<ComponentRow>> rowsByComponent = ddlDao.loadComponentStructureRows()
                                                                 .stream()
                                                                 .filter(row -> row.componentId() != null)
                                                                 .collect(Collectors.groupingBy(
                                                                         ComponentRow::componentId,
                                                                         LinkedHashMap::new,
                                                                         Collectors.toList()));

        List<ComponentData> components = new ArrayList<>();
        for (List<ComponentRow> rows : rowsByComponent.values()) {
            components.add(toComponentData(rows));
        }
        return components;
    }

    private ComponentData toComponentData(List<ComponentRow> rows) {
        ComponentRow first = rows.get(0);

        List<TableData> tables = new ArrayList<>();
        for (ComponentRow row : rows) {
            if (row.componentTableId() != null) {
                tables.add(new TableData(row.tableName(),
                                         Objects.equals(row.tableName(), first.mainTable()),
                                         row.bpdItemTypeId(),
                                         row.bpdItemType()));
            }
        }
        tables.sort(Comparator.comparing((TableData table) -> !table.mainTable())
                              .thenComparing(TableData::tableName));

        return new ComponentData(first.componentId(),
                                 first.component(),
                                 first.mainTable(),
                                 Objects.equals(first.supportBpl(), 1),
                                 Objects.equals(first.supportAudit(), 1),
                                 first.componentNameColumn(),
                                 first.bpdItemTypeId(),
                                 first.bpdItemType(),
                                 tables);
    }

    private ObjectNode buildComponentStructure(ComponentData component) {
        List<TableData> tables = component.tables();
        Set<String> tableNames = tables.stream()
                                       .map(TableData::tableName)
                                       .collect(Collectors.toCollection(LinkedHashSet::new));
        Map<String, List<ForeignKey>> relationships = loadRelationships(tableNames);

        ObjectNode structure = mapper.createObjectNode();
        structure.set(FIELD_COMPONENT, componentNode(component));
        structure.set(FIELD_TABLES, tablesNode(tables));
        structure.set(FIELD_RELATIONSHIPS, relationshipsNode(relationships));
        structure.set(FIELD_HIERARCHY, hierarchyNode(component.mainTable(), tables, relationships, new LinkedHashSet<>()));
        return structure;
    }

    private Map<String, List<ForeignKey>> loadRelationships(Set<String> tableNames) {
        Map<String, List<ForeignKey>> relationships = new LinkedHashMap<>();
        for (String tableName : tableNames) {
            List<ForeignKey> tableRelationships = ddlDao.getTableForeignKeys(tableName)
                                                        .stream()
                                                        .filter(foreignKey -> tableNames.contains(foreignKey.toTable()))
                                                        .toList();
            if (!tableRelationships.isEmpty()) {
                relationships.put(tableName, tableRelationships);
            }
        }
        return relationships;
    }

    private ObjectNode componentNode(ComponentData component) {
        ObjectNode node = mapper.createObjectNode();
        putValue(node, FIELD_COMPONENT_ID, component.componentId());
        putValue(node, FIELD_COMPONENT_NAME, component.componentName());
        putValue(node, FIELD_MAIN_TABLE, component.mainTable());
        putValue(node, FIELD_SUPPORT_BPL, component.supportBpl());
        putValue(node, FIELD_SUPPORT_AUDIT, component.supportAudit());
        putValue(node, FIELD_COMPONENT_NAME_COLUMN, component.componentNameColumn());
        if (component.bpdItemTypeId() != null) {
            putValue(node, FIELD_BPD_ITEM_TYPE_ID, component.bpdItemTypeId());
            putValue(node, FIELD_BPD_ITEM_TYPE, component.bpdItemType());
        }
        return node;
    }

    private ArrayNode tablesNode(List<TableData> tables) {
        ArrayNode nodes = mapper.createArrayNode();
        for (TableData table : tables) {
            ObjectNode node = nodes.addObject();
            putValue(node, FIELD_TABLE_NAME, table.tableName());
            putValue(node, FIELD_IS_MAIN_TABLE, table.mainTable());
            putValue(node, FIELD_SCHEMA_FILE, getSchemaFileName(table.tableName()));
            putBpdData(node, table);
        }
        return nodes;
    }

    private ObjectNode relationshipsNode(Map<String, List<ForeignKey>> relationships) {
        ObjectNode node = mapper.createObjectNode();
        for (Map.Entry<String, List<ForeignKey>> entry : relationships.entrySet()) {
            ArrayNode relationshipNodes = node.putArray(entry.getKey());
            for (ForeignKey relationship : entry.getValue()) {
                relationshipNodes.add(relationshipNode(relationship));
            }
        }
        return node;
    }

    private ObjectNode hierarchyNode(String tableName, List<TableData> tables, Map<String, List<ForeignKey>> relationships,
                                     Set<String> visited) {
        if (visited.contains(tableName)) {
            ObjectNode circular = mapper.createObjectNode();
            putValue(circular, FIELD_TABLE_NAME, tableName);
            putValue(circular, FIELD_CIRCULAR_REFERENCE, true);
            return circular;
        }

        TableData table = tables.stream()
                                .filter(candidate -> Objects.equals(candidate.tableName(), tableName))
                                .findFirst()
                                .orElse(null);
        if (table == null) {
            return mapper.createObjectNode();
        }

        Set<String> nextVisited = new LinkedHashSet<>(visited);
        nextVisited.add(tableName);

        ObjectNode node = mapper.createObjectNode();
        putValue(node, FIELD_TABLE_NAME, table.tableName());
        putValue(node, FIELD_SCHEMA_FILE, getSchemaFileName(table.tableName()));
        putValue(node, FIELD_IS_MAIN_TABLE, table.mainTable());
        putBpdData(node, table);
        ArrayNode children = node.putArray(FIELD_CHILDREN);

        for (Map.Entry<String, List<ForeignKey>> entry : relationships.entrySet()) {
            String childTableName = entry.getKey();
            for (ForeignKey relationship : entry.getValue()) {
                if (Objects.equals(relationship.toTable(), tableName)) {
                    ObjectNode child = hierarchyNode(childTableName, tables, relationships, nextVisited);
                    child.set(FIELD_RELATIONSHIP, relationshipNode(relationship));
                    children.add(child);
                }
            }
        }
        return node;
    }

    private ObjectNode relationshipNode(ForeignKey relationship) {
        ObjectNode node = mapper.createObjectNode();
        putValue(node, FIELD_TYPE, FOREIGN_KEY_TYPE);
        putValue(node, FIELD_FROM_COLUMN, relationship.fromColumn());
        putValue(node, FIELD_TO_TABLE, relationship.toTable());
        putValue(node, FIELD_TO_COLUMN, relationship.toColumn());
        putValue(node, FIELD_CONSTRAINT_NAME, relationship.constraintName());
        return node;
    }

    private File componentFile(ComponentData component) {
        return new File(appArguments.getComponentStructuresDirectory(),
                        "component_" + component.componentId() + "_" + component.mainTable().toLowerCase(Locale.ROOT) +
                        ".json");
    }

    private String getSchemaFileName(String tableName) {
        return tableName.toLowerCase(Locale.ROOT) + SCHEMA_FILE_SUFFIX;
    }

    private void putValue(ObjectNode node, String fieldName, Object value) {
        node.set(fieldName, mapper.valueToTree(value));
    }

    private void putBpdData(ObjectNode node, TableData table) {
        if (!table.mainTable() || table.bpdItemTypeId() == null) {
            return;
        }

        putValue(node, FIELD_BPD_ITEM_TYPE_ID, table.bpdItemTypeId());
        putValue(node, FIELD_BPD_ITEM_TYPE, table.bpdItemType());
    }

    private void writeJson(File file, ObjectNode value) {
        try {
            mapper.writeValue(file, value);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write component structure file: " + file.getAbsolutePath(), e);
        }
    }

}
