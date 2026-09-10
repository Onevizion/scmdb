package com.onevizion.scmdb;

import com.onevizion.scmdb.dao.DdlDao;
import com.onevizion.scmdb.model.ColumnMetadata;
import com.onevizion.scmdb.model.ComponentHierarchyNode;
import com.onevizion.scmdb.model.ComponentMetadata;
import com.onevizion.scmdb.model.ForeignKeyMetadata;
import com.onevizion.scmdb.model.ReferenceKind;
import com.onevizion.scmdb.model.ReferenceMetadata;
import com.onevizion.scmdb.model.RelationMetadata;
import com.onevizion.scmdb.model.TableMetadata;
import com.onevizion.scmdb.vo.ComponentData;
import com.onevizion.scmdb.vo.ComponentRow;
import com.onevizion.scmdb.vo.TableData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Component
public class ComponentStructureGenerator {
    private final DdlDao ddlDao;
    private final DdlTableMetadataProvider ddlTableMetadataProvider;
    private final ColorLogger logger;

    @Autowired
    public ComponentStructureGenerator(DdlDao ddlDao,
                                       DdlTableMetadataProvider ddlTableMetadataProvider,
                                       ColorLogger logger) {
        this.ddlDao = ddlDao;
        this.ddlTableMetadataProvider = ddlTableMetadataProvider;
        this.logger = logger;
    }

    public BuildResult buildModels() {
        List<ComponentMetadata> models = new ArrayList<>();
        List<String> errors = new ArrayList<>();
        for (ComponentData component : loadComponents()) {
            if (component.tables().stream().noneMatch(table -> Objects.equals(table.tableName(), component.mainTable()))) {
                errors.add(componentLabel(component) + ": main table [" + component.mainTable()
                        + "] is not in component tables");
                continue;
            }
            try {
                models.add(buildModel(component));
            } catch (RuntimeException e) {
                errors.add(componentLabel(component) + ": " + e.getMessage());
            }
        }
        errors.forEach(error -> logger.warn(error, ColorLogger.Color.YELLOW));
        logger.info("Component metadata assembled: {}, errors={}", ColorLogger.Color.GREEN, models.size(), errors.size());
        return new BuildResult(models, errors);
    }

    private static String componentLabel(ComponentData component) {
        return "Component " + component.componentId() + " (" + component.componentName() + ")";
    }

    public record BuildResult(List<ComponentMetadata> models, List<String> errors) { }

    private List<ComponentData> loadComponents() {
        boolean hasBpdItemTypeId = ddlDao.hasColumnInTable("V_COMPONENT", "BPD_ITEM_TYPE_ID");
        Map<Integer, List<ComponentRow>> rowsByComponent = ddlDao.findComponentRows(hasBpdItemTypeId)
                .stream()
                .filter(row -> row.componentId() != null)
                .collect(Collectors.groupingBy(ComponentRow::componentId, LinkedHashMap::new, Collectors.toList()));
        return rowsByComponent.values().stream().map(this::toComponentData).toList();
    }

    private ComponentData toComponentData(List<ComponentRow> rows) {
        ComponentRow first = rows.get(0);
        List<TableData> tables = rows.stream()
                .filter(row -> row.componentTableId() != null)
                .map(row -> new TableData(row.tableName(), Objects.equals(row.tableName(), first.mainTable()),
                        row.bpdItemTypeId(), row.bpdItemType()))
                .sorted(Comparator.comparing((TableData table) -> !table.mainTable()).thenComparing(TableData::tableName))
                .toList();
        return new ComponentData(first.componentId(), first.component(), first.mainTable(),
                Objects.equals(first.supportBpl(), 1), Objects.equals(first.supportAudit(), 1),
                first.componentNameColumn(), first.bpdItemTypeId(), first.bpdItemType(), tables);
    }

    private ComponentMetadata buildModel(ComponentData component) {
        Set<String> componentTables = component.tables().stream()
                .map(TableData::tableName)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        Map<String, TableMetadata> tables = new LinkedHashMap<>();
        for (String tableName : componentTables) {
            loadTableAndReferences(tableName, tables, new LinkedHashSet<>());
        }
        for (String tableName : componentTables) {
            if (!tables.containsKey(tableName)) {
                throw new IllegalStateException("Canonical DDL was not found for table [" + tableName + "]");
            }
        }
        Map<String, TableMetadata> enrichedTables = attachReferences(tables);
        ComponentHierarchyNode hierarchy = hierarchyNode(component.mainTable(), componentTables,
                enrichedTables, new LinkedHashSet<>(), null);
        return new ComponentMetadata(component.componentId(), component.componentName(), component.mainTable(),
                new ArrayList<>(enrichedTables.values()), hierarchy);
    }

    private void loadTableAndReferences(String tableName, Map<String, TableMetadata> tables, Set<String> loading) {
        if (tables.containsKey(tableName) || !loading.add(tableName)) {
            return;
        }
        TableMetadata table = ddlTableMetadataProvider.load(tableName);
        if (table == null) {
            return;
        }
        tables.put(tableName, table);
        for (ForeignKeyMetadata foreignKey : table.foreignKeys()) {
            loadTableAndReferences(foreignKey.targetTable(), tables, loading);
        }
        loading.remove(tableName);
    }

    private static Map<String, TableMetadata> attachReferences(Map<String, TableMetadata> tables) {
        Map<String, TableMetadata> result = new LinkedHashMap<>();
        tables.forEach((tableName, table) -> {
            List<ColumnMetadata> columns = table.columns().stream()
                    .map(column -> attachReference(column, table, tables))
                    .toList();
            result.put(tableName, new TableMetadata(table.name(), table.description(), columns,
                    table.primaryKey(), table.foreignKeys(), table.checks(), table.referenceData()));
        });
        return result;
    }

    private static ColumnMetadata attachReference(ColumnMetadata column, TableMetadata source,
                                                   Map<String, TableMetadata> tables) {
        ForeignKeyMetadata foreignKey = source.foreignKeys().stream()
                .filter(key -> key.sourceColumns().contains(column.name()))
                .findFirst()
                .orElse(null);
        if (foreignKey == null) {
            return column;
        }
        TableMetadata target = tables.get(foreignKey.targetTable());
        ReferenceMetadata targetReference = target == null ? null : target.referenceData();
        ReferenceMetadata reference = targetReference == null
                ? new ReferenceMetadata(ReferenceKind.DYNAMIC, foreignKey.targetTable(),
                        foreignKey.targetColumns(), List.of(), List.of())
                : new ReferenceMetadata(targetReference.kind(), foreignKey.targetTable(),
                        foreignKey.targetColumns(), targetReference.lookupColumns(), targetReference.staticValues());
        return new ColumnMetadata(column.name(), column.oracleType(), column.graphqlScalar(), column.nullable(),
                column.precision(), column.scale(), column.maxLength(), column.description(), column.defaultValue(),
                column.source(), column.readOnly(), column.readOnlyReason(), column.constraints(), reference,
                column.labelReference());
    }

    private static ComponentHierarchyNode hierarchyNode(String tableName, Set<String> componentTables,
                                                        Map<String, TableMetadata> tables, Set<String> visited,
                                                        RelationMetadata parentRelation) {
        boolean cycle = visited.contains(tableName);
        if (cycle) {
            return new ComponentHierarchyNode(tableName, false, true,
                    cycleRelation(parentRelation), List.of());
        }
        TableMetadata table = tables.get(tableName);
        if (table == null) {
            throw new IllegalStateException("Canonical DDL was not found for hierarchy table [" + tableName + "]");
        }
        Set<String> nextVisited = new LinkedHashSet<>(visited);
        nextVisited.add(tableName);
        List<ComponentHierarchyNode> children = new ArrayList<>();
        for (String childTableName : componentTables) {
            TableMetadata childTable = tables.get(childTableName);
            if (childTable == null) {
                continue;
            }
            for (ForeignKeyMetadata foreignKey : childTable.foreignKeys()) {
                if (foreignKey.targetTable().equals(tableName)) {
                    RelationMetadata relation = relation(foreignKey, true, nextVisited.contains(childTableName));
                    children.add(hierarchyNode(childTableName, componentTables, tables, nextVisited, relation));
                }
            }
        }
        return new ComponentHierarchyNode(tableName, visited.isEmpty(), false, parentRelation, children);
    }

    private static RelationMetadata relation(ForeignKeyMetadata foreignKey, boolean containment, boolean cycle) {
        return new RelationMetadata(foreignKey.sourceTable(), foreignKey.sourceColumns(),
                foreignKey.targetTable(), foreignKey.targetColumns(), foreignKey.constraintName(), containment, cycle);
    }

    private static RelationMetadata cycleRelation(RelationMetadata relation) {
        return relation == null ? null : new RelationMetadata(relation.sourceTable(), relation.sourceColumns(),
                relation.targetTable(), relation.targetColumns(), relation.constraintName(), relation.containment(), true);
    }
}
