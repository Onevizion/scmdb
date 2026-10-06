package com.onevizion.scmdb;

import com.onevizion.scmdb.dao.DdlDao;
import com.onevizion.scmdb.exception.ScmdbException;
import com.onevizion.scmdb.model.ColumnMetadata;
import com.onevizion.scmdb.model.ComponentHierarchyNode;
import com.onevizion.scmdb.model.ComponentMetadata;
import com.onevizion.scmdb.model.ConstraintMetadata;
import com.onevizion.scmdb.model.GraphqlScalar;
import com.onevizion.scmdb.model.RelationMetadata;
import com.onevizion.scmdb.model.StaticValueMetadata;
import com.onevizion.scmdb.model.TableMetadata;
import com.onevizion.scmdb.vo.ComponentRow;
import graphql.Scalars;
import graphql.schema.GraphQLScalarType;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphqlSchemaGeneratorIntegrationTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void generatesParsesMergesAndLoadsSdl() throws Exception {
        AppArguments arguments = arguments("generation");
        Path tables = Files.createDirectories(arguments.getDdlsDirectory().toPath().resolve("tables"));
        Files.writeString(tables.resolve("widget.sql"), """
                CREATE TABLE WIDGET (
                  WIDGET_ID NUMBER NOT NULL,
                  WIDGET_NAME VARCHAR2(100) NOT NULL,
                  IS_ACTIVE NUMBER DEFAULT ON NULL 1 NOT NULL,
                  PROGRAM_ID NUMBER NOT NULL,
                  CONSTRAINT PK_WIDGET PRIMARY KEY (WIDGET_ID),
                  CONSTRAINT CHK_WIDGET CHECK (IS_ACTIVE IN (0, 1))
                );
                CREATE OR REPLACE TRIGGER BI_WIDGET BEFORE INSERT ON WIDGET FOR EACH ROW BEGIN
                  IF :NEW.WIDGET_ID IS NULL THEN :NEW.WIDGET_ID := SEQ_WIDGET_ID.NEXTVAL; END IF;
                  IF :NEW.PROGRAM_ID IS NULL THEN :NEW.PROGRAM_ID := CURRENT_PROGRAM_ID(); END IF;
                END;
                COMMENT ON TABLE WIDGET IS 'Widget definition';
                COMMENT ON COLUMN WIDGET.WIDGET_NAME IS 'Widget name';
                """);
            writeSimpleTable(tables, "CONFIG_FIELD");
            writeSimpleTable(tables, "REPORT_PARAMS");
            writeSimpleTable(tables, "XITOR_TYPE");
            writeSimpleTable(tables, "WORK_PLAN");
        FixtureDdlDao ddlDao = new FixtureDdlDao(List.of(
                componentRow(57, "Widget", "WIDGET"),
                componentRow(58, "ConfigField", "CONFIG_FIELD"),
                componentRow(59, "ReportParams", "REPORT_PARAMS"),
                componentRow(60, "XitorType", "XITOR_TYPE"),
                componentRow(61, "WorkPlan", "WORK_PLAN")));
        SilentColorLogger logger = new SilentColorLogger();
        DdlTableMetadataProvider provider = new DdlTableMetadataProvider(arguments, ddlDao, logger);
        ComponentStructureGenerator structures = new ComponentStructureGenerator(ddlDao, provider, logger);

        GraphqlSchemaGenerator.GenerationResult result = new GraphqlSchemaGenerator(arguments,
                                                                                    structures,
                                                                                    new GraphqlNamingService(),
                                                                                    logger).generateAll();

        assertEquals(5, result.generated());
        Path outputDirectory = arguments.getGraphqlSchemasDirectory().toPath();
        List<Path> files;
        try (var paths = Files.list(outputDirectory)) {
            files = paths.filter(path -> path.toString().endsWith(".graphql"))
                         .sorted(Comparator.comparing(Path::toString)).toList();
        }
        assertEquals(5, files.size());
        assertFalse(Files.exists(outputDirectory.resolve("schema.graphql")));
        SchemaParser parser = new SchemaParser();
        for (Path file : files) {
            TypeDefinitionRegistry parsed = parser.parse(file.toFile());
            parsed.merge(parser.parse("type Query { schemaHealth: Boolean }"));
            new SchemaGenerator().makeExecutableSchema(parsed, scalarWiring());
        }
        TypeDefinitionRegistry merged = GraphqlSchemaTestUtils.merge(files);
        merged.merge(parser.parse("type Query { schemaHealth: Boolean }"));
        new SchemaGenerator().makeExecutableSchema(merged, scalarWiring());

        String sdl = Files.readString(outputDirectory.resolve("component_57_widget.graphql"));
        assertTrue(sdl.contains("widgetName: String! @column(name: \"WIDGET_NAME\") @constraint(maxLength: 100)"));
        assertTrue(sdl.contains("widgetId: ID! @column(name: \"WIDGET_ID\") @source(value: \"Trigger: auto-increment from SEQ_WIDGET_ID\")"));
        assertTrue(sdl.contains("isActive: Boolean! @column(name: \"IS_ACTIVE\") @source(value: \"DEFAULT ON NULL 1\")"));
        assertTrue(sdl.contains("scalar BigInt"));
        assertTrue(sdl.contains("directive @column("));
        assertFalse(sdl.contains("JsonNode"));
        assertTrue(Files.readString(outputDirectory.resolve("component_58_config_field.graphql"))
                        .contains("type ConfigField"));
        assertTrue(Files.readString(outputDirectory.resolve("component_59_report_params.graphql"))
                        .contains("type ReportParams"));
        assertTrue(Files.readString(outputDirectory.resolve("component_60_xitor_type.graphql"))
                        .contains("type XitorType"));
        assertTrue(Files.readString(outputDirectory.resolve("component_61_work_plan.graphql"))
                        .contains("type WorkPlan"));
    }

    @Test
    void failsTheWholeCommandWhenAnyComponentCannotBeBuiltButKeepsSuccessfulOutput() throws Exception {
        AppArguments arguments = arguments("failure");
        ComponentMetadata valid = component(1, "VALID", false);
        ComponentMetadata invalid = component(2, "INVALID", true);
        ComponentStructureGenerator structures = new ComponentStructureGenerator(
                                                                                 new FixtureDdlDao(List.of()),
                                                                                 null,
                                                                                 new SilentColorLogger()) {
            @Override
            public BuildResult buildModels() {
                return new BuildResult(List.of(valid, invalid), List.of());
            }
        };
        RecordingColorLogger logger = new RecordingColorLogger();

        GraphqlSchemaGenerator generator = new GraphqlSchemaGenerator(arguments, structures,
                                                                      new GraphqlNamingService(),
                                                                      logger);

        ScmdbException exception = assertThrows(ScmdbException.class, generator::generateAll);

        assertTrue(exception.getMessage().contains("failed for 1 component(s)"));
        assertFalse(exception.getMessage().contains("Component 2 (INVALID)"));
        assertEquals(1, logger.warnings.stream().filter(warning -> warning.contains("Component 2 (INVALID)")).count());
        assertTrue(Files.isRegularFile(arguments.getGraphqlSchemasDirectory()
                                                .toPath()
                                                .resolve("component_1_valid.graphql")));
        assertFalse(Files.exists(arguments.getGraphqlSchemasDirectory()
                                          .toPath()
                                          .resolve("component_2_invalid.graphql")));
    }

    @Test
    void collectsErrorsFromEveryFailingComponentInsteadOfStoppingAtTheFirst() throws Exception {
        AppArguments arguments = arguments("multi-failure");
        ComponentMetadata valid = component(1, "VALID", false);
        ComponentMetadata firstInvalid = component(2, "FIRST_INVALID", true);
        ComponentMetadata secondInvalid = component(3, "SECOND_INVALID", true);
        ComponentStructureGenerator structures = new ComponentStructureGenerator(new FixtureDdlDao(List.of()),
                                                                                 null,
                                                                                 new SilentColorLogger()) {
            @Override
            public BuildResult buildModels() {
                return new BuildResult(List.of(valid, firstInvalid, secondInvalid), List.of());
            }
        };
        RecordingColorLogger logger = new RecordingColorLogger();

        GraphqlSchemaGenerator generator = new GraphqlSchemaGenerator(arguments, structures,
                                                                      new GraphqlNamingService(),
                                                                      logger);

        ScmdbException exception = assertThrows(ScmdbException.class, generator::generateAll);

        assertTrue(exception.getMessage().contains("failed for 2 component(s)"));
        assertTrue(logger.warnings.stream().anyMatch(warning -> warning.contains("Component 2 (FIRST_INVALID)")));
        assertTrue(logger.warnings.stream().anyMatch(warning -> warning.contains("Component 3 (SECOND_INVALID)")));
        assertTrue(Files.isRegularFile(arguments.getGraphqlSchemasDirectory()
                                                .toPath()
                                                .resolve("component_1_valid.graphql")));
    }

    @Test
    void failsWhenAComponentIsSkippedBecauseMainTableIsMissingFromComponentTables() throws Exception {
        AppArguments arguments = arguments("missing-main-table");
        FixtureDdlDao ddlDao = new FixtureDdlDao(List.of(
                new ComponentRow(3, "Missing", "MISSING_TABLE", 0, 0, null, null, null, null, null)));
        RecordingColorLogger logger = new RecordingColorLogger();
        ComponentStructureGenerator structures = new ComponentStructureGenerator(ddlDao,
                new DdlTableMetadataProvider(arguments, ddlDao, logger), logger);
        GraphqlSchemaGenerator generator = new GraphqlSchemaGenerator(arguments, structures,
                                                                      new GraphqlNamingService(), logger);

        ScmdbException exception = assertThrows(ScmdbException.class, generator::generateAll);

        assertTrue(exception.getMessage().contains("failed for 1 component(s)"));
        assertEquals(1, logger.warnings.stream()
                .filter(warning -> warning.contains("Component 3 (Missing)") && warning.contains("MISSING_TABLE"))
                .count());
    }

    @Test
    void regeneratesOnlyComponentsAffectedThroughTheirTableDependencies() throws Exception {
        AppArguments arguments = arguments("incremental");
        Path tables = Files.createDirectories(arguments.getDdlsDirectory().toPath().resolve("tables"));
        Files.writeString(tables.resolve("dependent.sql"), """
                CREATE TABLE DEPENDENT (
                  DEPENDENT_ID NUMBER NOT NULL,
                  SHARED_LOOKUP_ID NUMBER,
                  CONSTRAINT PK_DEPENDENT PRIMARY KEY (DEPENDENT_ID),
                  CONSTRAINT FK_DEPENDENT_LOOKUP FOREIGN KEY (SHARED_LOOKUP_ID)
                    REFERENCES SHARED_LOOKUP (SHARED_LOOKUP_ID)
                );
                """);
        writeSimpleTable(tables, "SHARED_LOOKUP");
        writeSimpleTable(tables, "UNRELATED");
        FixtureDdlDao ddlDao = new FixtureDdlDao(List.of(componentRow(1, "Dependent", "DEPENDENT"),
                                                         componentRow(2, "Unrelated", "UNRELATED")));
        SilentColorLogger logger = new SilentColorLogger();
        DdlTableMetadataProvider provider = new DdlTableMetadataProvider(arguments, ddlDao, logger);
        ComponentStructureGenerator structures = new ComponentStructureGenerator(ddlDao, provider, logger);
        GraphqlSchemaGenerator generator = new GraphqlSchemaGenerator(arguments, structures,
                                                                      new GraphqlNamingService(), logger);
        Path outputDirectory = arguments.getGraphqlSchemasDirectory().toPath();
        Files.createDirectories(outputDirectory);
        Path dependentOutput = outputDirectory.resolve("component_1_old_name.graphql");
        Path unrelatedOutput = outputDirectory.resolve("component_2_unrelated.graphql");
        Path unrelatedAuxiliaryFile = outputDirectory.resolve("custom.graphql");
        Files.writeString(dependentOutput, "stale dependent schema");
        Files.writeString(unrelatedOutput, "unrelated schema");
        Files.writeString(unrelatedAuxiliaryFile, "custom schema");

        GraphqlSchemaGenerator.GenerationResult result = generator.generateAffected(Set.of("shared_lookup"));

        assertEquals(1, result.generated());
        assertFalse(Files.exists(dependentOutput));
        assertTrue(Files.readString(outputDirectory.resolve("component_1_dependent.graphql")).contains("type Dependent"));
        assertEquals("unrelated schema", Files.readString(unrelatedOutput));
        assertEquals("custom schema", Files.readString(unrelatedAuxiliaryFile));
    }

    @Test
    void doesNothingWhenNoChangedTablesWereResolved() throws Exception {
        AppArguments arguments = arguments("no-changes");
        ComponentStructureGenerator structures = new ComponentStructureGenerator(
                                                                                 new FixtureDdlDao(List.of()),
                                                                                 null, new SilentColorLogger()) {
            @Override
            public BuildResult buildModels() {
                throw new AssertionError("Models must not be loaded when there are no changed tables");
            }
        };
        GraphqlSchemaGenerator generator = new GraphqlSchemaGenerator(arguments, structures,
                new GraphqlNamingService(), new SilentColorLogger());
        Path output = arguments.getGraphqlSchemasDirectory().toPath().resolve("existing.graphql");
        Files.createDirectories(output.getParent());
        Files.writeString(output, "existing schema");

        GraphqlSchemaGenerator.GenerationResult result = generator.generateAffected(Set.of());

        assertEquals(0, result.generated());
        assertEquals("existing schema", Files.readString(output));
    }

    @Test
    void assemblesContainmentHierarchyAndTerminatesSameComponentCycles() throws Exception {
        AppArguments arguments = arguments("cycles");
        Path tables = Files.createDirectories(arguments.getDdlsDirectory().toPath().resolve("tables"));
        Files.writeString(tables.resolve("parent.sql"), """
                CREATE TABLE PARENT (
                  PARENT_ID NUMBER NOT NULL,
                  CHILD_ID NUMBER,
                  CONSTRAINT PK_PARENT PRIMARY KEY (PARENT_ID),
                  CONSTRAINT FK_PARENT_CHILD FOREIGN KEY (CHILD_ID) REFERENCES CHILD (CHILD_ID)
                );
                """);
        Files.writeString(tables.resolve("child.sql"), """
                CREATE TABLE CHILD (
                  CHILD_ID NUMBER NOT NULL,
                  PARENT_ID NUMBER NOT NULL,
                  CONSTRAINT PK_CHILD PRIMARY KEY (CHILD_ID),
                  CONSTRAINT FK_CHILD_PARENT FOREIGN KEY (PARENT_ID) REFERENCES PARENT (PARENT_ID)
                );
                """);
        FixtureDdlDao ddlDao = new FixtureDdlDao(List.of(new ComponentRow(9, "Cycle", "PARENT", 0, 0, null, 1, "PARENT", null, null),
                                                         new ComponentRow(9, "Cycle", "PARENT", 0, 0, null, 2, "CHILD", null, null)));
        DdlTableMetadataProvider provider = new DdlTableMetadataProvider(arguments, ddlDao, new SilentColorLogger());

        ComponentMetadata component = new ComponentStructureGenerator(ddlDao, provider,
                                                                      new SilentColorLogger()).buildModels().models().get(0);

        assertEquals("CHILD", component.hierarchy().children().get(0).tableName());
        assertTrue(component.hierarchy().children().get(0).children().get(0).cycle());
        assertTrue(component.hierarchy().children().get(0).parentRelation().containment());
    }

    private AppArguments arguments(String name) throws Exception {
        Path db = temporaryDirectory.resolve(name).resolve("db");
        Path scripts = Files.createDirectories(db.resolve("scripts"));
        Files.createDirectories(db.resolve("ddl"));
        AppArguments arguments = new AppArguments();
        arguments.parse(new String[] {"--owner-schema=test/test@localhost:1521/ORCLCDB",
                                      "--scripts-dir=" + scripts,
                                      "--gen-comps-schema"
        }, false);
        return arguments;
    }

    private static void writeSimpleTable(Path tables, String tableName) throws Exception {
        Files.writeString(tables.resolve(tableName.toLowerCase(Locale.ROOT) + ".sql"),
                "CREATE TABLE " + tableName + " (" + tableName + "_ID NUMBER NOT NULL, "
                        + tableName + "_NAME VARCHAR2(100), CONSTRAINT PK_" + tableName
                        + " PRIMARY KEY (" + tableName + "_ID));");
    }

    private static ComponentRow componentRow(int id, String componentName, String tableName) {
        return new ComponentRow(id, componentName, tableName, 0, 0, null,
                                id, tableName, null, null);
    }

    private static ComponentMetadata component(int id, String name, boolean invalidRelation) {
        ColumnMetadata idColumn = new ColumnMetadata(name + "_ID", "NUMBER", GraphqlScalar.ID,
                                                     false, null, null, null, null,
                                                     null, null, false, null, ConstraintMetadata.empty(), null, null);
        TableMetadata table = new TableMetadata(name, null, List.of(idColumn), List.of(idColumn.name()),
                                                List.of(), List.of(), null);
        if (!invalidRelation) {
            return new ComponentMetadata(id, name, name, List.of(table),
                    new ComponentHierarchyNode(name, true, false, null, List.of()));
        }
        RelationMetadata brokenRelation = new RelationMetadata(name, List.of(), name,
                                                               List.of(), "BROKEN", true, false);
        ComponentHierarchyNode child = new ComponentHierarchyNode(name, false, false, brokenRelation, List.of());
        return new ComponentMetadata(id, name, name, List.of(table),
                                     new ComponentHierarchyNode(name, true, false, null, List.of(child)));
    }

    private static RuntimeWiring scalarWiring() {
        RuntimeWiring.Builder wiring = RuntimeWiring.newRuntimeWiring();
        for (String name : List.of("BigInt", "Decimal", "Date", "DateTime")) {
            GraphQLScalarType scalar = GraphQLScalarType.newScalar(Scalars.GraphQLString).name(name).build();
            wiring.scalar(scalar);
        }
        return wiring.build();
    }

    private static class FixtureDdlDao extends DdlDao {
        private final List<ComponentRow> componentRows;

        private FixtureDdlDao(List<ComponentRow> componentRows) {
            this.componentRows = componentRows;
        }

        @Override
        public boolean hasColumnInTable(String tableName, String columnName) {
            return false;
        }

        @Override
        public List<ComponentRow> findComponentRows(boolean hasBpdItemTypeId) {
            return componentRows;
        }

        @Override
        public List<String> findPrimaryKeyColumnNamesByTableName(String tableName) {
            return List.of(tableName + "_ID");
        }

        @Override
        public boolean isStaticReferenceTableByName(String tableName) {
            return false;
        }

        @Override
        public String getComponentLookupColumn(String tableName) {
            return null;
        }

        @Override
        public List<StaticValueMetadata> getTableData(String tableName, String pkColumn, String lookupColumn) {
            return List.of();
        }
    }

    private static class SilentColorLogger extends ColorLogger {
        @Override
        public void info(String message, Color color, Object... arguments) { }

        @Override
        public void warn(String message, Color color, Object... arguments) { }
    }

    private static class RecordingColorLogger extends ColorLogger {
        private final List<String> warnings = new ArrayList<>();

        @Override
        public void info(String message, Color color, Object... arguments) { }

        @Override
        public void warn(String message, Color color, Object... arguments) {
            String formatted = message;
            for (Object argument : arguments) {
                formatted = formatted.replaceFirst("\\{\\}", java.util.regex.Matcher.quoteReplacement(String.valueOf(argument)));
            }
            warnings.add(formatted);
        }
    }
}
