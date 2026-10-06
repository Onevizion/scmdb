package com.onevizion.scmdb;

import com.onevizion.scmdb.dao.DdlDao;
import com.onevizion.scmdb.model.StaticValueMetadata;
import com.onevizion.scmdb.model.TableMetadata;
import com.onevizion.scmdb.vo.ComponentRow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DdlTableMetadataProviderTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void fallsBackToDatabasePrimaryKeyWhenDdlDeclaresNone() throws Exception {
        AppArguments arguments = arguments("fallback");
        writeTable(arguments, """
                CREATE TABLE TEST_ROOT (
                  TEST_ROOT_ID NUMBER NOT NULL,
                  TEST_ROOT_NAME VARCHAR2(100)
                );
                """);
        RecordingColorLogger logger = new RecordingColorLogger();
        DdlTableMetadataProvider provider = new DdlTableMetadataProvider(arguments,
                new FixtureDdlDao(List.of("TEST_ROOT_ID")), logger);

        TableMetadata table = provider.load("TEST_ROOT");

        assertEquals(List.of("TEST_ROOT_ID"), table.primaryKey());
        assertTrue(logger.warnings.isEmpty());
    }

    @Test
    void warnsAndPrefersDdlPrimaryKeyOnMismatchWithDatabase() throws Exception {
        AppArguments arguments = arguments("mismatch");
        writeTable(arguments, """
                CREATE TABLE TEST_ROOT (
                  TEST_ROOT_ID NUMBER NOT NULL,
                  TEST_ROOT_NAME VARCHAR2(100),
                  CONSTRAINT PK_TEST_ROOT PRIMARY KEY (TEST_ROOT_ID)
                );
                """);
        RecordingColorLogger logger = new RecordingColorLogger();
        DdlTableMetadataProvider provider = new DdlTableMetadataProvider(arguments,
                new FixtureDdlDao(List.of("LEGACY_ID")), logger);

        TableMetadata table = provider.load("TEST_ROOT");

        assertEquals(List.of("TEST_ROOT_ID"), table.primaryKey());
        assertEquals(1, logger.warnings.size());
        assertTrue(logger.warnings.get(0).contains("TEST_ROOT"));
    }

    @Test
    void keepsDdlPrimaryKeyWithoutWarningWhenDatabaseAgrees() throws Exception {
        AppArguments arguments = arguments("agree");
        writeTable(arguments, """
                CREATE TABLE TEST_ROOT (
                  TEST_ROOT_ID NUMBER NOT NULL,
                  TEST_ROOT_NAME VARCHAR2(100),
                  CONSTRAINT PK_TEST_ROOT PRIMARY KEY (TEST_ROOT_ID)
                );
                """);
        RecordingColorLogger logger = new RecordingColorLogger();
        DdlTableMetadataProvider provider = new DdlTableMetadataProvider(arguments,
                new FixtureDdlDao(List.of("TEST_ROOT_ID")), logger);

        TableMetadata table = provider.load("TEST_ROOT");

        assertEquals(List.of("TEST_ROOT_ID"), table.primaryKey());
        assertTrue(logger.warnings.isEmpty());
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

    private static void writeTable(AppArguments arguments, String ddl) throws Exception {
        Path tables = Files.createDirectories(arguments.getDdlsDirectory().toPath().resolve("tables"));
        Files.writeString(tables.resolve("TEST_ROOT".toLowerCase(java.util.Locale.ROOT) + ".sql"), ddl);
    }

    private static class FixtureDdlDao extends DdlDao {
        private final List<String> primaryKeyColumns;

        private FixtureDdlDao(List<String> primaryKeyColumns) {
            this.primaryKeyColumns = primaryKeyColumns;
        }

        @Override
        public List<String> findPrimaryKeyColumnNamesByTableName(String tableName) {
            return primaryKeyColumns;
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
        public List<String> findLookupColumnNamesByTableName(String tableName, List<String> excludedColumns) {
            return List.of();
        }

        @Override
        public List<StaticValueMetadata> getTableData(String tableName, String pkColumn, String lookupColumn) {
            return List.of();
        }

        @Override
        public boolean hasColumnInTable(String tableName, String columnName) {
            return false;
        }

        @Override
        public List<ComponentRow> findComponentRows(boolean hasBpdItemTypeId) {
            return List.of();
        }
    }

    private static class RecordingColorLogger extends ColorLogger {
        private final List<String> warnings = new ArrayList<>();

        @Override
        public void info(String message, Color color, Object... arguments) { }

        @Override
        public void warn(String message, Color color, Object... arguments) {
            String formatted = message;
            for (Object argument : arguments) {
                formatted = formatted.replaceFirst("\\{}", java.util.regex.Matcher.quoteReplacement(String.valueOf(argument)));
            }
            warnings.add(formatted);
        }
    }
}
