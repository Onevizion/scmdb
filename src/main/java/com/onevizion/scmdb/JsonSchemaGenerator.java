package com.onevizion.scmdb;

import com.onevizion.scmdb.dao.DdlDao;
import com.onevizion.scmdb.vo.DbObject;
import com.onevizion.scmdb.vo.DbObjectType;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Collectors;

import static com.onevizion.scmdb.ColorLogger.Color.GREEN;
import static com.onevizion.scmdb.ColorLogger.Color.YELLOW;

@Component
public class JsonSchemaGenerator {

    private static final String GENERATOR_RESOURCE = "json_schema_generator.py";
    private static final String PARSER_RESOURCE = "json_schema_py_ddl_parser.py";
    private static final String OK_PREFIX = "  OK";
    private static final String SKIP_PREFIX = "  SKIP";

    private final AppArguments appArguments;
    private final DdlDao ddlDao;
    private final ColorLogger logger;
    private final StaticDataSchemaEnricher staticDataSchemaEnricher;
    private final ComponentStructureGenerator componentStructureGenerator;

    @Autowired
    public JsonSchemaGenerator(AppArguments appArguments, DdlDao ddlDao, ColorLogger logger,
                               StaticDataSchemaEnricher staticDataSchemaEnricher,
                               ComponentStructureGenerator componentStructureGenerator) {
        this.appArguments = appArguments;
        this.ddlDao = ddlDao;
        this.logger = logger;
        this.staticDataSchemaEnricher = staticDataSchemaEnricher;
        this.componentStructureGenerator = componentStructureGenerator;
    }

    public void generateSchemas(Collection<DbObject> changedDbObjects) {
        Set<String> tableNames = resolveTableNames(changedDbObjects);
        if (tableNames.isEmpty()) {
            logger.info("No changed tables found for JSON schema generation");
        } else {
            Set<String> generatedTableNames = runPythonGenerator(tableNames, false);
            staticDataSchemaEnricher.enrichSchemas(generatedTableNames);
        }
        componentStructureGenerator.generate();
    }

    public void generateSchemasForAllTables() {
        Set<String> tableNames = resolveConfigurationTableNames();
        if (tableNames.isEmpty()) {
            logger.info("No configuration tables found for JSON schema generation");
        } else {
            Set<String> generatedTableNames = runPythonGenerator(tableNames, true);
            staticDataSchemaEnricher.enrichSchemas(generatedTableNames);
        }
        componentStructureGenerator.generate();
    }

    private Set<String> resolveConfigurationTableNames() {
        return ddlDao.loadComponentTableNames()
                     .stream()
                     .filter(StringUtils::isNotBlank)
                     .map(name -> name.toUpperCase(Locale.ROOT))
                     .collect(Collectors.toCollection(TreeSet::new));
    }

    private Set<String> resolveTableNames(Collection<DbObject> dbObjects) {
        return dbObjects.stream()
                        .map(this::resolveTableName)
                        .filter(StringUtils::isNotBlank)
                        .map(name -> name.toUpperCase(Locale.ROOT))
                        .collect(Collectors.toCollection(TreeSet::new));
    }

    private String resolveTableName(DbObject dbObject) {
        DbObjectType type = dbObject.getType();
        if (type == null) {
            return null;
        }
        return switch (type) {
            case TABLE -> dbObject.getName();
            case COMMENT -> dbObject.getName().split("\\.")[0];
            case INDEX, TRIGGER, SEQUENCE -> ddlDao.getTableNameByDepObject(dbObject);
            default -> null;
        };
    }

    private Set<String> runPythonGenerator(Set<String> tableNames, boolean includeReferences) {
        File tempDir = null;
        Set<String> generatedTableNames = new TreeSet<>();
        try {
            tempDir = Files.createTempDirectory("scmdb_json_schema_").toFile();
            File generator = extractResource(GENERATOR_RESOURCE, tempDir);
            extractResource(PARSER_RESOURCE, tempDir);

            List<String> command = new ArrayList<>();
            command.add("python3");
            command.add(generator.getAbsolutePath());
            command.add("--ddl-root");
            command.add(appArguments.getDdlsDirectory().getAbsolutePath());
            command.add("--output-dir");
            command.add(appArguments.getJsonSchemasDirectory().getAbsolutePath());
            command.add("--tables");
            command.add(String.join(",", tableNames));
            if (includeReferences) {
                command.add("--include-refs");
            }

            logger.info("Generating JSON schemas for selected tables", GREEN);
            Process process = new ProcessBuilder(command)
                    .redirectErrorStream(true)
                    .start();

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(),
                                                                                  StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    logger.info(line, chooseColor(line));
                    collectGeneratedTableName(line, generatedTableNames);
                }
            }

            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new RuntimeException("JSON schema generator failed with exit code " + exitCode);
            }
            return generatedTableNames;
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Failed to generate JSON schemas: " + e.getMessage(), e);
        } finally {
            if (tempDir != null) {
                FileUtils.deleteQuietly(tempDir);
            }
        }
    }

    private void collectGeneratedTableName(String line, Set<String> generatedTableNames) {
        if (!line.startsWith(OK_PREFIX)) {
            return;
        }

        String[] parts = line.trim().split("\\s+");
        if (parts.length >= 2) {
            generatedTableNames.add(parts[1].toUpperCase(Locale.ROOT));
        }
    }

    private File extractResource(String resourceName, File tempDir) throws IOException {
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(resourceName)) {
            if (inputStream == null) {
                throw new FileNotFoundException("Resource not found: " + resourceName);
            }
            File file = new File(tempDir, resourceName);
            try (OutputStream outputStream = new FileOutputStream(file)) {
                inputStream.transferTo(outputStream);
            }
            return file;
        }
    }

    private ColorLogger.Color chooseColor(String line) {
        return line.startsWith(OK_PREFIX) ? GREEN
                                          : line.startsWith(SKIP_PREFIX) ? YELLOW
                                                                         : ColorLogger.Color.WHITE;
    }

}
