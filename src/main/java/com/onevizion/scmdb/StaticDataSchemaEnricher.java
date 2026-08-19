package com.onevizion.scmdb;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.onevizion.scmdb.dao.DdlDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static com.onevizion.scmdb.ColorLogger.Color.GREEN;
import static com.onevizion.scmdb.ColorLogger.Color.YELLOW;

@Component
public class StaticDataSchemaEnricher {

    private static final String SCHEMA_FILE_SUFFIX = ".schema.json";

    private final JsonMapper mapper = JsonMapper.builder()
                                                .enable(SerializationFeature.INDENT_OUTPUT)
                                                .build();

    private final AppArguments appArguments;
    private final DdlDao ddlDao;
    private final ColorLogger logger;

    @Autowired
    public StaticDataSchemaEnricher(AppArguments appArguments, DdlDao ddlDao, ColorLogger logger) {
        this.appArguments = appArguments;
        this.ddlDao = ddlDao;
        this.logger = logger;
    }

    public void enrichSchemas(Set<String> tableNames) {
        for (String tableName : tableNames) {
            enrichSchema(tableName, getSchemaFile(tableName));
        }
    }

    private void enrichSchema(String tableName, Path schemaFile) {
        if (!Files.isRegularFile(schemaFile)) {
            return;
        }

        try {
            ObjectNode schema;
            try (InputStream inputStream = Files.newInputStream(schemaFile)) {
                schema = (ObjectNode) mapper.readTree(inputStream);
            }
            ObjectNode properties = (ObjectNode) schema.get("properties");
            if (properties == null) {
                return;
            }

            List<String> pkColumns = ddlDao.findPrimaryKeyColumnNamesByTableName(tableName);
            if (pkColumns.size() != 1) {
                logger.info("  SKIP reference data {}: primary key is not single-column", YELLOW, tableName);
                return;
            }

            String pkColumn = pkColumns.get(0);
            if (ddlDao.isStaticReferenceTableByName(tableName)) {
                enrichStaticReferenceData(tableName, schemaFile, schema, pkColumns, pkColumn);
                return;
            }

            enrichComponentReferenceData(tableName, schemaFile, schema, properties, pkColumns, pkColumn);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to enrich JSON schema with reference data: " + schemaFile.toAbsolutePath(), e);
        }
    }

    private void enrichStaticReferenceData(String tableName, Path schemaFile, ObjectNode schema,
                                           List<String> pkColumns, String pkColumn) throws IOException {

        List<String> lookupColumns = ddlDao.findLookupColumnNamesByTableName(tableName, pkColumns);
        if (lookupColumns.isEmpty()) {
            logger.info("  SKIP static data {}: lookup column was not found", YELLOW, tableName);
            return;
        }

        String lookupColumn = lookupColumns.get(0);
        List<Map<String, Object>> rows = ddlDao.getTableData(tableName, pkColumn, lookupColumn);

        ObjectNode referenceData = createReferenceData("static", pkColumn, lookupColumn);
        ArrayNode data = referenceData.putArray("data");
        for (Map<String, Object> row : rows) {
            ObjectNode item = data.addObject();
            item.set("id", mapper.valueToTree(row.get("id")));
            item.put("name", row.get("name") == null ? null : String.valueOf(row.get("name")));
        }

        schema.set("x-reference-data", referenceData);
        writeSchema(schemaFile, schema);
        logger.info("  OK static data {}: {} values", GREEN, tableName, rows.size());
    }

    private void enrichComponentReferenceData(String tableName, Path schemaFile, ObjectNode schema,
                                              ObjectNode properties, List<String> pkColumns, String pkColumn)
            throws IOException {
        String lookupColumn = ddlDao.getComponentLookupColumn(tableName);
        if (lookupColumn == null || !properties.has("PROGRAM_ID") || !properties.has(lookupColumn)) {
            return;
        }

        ObjectNode referenceData = createReferenceData("user", pkColumn, lookupColumn);
        referenceData.putArray("data");

        schema.set("x-reference-data", referenceData);
        writeSchema(schemaFile, schema);
        logger.info("  OK component reference data {}: lookup by {}", GREEN,
                    tableName, lookupColumn);
    }

    private void writeSchema(Path schemaFile, ObjectNode schema) throws IOException {
        try (OutputStream outputStream = Files.newOutputStream(schemaFile)) {
            mapper.writeValue(outputStream, schema);
        }
    }

    private ObjectNode createReferenceData(String type, String pkColumn, String lookupColumn) {
        ObjectNode referenceData = mapper.createObjectNode();
        referenceData.put("type", type);
        referenceData.put("pk_column", pkColumn);
        ArrayNode lookupColumnsNode = referenceData.putArray("lookup_columns");
        lookupColumnsNode.add(lookupColumn);
        return referenceData;
    }

    private Path getSchemaFile(String tableName) {
        return appArguments.getJsonSchemasDirectory()
                           .toPath()
                           .resolve(tableName.toLowerCase(Locale.ROOT) + SCHEMA_FILE_SUFFIX);
    }

}
