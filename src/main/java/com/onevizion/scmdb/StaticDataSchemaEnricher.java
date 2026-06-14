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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static com.onevizion.scmdb.ColorLogger.Color.GREEN;
import static com.onevizion.scmdb.ColorLogger.Color.YELLOW;

@Component
public class StaticDataSchemaEnricher {

    private static final String SCHEMA_FILE_SUFFIX = ".schema.json";

    @Autowired
    private AppArguments appArguments;

    @Autowired
    private DdlDao ddlDao;

    @Autowired
    private ColorLogger logger;

    private final ObjectMapper mapper = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);

    public void enrichSchemas(Set<String> tableNames) {
        for (String tableName : tableNames) {
            enrichSchema(tableName, schemaFile(tableName));
        }
    }

    public void enrichAllSchemas() {
        File[] schemaFiles = appArguments.getJsonSchemasDirectory().listFiles((dir, name) -> name.endsWith(SCHEMA_FILE_SUFFIX));
        if (schemaFiles == null || schemaFiles.length == 0) {
            logger.info("No JSON schemas found for static data enrichment");
            return;
        }

        for (File schemaFile : schemaFiles) {
            String fileName = schemaFile.getName();
            String tableName = fileName.substring(0, fileName.length() - SCHEMA_FILE_SUFFIX.length())
                                   .toUpperCase(Locale.ROOT);
            enrichSchema(tableName, schemaFile);
        }
    }

    private void enrichSchema(String tableName, File schemaFile) {
        if (!schemaFile.isFile()) {
            return;
        }

        try {
            ObjectNode schema = (ObjectNode) mapper.readTree(schemaFile);
            ObjectNode properties = (ObjectNode) schema.get("properties");
            if (properties == null) {
                return;
            }

            List<String> pkColumns = ddlDao.getPrimaryKeyColumns(tableName);
            if (pkColumns.size() != 1) {
                logger.info("  SKIP reference data {}: primary key is not single-column", YELLOW, tableName);
                return;
            }

            String pkColumn = pkColumns.get(0);
            if (ddlDao.isStaticReferenceTable(tableName)) {
                enrichStaticReferenceData(tableName, schemaFile, schema, pkColumns, pkColumn);
                return;
            }

            enrichComponentReferenceData(tableName, schemaFile, schema, properties, pkColumns, pkColumn);
        } catch (IOException e) {
            throw new RuntimeException("Failed to enrich JSON schema with reference data: " + schemaFile.getAbsolutePath(), e);
        }
    }

    private void enrichStaticReferenceData(
            String tableName,
            File schemaFile,
            ObjectNode schema,
            List<String> pkColumns,
            String pkColumn
    ) throws IOException {
        List<String> lookupColumns = ddlDao.getLookupColumns(tableName, pkColumns);
        if (lookupColumns.isEmpty()) {
            logger.info("  SKIP static data {}: lookup column was not found", YELLOW, tableName);
            return;
        }

        String lookupColumn = lookupColumns.get(0);
        List<Map<String, Object>> rows = ddlDao.loadReferenceData(tableName, pkColumn, lookupColumn);

        ObjectNode referenceData = createReferenceData("static", pkColumn, lookupColumn);
        ArrayNode data = referenceData.putArray("data");
        for (Map<String, Object> row : rows) {
            ObjectNode item = data.addObject();
            item.set("id", mapper.valueToTree(row.get("id")));
            item.put("name", row.get("name") == null ? null : String.valueOf(row.get("name")));
        }

        schema.set("x-reference-data", referenceData);
        mapper.writeValue(schemaFile, schema);
        logger.info("  OK static data {}: {} values", GREEN, tableName, rows.size());
    }

    private void enrichComponentReferenceData(
            String tableName,
            File schemaFile,
            ObjectNode schema,
            ObjectNode properties,
            List<String> pkColumns,
            String pkColumn
    ) throws IOException {
        String lookupColumn = ddlDao.getComponentLookupColumn(tableName);
        if (lookupColumn == null) {
            return;
        }
        if (!properties.has("PROGRAM_ID") || !properties.has(lookupColumn)) {
            return;
        }

        ObjectNode referenceData = createReferenceData("user", pkColumn, lookupColumn);
        referenceData.putArray("data");

        schema.set("x-reference-data", referenceData);
        mapper.writeValue(schemaFile, schema);
        logger.info("  OK component reference data {}: lookup by {}", GREEN, tableName, lookupColumn);
    }

    private ObjectNode createReferenceData(String type, String pkColumn, String lookupColumn) {
        ObjectNode referenceData = mapper.createObjectNode();
        referenceData.put("type", type);
        referenceData.put("pk_column", pkColumn);
        ArrayNode lookupColumnsNode = referenceData.putArray("lookup_columns");
        lookupColumnsNode.add(lookupColumn);
        return referenceData;
    }

    private File schemaFile(String tableName) {
        return new File(appArguments.getJsonSchemasDirectory(),
                        tableName.toLowerCase(Locale.ROOT) + SCHEMA_FILE_SUFFIX);
    }
}
