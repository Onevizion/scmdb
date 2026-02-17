package com.onevizion.scmdb.vo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;

public class BackportResult {

    private boolean shouldRunScmdb;
    private int scriptsWritten;
    private int rollbackScriptsWritten;
    private int renamed;
    private List<String> warnings;
    private String error;

    public static BackportResult fromJson(String json) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(json);

            BackportResult result = new BackportResult();
            result.shouldRunScmdb = root.path("should_run_scmdb").asBoolean(false);
            result.scriptsWritten = root.path("scripts_written").asInt(0);
            result.rollbackScriptsWritten = root.path("rollback_scripts_written").asInt(0);
            result.renamed = root.path("renamed").asInt(0);

            result.warnings = new ArrayList<>();
            JsonNode warningsNode = root.path("warnings");
            if (warningsNode.isArray()) {
                for (JsonNode w : warningsNode) {
                    result.warnings.add(w.asText());
                }
            }

            JsonNode errorNode = root.path("error");
            if (!errorNode.isMissingNode() && !errorNode.isNull()) {
                result.error = errorNode.asText();
            }

            return result;
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse backport result JSON: " + e.getMessage(), e);
        }
    }

    public boolean isShouldRunScmdb() {
        return shouldRunScmdb;
    }

    public int getScriptsWritten() {
        return scriptsWritten;
    }

    public int getRollbackScriptsWritten() {
        return rollbackScriptsWritten;
    }

    public int getRenamed() {
        return renamed;
    }

    public List<String> getWarnings() {
        return warnings;
    }

    public String getError() {
        return error;
    }

}
