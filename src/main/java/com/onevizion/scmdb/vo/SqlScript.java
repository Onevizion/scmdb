package com.onevizion.scmdb.vo;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.Objects;

public class SqlScript implements Comparable<SqlScript> {
    private Long id;
    private String name;
    private String fileHash;
    private String text;
    private Date ts;
    private String output;
    private ScriptType type;
    private ScriptStatus status;
    private File file;
    private SchemaType schemaType = SchemaType.OWNER;
    private int orderNumber;

    private static final String ROLLBACK_SUFFIX = "_rollback";

    public static SqlScript create(File scriptFile) {
        return create(scriptFile, true);
    }

    public static SqlScript create(File scriptFile, boolean readFileContent) {
        SqlScript script = new SqlScript();

        script.setFile(scriptFile);
        script.setName(scriptFile.getName());
        script.setTs(new Date(scriptFile.lastModified()));
        script.setOrderNumber(extractOrderNumber(script.getName()));

        if (FilenameUtils.getBaseName(script.getName()).endsWith(ROLLBACK_SUFFIX)) {
            script.setType(ScriptType.ROLLBACK);
        } else {
            script.setType(ScriptType.COMMIT);
        }
        script.setStatus(ScriptStatus.EXECUTED);
        script.setSchemaType(SchemaType.getByScriptFileName(scriptFile.getName()));

        if (readFileContent) {
            script.loadContentFromFile();
        }

        return script;
    }

    public void loadContentFromFile() {
        String fileContent;
        try {
            fileContent = FileUtils.readFileToString(file, "UTF-8");
            text = fileContent;
            fileHash = DigestUtils.sha1Hex(fileContent.replaceAll("\\r\\n", "\n"));
        } catch (IOException e) {
            throw new RuntimeException("Can't read file content [" + name + "]", e);
        }
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
        this.orderNumber = extractOrderNumber(name);
    }

    public String getFileHash() {
        return fileHash;
    }

    public void setFileHash(String fileHash) {
        this.fileHash = fileHash;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public Date getTs() {
        return ts;
    }

    public void setTs(Date ts) {
        this.ts = ts;
    }

    public String getOutput() {
        return output;
    }

    public void setOutput(String output) {
        this.output = output;
    }

    public ScriptType getType() {
        return type;
    }

    public void setType(ScriptType type) {
        this.type = type;
    }

    public ScriptStatus getStatus() {
        return status;
    }

    public void setStatus(ScriptStatus status) {
        this.status = status;
    }

    public File getFile() {
        return file;
    }

    public void setFile(File file) {
        this.file = file;
    }

    public SchemaType getSchemaType() {
        return schemaType;
    }

    public void setSchemaType(SchemaType schemaType) {
        this.schemaType = schemaType;
    }

    public String getRollbackName() {
        if (type == ScriptType.ROLLBACK) {
            return name;
        } else {
            return FilenameUtils.getBaseName(name) + ROLLBACK_SUFFIX + FilenameUtils.EXTENSION_SEPARATOR_STR
                    + FilenameUtils.getExtension(name);
        }
    }

    public String getCommitName() {
        if (type == ScriptType.COMMIT) {
            return name;
        } else {
            return name.replaceAll(ROLLBACK_SUFFIX, "");
        }
    }

    public int getOrderNumber() {
        return orderNumber;
    }

    public void setOrderNumber(int orderNumber) {
        this.orderNumber = orderNumber;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SqlScript that = (SqlScript) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int compareTo(SqlScript anotherScript) {
        return Integer.compare(orderNumber, anotherScript.orderNumber);
    }

    private static int extractOrderNumber(String scriptName) {
        String[] parts = scriptName.split("_");
        if (parts.length >= 1 && NumberUtils.isDigits(parts[0])) {
            return Integer.parseInt(parts[0]);
        } else {
            return -1;
        }
    }
}
