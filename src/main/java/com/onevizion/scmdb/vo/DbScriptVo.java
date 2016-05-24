package com.onevizion.scmdb.vo;

import com.onevizion.scmdb.Checkouter;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Date;

public class DbScriptVo {
    private Long dbScriptId;
    private String name;
    private String fileHash;
    private String text;
    private Date ts;
    private String output;
    private DbScriptType type;
    private DbScriptStatus status;
    private File file;

    private static final Logger logger = LoggerFactory.getLogger(Checkouter.class);
    private static final String ROLLBACK_SUFX = "_rollback";

    public static DbScriptVo create(File scriptFile) {
        DbScriptVo scriptVo = new DbScriptVo();

        scriptVo.setFile(scriptFile);
        scriptVo.setName(scriptFile.getName());
        String fileContent = null;
        try {
            fileContent = FileUtils.readFileToString(scriptFile);
            scriptVo.setFileHash(DigestUtils.sha1Hex(fileContent.replaceAll("\\r\\n", "\n")));
        } catch (IOException e) {
            logger.warn("Can't read file content [{}]", scriptFile.getName(), e);
        }
        scriptVo.setTs(new Date(scriptFile.lastModified()));

        if (FilenameUtils.getBaseName(scriptVo.getName()).endsWith(ROLLBACK_SUFX)) {
            scriptVo.setText(fileContent);
            scriptVo.setType(DbScriptType.ROLLBACK);
        } else {
            scriptVo.setType(DbScriptType.COMMIT);
        }
        scriptVo.setStatus(DbScriptStatus.EXECUTED);

        return scriptVo;
    }

    public Long getDbScriptId() {
        return dbScriptId;
    }

    public void setDbScriptId(Long dbScriptId) {
        this.dbScriptId = dbScriptId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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

    public Long getType() {
        return type.getTypeId();
    }

    public void setType(Long type) {
        this.type = DbScriptType.getForId(type);
    }

    public void setType(DbScriptType type) {
        this.type = type;
    }

    public Long getStatus() {
        return status.getStatusId();
    }

    public void setStatus(Long status) {
        this.status = DbScriptStatus.getForId(status);
    }

    public void setStatus(DbScriptStatus status) {
        this.status = status;
    }

    public File getFile() {
        return file;
    }

    public void setFile(File file) {
        this.file = file;
    }

    public String getRollbackName() {
        if (type == DbScriptType.ROLLBACK) {
            return name;
        } else {
            return FilenameUtils.getBaseName(name) + ROLLBACK_SUFX + FilenameUtils.EXTENSION_SEPARATOR_STR
                    + FilenameUtils.getExtension(name);
        }
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

        DbScriptVo that = (DbScriptVo) o;
        return name != null ? name.equals(that.name) : that.name == null;
    }
}
