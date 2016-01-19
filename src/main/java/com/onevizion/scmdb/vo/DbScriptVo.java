package com.onevizion.scmdb.vo;

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

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null) {
            return false;
        } else if (getClass() != obj.getClass()) {
            return false;
        }

        DbScriptVo vo = (DbScriptVo) obj;
        return fileHash.equals(vo.getFileHash());
    }
}
