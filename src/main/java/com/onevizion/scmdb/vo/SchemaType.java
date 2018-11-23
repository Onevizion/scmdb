package com.onevizion.scmdb.vo;

import org.apache.commons.io.FilenameUtils;

public enum SchemaType {
    OWNER("", true),
    USER("_user", false),
    RPT("_rpt", false);

    private final String schemaPostfix;
    private final boolean compileInvalids;

    SchemaType(String schemaPostfix, boolean compileInvalids) {
        this.schemaPostfix = schemaPostfix;
        this.compileInvalids = compileInvalids;
    }

    public static SchemaType getByScriptFileName(String scriptFileName) {
        String baseName = FilenameUtils.getBaseName(scriptFileName);
        if (baseName.endsWith("_user") && !baseName.endsWith("pkg_user")) {
            return USER;
        } else if (baseName.endsWith("_rpt") && !baseName.endsWith("pkg_rpt")) {
            return RPT;
        } else {
            return OWNER;
        }
    }

    public boolean isCompileInvalids() {
        return compileInvalids;
    }

    public String getSchemaPostfix() {
        return schemaPostfix;
    }
}
