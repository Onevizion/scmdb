package com.onevizion.scmdb.vo;

import org.apache.commons.io.FilenameUtils;

public enum SchemaType {
    OWNER("", true),
    USER("_user", true),
    RPT("_rpt", true),
    PKG("_pkg", true),
    PERFSTAT("_perfstat", true);

    private final String schemaPostfix;
    private final boolean compileInvalids;

    SchemaType(String schemaPostfix, boolean compileInvalids) {
        this.schemaPostfix = schemaPostfix;
        this.compileInvalids = compileInvalids;
    }

    public static SchemaType getByScriptFileName(String scriptFileName) {
        String baseName = FilenameUtils.getBaseName(scriptFileName).toLowerCase();
        if (baseName.endsWith(USER.getSchemaPostfix()) &&
                !(baseName.endsWith("pkg_user") || baseName.endsWith("pkg_ext_imp_user"))) {
            return USER;
        } else if (baseName.endsWith(RPT.getSchemaPostfix()) &&
                !(baseName.endsWith("pkg_rpt") || baseName.endsWith("pkg_config_field_rpt"))) {
            return RPT;
        }  else if (baseName.endsWith(PKG.getSchemaPostfix())) {
            return PKG;
        } else if (baseName.endsWith(PERFSTAT.getSchemaPostfix())) {
            return PERFSTAT;
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
