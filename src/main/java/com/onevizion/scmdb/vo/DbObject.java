package com.onevizion.scmdb.vo;

import java.lang.String;

public class DbObject {
    private String name;

    private String ddl;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDdl() {
        return ddl;
    }

    public void setDdl(String ddl) {
        this.ddl = ddl;
    }
}
