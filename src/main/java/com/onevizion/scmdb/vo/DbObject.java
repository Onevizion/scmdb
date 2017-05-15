package com.onevizion.scmdb.vo;

import java.lang.String;

public class DbObject {
    private String name;
    private DbObjectType type;
    private String ddl;

    public DbObject(String name, DbObjectType type) {
        this.name = name;
        this.type = type;
    }

    public DbObject() {}

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

    public DbObjectType getType() {
        return type;
    }

    public void setType(DbObjectType type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return name + " | " + type;
    }
}
