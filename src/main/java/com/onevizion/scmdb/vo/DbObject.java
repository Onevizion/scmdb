package com.onevizion.scmdb.vo;

public class DbObject {
    private String name;
    private DbObjectType type;
    private String ddl;

    public DbObject(String name, DbObjectType type) {
        this.name = name.toLowerCase();
        this.type = type;
    }

    public DbObject() {}

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name.toLowerCase();
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DbObject dbObject = (DbObject) o;
        return name.equals(dbObject.name) && type == dbObject.type;
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + type.hashCode();
        return result;
    }
}
