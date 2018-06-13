package com.onevizion.scmdb.vo;

import java.util.Arrays;
import java.util.List;

import static java.util.Arrays.asList;

public enum DbObjectType {
    PACKAGE_BODY("package body", asList("create package body", "replace package body", "drop package body")),
    PACKAGE_SPEC("package", asList("create package", "replace package", "drop package", "package")),
    TABLE("table", asList("create table", "alter table", "drop table")),
    VIEW("view", asList("create view", "replace view", "drop view", "create force view", "replace force view")),
    COMMENT("comment", asList("comment on table", "comment on column")),
    INDEX("index", asList("create index", "create unique index", "drop index")),
    SEQUENCE("sequence", asList("create sequence", "drop sequence")),
    TRIGGER("trigger", asList("create trigger", "replace trigger", "drop trigger", "alter trigger"));

    private String name;
    private List<String> changeKeywords;

    DbObjectType(String name, List<String> changeKeywords) {
        this.name = name;
        this.changeKeywords = changeKeywords;
    }

    public String getName() {
        return name;
    }

    public List<String> getChangeKeywords() {
        return changeKeywords;
    }

    public static DbObjectType getByName(String objectType) {
        return Arrays.stream(values())
                     .filter(t -> t.getName().equals(objectType.toLowerCase()))
                     .findAny()
                     .orElseThrow(
                             () -> new IllegalArgumentException("Can't find DbObjectType with name=" + objectType));
    }
}