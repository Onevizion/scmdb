package com.onevizion.scmdb.vo;

public enum ScriptStatus {
    EXECUTED(0L),
    NOT_EXECUTED(1L),
    EXECUTED_WITH_ERRORS(2L);

    private Long id;

    ScriptStatus(Long id) {
        this.id = id;
    }

    public Long getId() {
        return id;
    }

    public static ScriptStatus getById(Long id) {
        if (Long.valueOf(0L).equals(id)) {
            return EXECUTED;
        } else if (Long.valueOf(1L).equals(id)) {
            return NOT_EXECUTED;
        } else if (Long.valueOf(2L).equals(id)) {
            return EXECUTED_WITH_ERRORS;
        } else {
            throw new IllegalArgumentException("Not supported db script id: [" + id + "]");
        }
    }
}
