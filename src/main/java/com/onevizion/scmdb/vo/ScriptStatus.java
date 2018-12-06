package com.onevizion.scmdb.vo;

import java.util.Arrays;

public enum ScriptStatus {
    EXECUTED(0L),
    NOT_EXECUTED(1L),
    EXECUTED_WITH_ERRORS(2L),
    COMMAND_EXEC_FAILURE(3L);

    private final Long id;

    ScriptStatus(Long id) {
        this.id = id;
    }

    public Long getId() {
        return id;
    }

    public static ScriptStatus getById(Long id) {
        return Arrays.stream(values())
                     .filter(s -> s.getId().equals(id))
                     .findFirst()
                     .orElseThrow(() -> new IllegalArgumentException("Not supported db script id: [" + id + "]"));
    }

    public static ScriptStatus getByScriptExitCode(int exitCode) {
        if (exitCode == 0) {
            return ScriptStatus.EXECUTED;
        } else {
            return EXECUTED_WITH_ERRORS;
        }
    }
}
