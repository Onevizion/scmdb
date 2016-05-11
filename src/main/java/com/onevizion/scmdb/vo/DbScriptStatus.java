package com.onevizion.scmdb.vo;

public enum DbScriptStatus {
    EXECUTED(0L), NOT_EXECUTED(1L), EXECUTED_WITH_ERRORS(2L);

    Long statusId;

    DbScriptStatus(Long statusId) {
        this.statusId = statusId;
    }

    public Long getStatusId() {
        return statusId;
    }

    public static DbScriptStatus getForId(Long statusId) {
        if (Long.valueOf(0L).equals(statusId)) {
            return EXECUTED;
        } else if (Long.valueOf(1L).equals(statusId)) {
            return NOT_EXECUTED;
        } else if (Long.valueOf(2L).equals(statusId)) {
            return EXECUTED_WITH_ERRORS;
        } else {
            throw new IllegalArgumentException("Not supported db script statusId: [" + statusId + "]");
        }
    }
}
