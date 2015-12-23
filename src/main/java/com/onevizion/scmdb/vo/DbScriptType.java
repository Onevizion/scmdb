package com.onevizion.scmdb.vo;

public enum DbScriptType {
    COMMIT(0L), ROLLBACK(1L);

    Long typeId;

    DbScriptType(Long typeId) {
        this.typeId = typeId;
    }

    public Long getTypeId() {
        return typeId;
    }

    public static DbScriptType getForId(Long typeId) {
        if (Long.valueOf(0L).equals(typeId)) {
            return COMMIT;
        } else if (Long.valueOf(1L).equals(typeId)) {
            return ROLLBACK;
        } else {
            throw new IllegalArgumentException("Not supported db script statusId: [" + typeId + "]");
        }
    }
}
