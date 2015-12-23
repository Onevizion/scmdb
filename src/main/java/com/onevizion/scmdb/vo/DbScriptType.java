package com.onevizion.scmdb.vo;

public enum DbScriptType {
    COMMIT(0), ROLLBACK(1);

    int typeId;

    DbScriptType(int typeId) {
        this.typeId = typeId;
    }

    public int getTypeId() {
        return typeId;
    }

    public static DbScriptType getForInt(int typeId) {
        switch (typeId) {
            case 0:
                return COMMIT;
            case 1:
                return ROLLBACK;
            default:
                throw new IllegalArgumentException("Not supported db script statusId: [" + typeId + "]");
        }
    }
}
