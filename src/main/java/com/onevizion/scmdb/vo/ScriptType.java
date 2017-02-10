package com.onevizion.scmdb.vo;

public enum ScriptType {
    COMMIT(0L),
    ROLLBACK(1L);

    private Long id;

    ScriptType(Long id) {
        this.id = id;
    }

    public Long getId() {
        return id;
    }

    public static ScriptType getById(Long id) {
        if (Long.valueOf(0L).equals(id)) {
            return COMMIT;
        } else if (Long.valueOf(1L).equals(id)) {
            return ROLLBACK;
        } else {
            throw new IllegalArgumentException("Not supported db script type: [" + id + "]");
        }
    }
}
