package com.onevizion.scmdb.vo;

public enum RollbackMode {

    ASK /* Ask execute rollbacks or not */,
    SKIP /* Do not execute rollback scripts */,
    FORCE_EXECUTE /* Execute rollback scripts without asking */

}
