package com.onevizion.scmdb.vo;

public enum RollbackMode {

    ASK /* Ask execute rollbacks or not */,
    SKIP /* Do not execute rollback scripts and delete information about deleted scripts
        (in compare with files in db/scripts directory) in DB_SCRIPT table, so next launch.
        Previously this mode can be enabled by using --omit-changed command line argument */,
    FORCE_EXECUTE /* Execute rollback scripts without asking */

}
