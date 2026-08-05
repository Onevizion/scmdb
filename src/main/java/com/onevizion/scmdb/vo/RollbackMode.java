package com.onevizion.scmdb.vo;

public enum RollbackMode {

    ASK /* Ask whether to execute rollbacks */,
    SKIP /* Do not execute rollback scripts and delete information about deleted scripts
        (compared to files in db/scripts directory) in DB_SCRIPT table, so that on the next launch
         they are not detected again. Previously, this behavior could be enabled with --omit-changed */,
    FORCE_EXECUTE /* Execute rollback scripts without asking */

}
