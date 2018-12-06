package com.onevizion.scmdb.exception;

public class ScriptExecException extends ScmdbException {
    public ScriptExecException(String message, Throwable cause) {
        super(message, cause);
    }

    public ScriptExecException(String message) {
        super(message);
    }
}
