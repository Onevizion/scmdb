package com.onevizion.scmdb.exception;

public class InvalidObjectException extends ScmdbException {

    public InvalidObjectException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidObjectException(String message) {
        super(message);
    }
}
