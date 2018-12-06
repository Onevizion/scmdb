package com.onevizion.scmdb.exception;

import org.slf4j.helpers.MessageFormatter;

public class ScmdbException extends RuntimeException {
    public ScmdbException() { }

    public ScmdbException(String message, Throwable cause) {
        super(message, cause);
    }

    public ScmdbException(String message) {
        super(message);
    }

    /*public ScmdbException(Throwable cause) {
        super(cause);
    }

    public ScmdbException(String message, Throwable cause, Object ... params) {
        super(MessageFormatter.format(message, params), cause);
    }

    public ScmdbException(String message, Object ... params) {
        super(MessageFormatter.format(message, params));
    }*/
}
