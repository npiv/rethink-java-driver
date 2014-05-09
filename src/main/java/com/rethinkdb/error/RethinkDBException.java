package com.rethinkdb.error;

public class RethinkDBException extends RuntimeException {
    public RethinkDBException() {
    }

    public RethinkDBException(String message) {
        super(message);
    }

    public RethinkDBException(String message, Throwable cause) {
        super(message, cause);
    }

    public RethinkDBException(Throwable cause) {
        super(cause);
    }

    public RethinkDBException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
