package com.rethinkdb.error;

public class RethinkDBQueryException extends RethinkDBException {
    public RethinkDBQueryException() {
    }

    public RethinkDBQueryException(String message) {
        super(message);
    }

    public RethinkDBQueryException(String message, Throwable cause) {
        super(message, cause);
    }

    public RethinkDBQueryException(Throwable cause) {
        super(cause);
    }

    public RethinkDBQueryException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
