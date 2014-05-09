package com.rethinkdb.error;

public class RethinkDBConnectException extends RethinkDBException {
    public RethinkDBConnectException() {
    }

    public RethinkDBConnectException(String message) {
        super(message);
    }

    public RethinkDBConnectException(String message, Throwable cause) {
        super(message, cause);
    }

    public RethinkDBConnectException(Throwable cause) {
        super(cause);
    }

    public RethinkDBConnectException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
