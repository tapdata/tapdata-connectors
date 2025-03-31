package io.tapdata.connector.starrocks.streamload.exception;

public class StarrocksRetryableException extends Exception {
    public StarrocksRetryableException() {
        super();
    }

    public StarrocksRetryableException(String message) {
        super(message);
    }

    public StarrocksRetryableException(String message, Throwable cause) {
        super(message, cause);
    }

    public StarrocksRetryableException(Throwable cause) {
        super(cause);
    }

    protected StarrocksRetryableException(String message, Throwable cause,
                                  boolean enableSuppression,
                                  boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
