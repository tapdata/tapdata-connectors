package io.tapdata.connector.gauss.entity;

public class IllegalDataLengthException extends RuntimeException {

    public IllegalDataLengthException() {
    }

    public IllegalDataLengthException(String message) {
        super(message);
    }

    public IllegalDataLengthException(String message, Throwable cause) {
        super(message, cause);
    }

    public IllegalDataLengthException(Throwable cause) {
        super(cause);
    }

    public IllegalDataLengthException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
