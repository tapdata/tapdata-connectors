package io.tapdata.connector.gauss.entity;

public class IllegalDataLengthException extends RuntimeException {
    public IllegalDataLengthException(String message) {
        super(message);
    }
}
