package io.tapdata.mongodb.batch;

public interface ErrorHandler {
    void doHandle(Exception e);
}
