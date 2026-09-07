package io.tapdata.common.file;

import io.tapdata.file.operation.FileEndpoint;

public interface FileStorageSessionManager extends AutoCloseable {
    FileStorageSession retain(FileEndpoint endpoint);
    void release(FileStorageSession session);
    void invalidate(FileStorageSession session);
    @Override void close();
}
