package io.tapdata.common.file;

import io.tapdata.file.TapFileStorage;
import io.tapdata.file.operation.FileEndpoint;

public final class FileStorageSession {
    private final FileEndpoint endpoint;
    private final String key;
    private final TapFileStorage storage;

    public FileStorageSession(FileEndpoint endpoint, String key, TapFileStorage storage) {
        this.endpoint = endpoint;
        this.key = key;
        this.storage = storage;
    }

    public FileEndpoint getEndpoint() { return endpoint; }
    public String getKey() { return key; }
    public TapFileStorage getStorage() { return storage; }
}
