package io.tapdata.common.file;

import io.tapdata.file.TapFileStorage;
import io.tapdata.file.operation.FileEndpoint;
import io.tapdata.file.operation.FileOperationErrorCode;
import io.tapdata.file.operation.FileOperationException;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringJoiner;

public class DefaultFileStorageSessionManager implements FileStorageSessionManager {
    private final FileServiceConfigMapper configMapper;
    private final int maxSessions;
    private final Map<String, Entry> sessions = new LinkedHashMap<>();

    public DefaultFileStorageSessionManager() {
        this(new DefaultFileServiceConfigMapper(), 32);
    }

    public DefaultFileStorageSessionManager(FileServiceConfigMapper configMapper, int maxSessions) {
        this.configMapper = configMapper;
        this.maxSessions = maxSessions;
    }

    @Override
    public synchronized FileStorageSession retain(FileEndpoint endpoint) {
        String key = key(endpoint);
        Entry entry = sessions.get(key);
        if (entry == null) {
            if (sessions.size() >= maxSessions) {
                throw new FileOperationException(FileOperationErrorCode.FILE_SESSION_LIMIT,
                        "file storage session limit reached");
            }
            TapFileStorage storage = build(endpoint);
            entry = new Entry(new FileStorageSession(endpoint, key, storage));
            sessions.put(key, entry);
        }
        entry.references++;
        return entry.session;
    }

    protected TapFileStorage build(FileEndpoint endpoint) {
        try {
            return FileStorageFactory.build(endpoint.getProtocol(), configMapper.mapStorageParams(endpoint));
        } catch (FileOperationException e) {
            throw e;
        } catch (Exception e) {
            throw new FileOperationException(FileOperationErrorCode.FILE_CONNECT_FAILED,
                    "failed to create file storage for protocol " + endpoint.getProtocol(), e);
        }
    }

    @Override
    public synchronized void release(FileStorageSession session) {
        Entry entry = sessions.get(session.getKey());
        if (entry != null && entry.references > 0) entry.references--;
    }

    @Override
    public synchronized void invalidate(FileStorageSession session) {
        Entry entry = sessions.remove(session.getKey());
        if (entry != null) destroy(entry.session);
    }

    @Override
    public synchronized void close() {
        for (Entry entry : sessions.values()) destroy(entry.session);
        sessions.clear();
    }

    private void destroy(FileStorageSession session) {
        try {
            session.getStorage().destroy();
        } catch (Exception ignored) {
        }
    }

    private String key(FileEndpoint endpoint) {
        StringJoiner joiner = new StringJoiner("|");
        joiner.add(String.valueOf(Thread.currentThread().getId()));
        joiner.add(endpoint.getProtocol());
        joiner.add(endpoint.getRootPath());
        endpoint.getParams().entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> joiner.add(entry.getKey() + "=" + String.valueOf(entry.getValue())));
        return joiner.toString();
    }

    private static final class Entry {
        private final FileStorageSession session;
        private int references;
        private Entry(FileStorageSession session) { this.session = session; }
    }
}
