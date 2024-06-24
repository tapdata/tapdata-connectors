package io.tapdata.pdk.cli.services.request;

import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicBoolean;

public class ProcessGroupInfo {
    final AtomicBoolean lock;
    long totalByte;
    long uploadedBytes;
    final StringJoiner allFileName;
    Long uploadStart;
    boolean over;
    public ProcessGroupInfo(AtomicBoolean lock) {
        this.lock = lock;
        allFileName = new StringJoiner(", ");
    }

    public String joinFile(String name) {
        allFileName.add(name);
        return allFileName.toString();
    }

    public AtomicBoolean getLock() {
        return lock;
    }

    public long getTotalByte() {
        return totalByte;
    }

    public long getUploadedBytes() {
        return uploadedBytes;
    }

    public long addUploadedBytes(long bytes) {
        this.uploadedBytes += bytes;
        return this.uploadedBytes;
    }
    public long addTotalBytes(long bytes) {
        this.totalByte += bytes;
        return this.totalByte;
    }

    public long withUploadStart() {
        if (null == uploadStart) {
            this.uploadStart = System.nanoTime();
        }
        return this.uploadStart;
    }
}
