package io.tapdata.pdk.cli.services.request;

import io.tapdata.pdk.cli.utils.PrintUtil;

import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicBoolean;

public class ProcessGroupInfo {
    public final AtomicBoolean lock;
    public long totalByte;
    public long uploadedBytes;
    public final StringJoiner allFileName;
    public Long uploadStart;
    public boolean over;
    public PrintUtil printUtil;
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

    public PrintUtil getPrintUtil() {
        return printUtil;
    }

    public void setPrintUtil(PrintUtil printUtil) {
        this.printUtil = printUtil;
    }
}
