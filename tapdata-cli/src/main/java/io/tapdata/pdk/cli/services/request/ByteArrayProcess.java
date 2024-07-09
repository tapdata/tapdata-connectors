package io.tapdata.pdk.cli.services.request;

import io.tapdata.pdk.cli.services.Uploader;
import io.tapdata.pdk.cli.utils.PrintUtil;
import okio.BufferedSink;
import picocli.CommandLine;

import java.io.IOException;

public class ByteArrayProcess extends ProgressRequestBody<byte[]> {
    String name;

    public ByteArrayProcess(String name, byte[] bytes, String contentType, PrintUtil printUtil, ProcessGroupInfo groupInfo) {
        super(bytes, contentType, printUtil, groupInfo);
        this.name = name;
    }

    @Override
    public long length() {
        return file.length;
    }

    @Override
    public String name() {
        return "file";
    }

    @Override
    public String fileName() {
        return name;
    }

    @Override
    public void writeTo(BufferedSink sink) throws IOException {
        try {
            synchronized (groupInfo.lock) {
                name = groupInfo.joinFile(name);
                long totalBytes = groupInfo.getTotalByte();
                long start = groupInfo.withUploadStart();
                int from = 0;
                int logTimes = 100; //每上传200k更新一次进度
                int maxReadSize = 2048;
                int readSize;
                int step = logTimes;
                long uploadedBytes = groupInfo.getUploadedBytes();
                while (from < file.length) {
                    int bytesCount = file.length - from;
                    readSize = Math.min(bytesCount, maxReadSize);
                    sink.write(file, from, readSize);
                    from += readSize;
                    uploadedBytes = groupInfo.addUploadedBytes(readSize);
                    if (step > 0) {
                        step--;
                        continue;
                    }
                    double avg = avg(start, groupInfo.getUploadedBytes());
                    String avgWithUtil = withUint(avg, 0);
                    boolean isLower = avg < WARN_AVG;
                    avgWithUtil = CommandLine.Help.Ansi.AUTO.string("@|fg(" + (isLower ? "124" : "28") + ") " + (isLower ? "⚠️ " : "") + avgWithUtil + "|@");
                    onProgress(tops[0], avgWithUtil, calculate(avg, uploadedBytes, totalBytes), uploadedBytes, totalBytes, printUtil);
                }
                double avg = avg(start, uploadedBytes);
                onProgress(tops[1], withUint(avg, 0), calculate(avg, uploadedBytes, totalBytes), uploadedBytes, totalBytes, printUtil);
            }
        } catch (Exception e) {
            printUtil.print(PrintUtil.TYPE.ERROR, "Upload failed [" + name + "], message: " + e.getMessage());
            throw e;
        } finally {
            checkLock();
        }
    }
}
