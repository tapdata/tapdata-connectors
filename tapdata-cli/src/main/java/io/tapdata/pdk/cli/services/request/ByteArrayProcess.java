package io.tapdata.pdk.cli.services.request;

import io.tapdata.pdk.cli.services.Uploader;
import io.tapdata.pdk.cli.utils.PrintUtil;
import okio.BufferedSink;
import picocli.CommandLine;

import java.io.IOException;

public class ByteArrayProcess extends ProgressRequestBody<byte[]>  {
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

                sink.write(file, 0, file.length);

                long uploadedBytes = groupInfo.addUploadedBytes(file.length);
                double avg = avg(start, groupInfo.getUploadedBytes());
                String avgWithUtil = withUint(avg, 0);
                boolean isLower = avg < WARN_AVG;
                avgWithUtil = CommandLine.Help.Ansi.AUTO.string("@|fg(" + (isLower ? "124" : "28") + ") " + (isLower ? "⚠️ " : "") + avgWithUtil + "|@");

                onProgress(tops[0], avgWithUtil, calculate(avg, uploadedBytes, totalBytes), uploadedBytes, totalBytes, printUtil);
            }
        } catch (Exception e) {
            printUtil.print(PrintUtil.TYPE.ERROR, "Upload failed [" + name + "], message: " + e.getMessage());
            throw e;
        } finally {
            boolean over = false;
            synchronized (groupInfo.lock) {
                over = groupInfo.uploadedBytes >= groupInfo.totalByte;
                if (over && !groupInfo.over) {
                    groupInfo.over = true;
                    Uploader.asyncWait(printUtil, groupInfo.lock, " Waiting for remote service to return request result", true);
                }
            }
        }
    }
}
