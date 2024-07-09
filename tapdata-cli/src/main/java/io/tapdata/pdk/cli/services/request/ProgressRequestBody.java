package io.tapdata.pdk.cli.services.request;

import io.tapdata.pdk.cli.services.Uploader;
import io.tapdata.pdk.cli.utils.PrintUtil;
import okhttp3.MediaType;
import okhttp3.RequestBody;
import okio.BufferedSink;
import picocli.CommandLine;

import java.io.IOException;
import java.io.InputStream;

public abstract class ProgressRequestBody<T> extends RequestBody {
    public static final long WARN_AVG = 1024 * 1024 * 2;
    public static final String[] tops = new String[]{
            CommandLine.Help.Ansi.AUTO.string("@|bold,fg(196) ⌾|@"),
            CommandLine.Help.Ansi.AUTO.string("@|bold,fg(28) ✔|@")
    };
    public static final String[] UNIT = new String[]{"B/s", "KB/s", "MB/s", "GB/s"};
    protected final T file;
    protected final String contentType;
    protected PrintUtil printUtil;
    final ProcessGroupInfo groupInfo;

    public ProgressRequestBody(T file, String contentType, PrintUtil printUtil, ProcessGroupInfo groupInfo) {
        this.file = file;
        this.contentType = contentType;
        this.printUtil = printUtil;
        this.groupInfo = groupInfo;
    }

    @Override
    public MediaType contentType() {
        if (null == contentType) return null;
        return MediaType.parse(contentType);
    }

    @Override
    public long contentLength() {
        return -1;
    }

    public abstract long length();

    public abstract String name();

    public String fileName() {
        return null;
    }

    @Override
    public abstract void writeTo(BufferedSink sink) throws IOException;

    protected InputStream write(BufferedSink sink, InputStream fis, String name) throws IOException {
        try {
            synchronized (groupInfo.lock) {
                name = groupInfo.joinFile(name);
                long totalBytes = groupInfo.getTotalByte();
                int logTimes = 100; //每上传200k更新一次进度
                long start = groupInfo.withUploadStart();
                byte[] buffer = new byte[2048];
                long uploadedBytes = groupInfo.getUploadedBytes();
                int bytesRead;
                int step = logTimes;
                while ((bytesRead = fis.read(buffer)) != -1) {
                    sink.write(buffer, 0, bytesRead);
                    uploadedBytes = groupInfo.addUploadedBytes(bytesRead);
                    if (step > 0) {
                        step--;
                        continue;
                    }
                    double avg = avg(start, groupInfo.getUploadedBytes());
                    String avgWithUtil = withUint(avg, 0);
                    boolean isLower = avg < WARN_AVG;
                    avgWithUtil = CommandLine.Help.Ansi.AUTO.string("@|fg(" + (isLower ? "124" : "28" ) + ") " + (isLower ? "⚠️ " : "") + avgWithUtil + "|@");

                    onProgress(tops[0], avgWithUtil, calculate(avg, uploadedBytes, totalBytes), uploadedBytes, totalBytes, printUtil);
                    step = logTimes;
                }
                double avg = avg(start, uploadedBytes);
                onProgress(tops[1], withUint(avg, 0), calculate(avg, uploadedBytes, totalBytes), uploadedBytes, totalBytes, printUtil);
            }
            return fis;
        } catch (Exception e) {
            printUtil.print(PrintUtil.TYPE.ERROR, "Upload failed ["+ name +"], message: " + e.getMessage());
            throw e;
        } finally {
            checkLock();
        }
    }

    protected void checkLock() {
        boolean over = false;
        synchronized (groupInfo.lock) {
            over = groupInfo.uploadedBytes >= groupInfo.totalByte;
            if (over && !groupInfo.over) {
                groupInfo.over = true;
                Uploader.asyncWait(printUtil, groupInfo.lock, " Waiting for remote service to return response result", true);
            }
        }
    }


    protected double avg(long start, long uploadedBytes) {
        long now = System.nanoTime() - start;
        return (uploadedBytes * 1000000000.000000000D) / now;
    }

    protected String calculate(double avg, long uploadedBytes, long total) {
        long surplus = total - uploadedBytes;
        double surplusTime = (surplus > 0 ? surplus : 0) / avg;
        return String.format("%.2fs", surplusTime);
    }

    protected String withUint(double number, int unitIndex) {
        double newNumber = number / 1024;
        if (unitIndex < UNIT.length - 1 && newNumber > 1) {
            return withUint(newNumber, unitIndex + 1);
        }
        return String.format("%.2f", number) + UNIT[unitIndex];
    }

    protected void onProgress(String top, String avg, String surplus, long uploadedBytes, long totalBytes, PrintUtil printUtil) {
        int progressWidth = 50;
        double progress = (double) uploadedBytes / totalBytes;
        int progressInWidth = (int) (progress * progressWidth);
        StringBuilder builder = new StringBuilder("\r  ⎢");
        for (int i = 0; i < progressWidth; i++) {
            if (i < progressInWidth) {
                builder.append(CommandLine.Help.Ansi.AUTO.string("@|bold,fg(28) █|@"));
            } else {
                builder.append(" ");
            }
        }
        builder.append("⎥ ").append(top).append(" ");
        String ps = CommandLine.Help.Ansi.AUTO.string("@|bold,fg(28) " + String.format("%.2f%%", progress * 100) + "|@");
        builder.append(ps).append(" Avg-speed: ").append(avg);
        if (null != surplus) {
            builder.append(" | Remaining-time： ").append(surplus);
        }
        builder.append("\r");
        printUtil.print0(builder.toString());
    }
}
