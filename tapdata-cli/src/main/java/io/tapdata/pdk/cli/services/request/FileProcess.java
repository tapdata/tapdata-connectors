package io.tapdata.pdk.cli.services.request;

import io.tapdata.pdk.cli.utils.PrintUtil;
import okio.BufferedSink;
import picocli.CommandLine;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class FileProcess extends ProgressRequestBody<File> {
    public static final String[] tops = new String[]{
            CommandLine.Help.Ansi.AUTO.string("@|bold,fg(196) ⌾|@"),
            CommandLine.Help.Ansi.AUTO.string("@|bold,fg(22) ✔|@")
    };
    public static final String[] UNIT = new String[]{"B/s", "KB/s", "MB/s", "GB/s"};

    public FileProcess(File file, String contentType, PrintUtil printUtil) {
        super(file, contentType, printUtil);
    }

    @Override
    public long contentLength() {
        return file.length();
    }

    @Override
    public void writeTo(BufferedSink sink) throws IOException {
        long totalBytes = contentLength();
        int logTimes = 100; //每上传200k更新一次进度
        long start = System.nanoTime();
        try (FileInputStream fis = new FileInputStream(file)) {
            byte[] buffer = new byte[2048];
            long uploadedBytes = 0;
            int bytesRead;
            int step = logTimes;
            while ((bytesRead = fis.read(buffer)) != -1) {
                sink.write(buffer, 0, bytesRead);
                uploadedBytes += bytesRead;
                if (step > 0) {
                    step--;
                    continue;
                }
                double avg = avg(start, uploadedBytes);
                progressListener.onProgress(tops[0], withUint(avg, 0), calculate(avg, uploadedBytes, totalBytes), uploadedBytes, totalBytes, printUtil);
                step = logTimes;
            }
            double avg = avg(start, totalBytes);
            progressListener.onProgress(tops[1], withUint(avg, 0), "", uploadedBytes, totalBytes, printUtil);
        }
        printUtil.print(PrintUtil.TYPE.TIP, "\n  this connector file upload succeed, next will upload doc and icon, please wait");
    }

    protected double avg(long start, long uploadedBytes) {
        long now = System.nanoTime() - start;
        return (uploadedBytes * 1000000000.000000000D) / now;
    }

    protected String calculate(double avg, long uploadedBytes, long total) {
        long surplus = total - uploadedBytes;
        double surplusTime = surplus / avg;
        return String.format("%.2fs", surplusTime);
    }

    protected String withUint(double number, int unitIndex) {
        double newNumber = number / 1024;
        if (unitIndex < UNIT.length - 1 && newNumber > 1) {
            return withUint(newNumber, unitIndex + 1);
        }
        return String.format("%.2f", number) + UNIT[unitIndex];
    }
}
