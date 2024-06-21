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
            long onceStart = start;
            String realTime = null;
            while ((bytesRead = fis.read(buffer)) != -1) {
                sink.write(buffer, 0, bytesRead);
                try {
                    uploadedBytes += bytesRead;
                    if (step > 0) {
                        step--;
                        continue;
                    }
                    realTime = calculate(onceStart, bytesRead);
                    progressListener.onProgress(tops[0], calculate(start, uploadedBytes), realTime, uploadedBytes, totalBytes, printUtil);
                    step = logTimes;
                } finally {
                    onceStart = System.nanoTime();
                }
            }
            if (null == realTime) {
                realTime = calculate(start, totalBytes);
            }
            progressListener.onProgress(tops[1], calculate(start, totalBytes), realTime, uploadedBytes, totalBytes, printUtil);
        }
        printUtil.print(PrintUtil.TYPE.TIP, "\n  this connector file upload succeed, next will upload doc and icon, please wait");
    }

    protected String calculate(long start, long uploadedBytes) {
        long now = System.nanoTime() - start;
        return withUint((uploadedBytes * 1.00) / now * 1000000000, 0);
    }


    protected String withUint(double number, int unitIndex) {
        long newNumber = ((Double)number).longValue() >> 10;
        if (unitIndex < UNIT.length - 1 && newNumber > 1) {
            return withUint(number / 1024, unitIndex + 1);
        }
        return String.format("%.2f", number) + UNIT[unitIndex];
    }
}
