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
        long start = System.currentTimeMillis();
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
                double avg = ((uploadedBytes >> 10) * 1.00D) / ((System.currentTimeMillis() - start) / 1000.00);
                progressListener.onProgress(tops[0], avg, uploadedBytes, totalBytes, printUtil);
                step = logTimes;
            }
            double avg = ((uploadedBytes >> 10) * 1.00D) / ((System.currentTimeMillis() - start) / 1000.00);
            progressListener.onProgress(tops[1], avg, uploadedBytes, totalBytes, printUtil);
        }
        printUtil.print(PrintUtil.TYPE.INFO, "\n  this connector file upload succeed, next will upload doc and icon, please wait");
    }
}
