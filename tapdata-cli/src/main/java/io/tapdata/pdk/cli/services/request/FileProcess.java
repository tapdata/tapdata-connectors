package io.tapdata.pdk.cli.services.request;

import io.tapdata.pdk.cli.utils.PrintUtil;
import okio.BufferedSink;
import picocli.CommandLine;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class FileProcess extends ProgressRequestBody<File> {

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
        try (FileInputStream fis = new FileInputStream(file)) {
            byte[] buffer = new byte[1024];
            long uploadedBytes = 0;
            int bytesRead;
            String[] tops = new String[]{
                CommandLine.Help.Ansi.AUTO.string("@|bold,fg(196) ⌾|@"),
                CommandLine.Help.Ansi.AUTO.string("@|bold,fg(22) ✔|@")
            };
            while ((bytesRead = fis.read(buffer)) != -1) {
                sink.write(buffer, 0, bytesRead);
                uploadedBytes += bytesRead;
                progressListener.onProgress(tops[0], uploadedBytes, totalBytes, printUtil);
            }
            progressListener.onProgress(tops[1], uploadedBytes, totalBytes, printUtil);
        }
        printUtil.print1("\n", "");
        printUtil.print(PrintUtil.TYPE.INFO, "  this connector file upload succeed, next will upload doc and icon, please wait");
    }
}
