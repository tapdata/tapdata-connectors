package io.tapdata.pdk.cli.services.request;

import io.tapdata.pdk.cli.utils.PrintUtil;
import okio.BufferedSink;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

public class InputStreamProcess extends ProgressRequestBody<InputStream> {
    String fileName;
    final long contentLength;

    public InputStreamProcess(InputStream file, String contentType, String fileName, PrintUtil printUtil, ProcessGroupInfo groupInfo) {
        super(new BufferedInputStream(file, 1024 * 1024 * 10), contentType, printUtil, groupInfo);
        this.fileName = fileName;
        contentLength = readLength();
        this.printUtil = printUtil;
    }

    protected long readLength() {
        try {
            if (file.markSupported()) {
                file.mark(Integer.MAX_VALUE);
                long length = getInputStreamLength(file);
                file.reset();
                return length;
            } else {
                printUtil.print(PrintUtil.TYPE.ERROR, "File is to lager, more than: " + 1024 * 1024 * 10 + "B");
            }
        } catch (Exception e) {
            printUtil.print(PrintUtil.TYPE.ERROR, "Get file " + fileName + " length, message: " + e.getMessage());
        }
        return 0;
    }

    private static long getInputStreamLength(InputStream inputStream) throws IOException {
        long length = 0;
        byte[] buffer = new byte[1024];
        int bytesRead;
        while ((bytesRead = inputStream.read(buffer)) != -1) {
            length += bytesRead;
        }
        return length;
    }

    @Override
    public long contentLength() {
        return contentLength;
    }


    @Override
    public void writeTo(BufferedSink sink) throws IOException {
        try(InputStream stream = write(sink, file, fileName)) {
            //printUtil.print(PrintUtil.TYPE.TIP, "\n");
            if (groupInfo.lock.get()) {

            }
            //printUtil.print(PrintUtil.TYPE.TIP, "\n  this connector file upload succeed, next will upload doc and icon, please wait");
        }
    }
}
