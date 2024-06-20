package io.tapdata.pdk.cli.services.request;

import io.tapdata.pdk.cli.utils.PrintUtil;
import okhttp3.MediaType;
import okhttp3.RequestBody;
import okio.BufferedSink;
import picocli.CommandLine;

import java.io.IOException;

public abstract class ProgressRequestBody<T> extends RequestBody {
    protected final T file;
    protected final String contentType;
    protected PrintUtil printUtil;
    protected ProgressRequestBody.ProgressListener progressListener = new ProgressListener(){};

    public ProgressRequestBody(T file, String contentType, PrintUtil printUtil) {
        this.file = file;
        this.contentType = contentType;
        this.printUtil = printUtil;
    }

    @Override
    public MediaType contentType() {
        return MediaType.parse(contentType);
    }

    @Override
    public abstract long contentLength();

    @Override
    public abstract void writeTo(BufferedSink sink) throws IOException;

    interface ProgressListener {
        default void onProgress(String top, long uploadedBytes, long totalBytes, PrintUtil printUtil) {
            int progressWidth = 50;
            double progress = (double) uploadedBytes / totalBytes;
            int progressInWidth = (int) (progress * progressWidth);
            StringBuilder builder = new StringBuilder("\r  [");
            boolean start = false;
            for (int i = 0; i < progressWidth; i++) {
                if (i < progressInWidth) {
                    builder.append("=");
                } else {
                    if (!start) {
                        builder.append("➤");
                        start = true;
                        continue;
                    }
                    if (i < progressWidth - 1)
                        builder.append(" ");
                }
            }
            builder.append("] ").append(top).append(" ");
            String ps = CommandLine.Help.Ansi.AUTO.string("@|bold,fg(22) " +String.format("%.2f%%\r", progress * 100) + "|@");
            builder.append(ps);
            printUtil.print0(builder.toString());
            System.out.flush();
        }
    }
}