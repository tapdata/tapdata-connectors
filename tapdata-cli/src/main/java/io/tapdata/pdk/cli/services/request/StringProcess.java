package io.tapdata.pdk.cli.services.request;

import io.tapdata.pdk.cli.services.Uploader;
import io.tapdata.pdk.cli.utils.PrintUtil;
import okio.BufferedSink;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;

import static java.nio.charset.StandardCharsets.UTF_8;

public class StringProcess extends ProgressRequestBody<String> {
    String name;
    byte[] bytes;
    CharsetEncoder encoder;
    int index;

    public StringProcess(String file, String name, String contentType, PrintUtil printUtil, ProcessGroupInfo groupInfo) {
        super(file, contentType, printUtil, groupInfo);
        this.name = name;
        encoder = StandardCharsets.UTF_8.newEncoder();
        try {
            ByteBuffer encode = encoder.encode(CharBuffer.wrap(file));
            bytes = encode.array();
            index = encode.limit();
//            bytes = file.getBytes(UTF_8);
//            index = bytes.length;
        } catch (Exception e) {
            bytes = new byte[]{};
        }
    }

    @Override
    public long contentLength() {
        return index;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void writeTo(BufferedSink sink) throws IOException {
        try {
            synchronized (groupInfo.lock) {
                name = groupInfo.joinFile(name);
                long totalBytes = groupInfo.getTotalByte();
                long start = groupInfo.withUploadStart();

                sink.write(bytes, 0, index);

                long uploadedBytes = groupInfo.addUploadedBytes(index);
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
