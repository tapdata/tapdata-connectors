package io.tapdata.pdk.cli.services.request;

import com.tapdata.tm.sdk.util.IOUtil;
import io.tapdata.pdk.cli.utils.PrintUtil;
import okio.BufferedSink;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class FileProcess extends ProgressRequestBody<File> {
    long length;
    public FileProcess(File file, String contentType, PrintUtil printUtil, ProcessGroupInfo groupInfo) {
        super(file, contentType, printUtil, groupInfo);
        try {
            byte[] bytes = IOUtil.readFile(file);
            length = bytes.length;
        } catch (Exception e) {
            // do nothing
        }
    }

    @Override
    public long contentLength() {
        return length;
    }

    @Override
    public String name() {
        return "file";
    }

    @Override
    public String fileName() {
        return file.getName();
    }

    @Override
    public void writeTo(BufferedSink sink) throws IOException {
        try(InputStream stream = write(sink, new FileInputStream(file), file.getName())) {
            //do nothing
        }
    }
}
