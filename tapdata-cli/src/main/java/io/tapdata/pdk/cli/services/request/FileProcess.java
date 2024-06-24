package io.tapdata.pdk.cli.services.request;

import io.tapdata.pdk.cli.utils.PrintUtil;
import okio.BufferedSink;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class FileProcess extends ProgressRequestBody<File> {

    public FileProcess(File file, String contentType, PrintUtil printUtil, ProcessGroupInfo groupInfo) {
        super(file, contentType, printUtil, groupInfo);
    }

    @Override
    public long contentLength() {
        return file.length();
    }

    @Override
    public void writeTo(BufferedSink sink) throws IOException {
        try(InputStream stream = write(sink, new FileInputStream(file), file.getName())) {
            //do nothing
        }
    }
}
