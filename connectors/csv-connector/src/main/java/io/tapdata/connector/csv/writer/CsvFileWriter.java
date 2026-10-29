package io.tapdata.connector.csv.writer;

import io.tapdata.common.AbstractFileWriter;
import io.tapdata.file.TapFileStorage;
import io.tapdata.kit.ErrorKit;

public class CsvFileWriter extends AbstractFileWriter {

    private DelimitedCsvWriter csvWriter;
    private String separator;
    private char quoteChar;
    private String lineEnd;

    public CsvFileWriter(TapFileStorage storage, String path, String fileEncoding) throws Exception {
        super(storage, path, fileEncoding);
    }

    @Override
    public void init() throws Exception {
        super.init();
        this.csvWriter = new DelimitedCsvWriter(writer, separator, quoteChar, lineEnd);
    }

    public void setRule(String separator, char quoteChar, String lineEnd) {
        this.separator = separator;
        this.quoteChar = quoteChar;
        this.lineEnd = lineEnd;
    }

    public DelimitedCsvWriter getCsvWriter() {
        return csvWriter;
    }

    public void setCsvWriter(DelimitedCsvWriter csvWriter) {
        this.csvWriter = csvWriter;
    }

    @Override
    public void close() {
        ErrorKit.ignoreAnyError(() -> csvWriter.close());
        super.close();
    }

    @Override
    public void flush() {
        ErrorKit.ignoreAnyError(() -> csvWriter.flush());
    }

}
