package io.tapdata.connector.csv.writer;

import java.io.IOException;
import java.io.Writer;

/** Writes CSV rows with a possibly multi-character delimiter. */
public class DelimitedCsvWriter {
    private final Writer writer;
    private final String separator;
    private final char quoteChar;
    private final String lineEnd;

    public DelimitedCsvWriter(Writer writer, String separator, char quoteChar, String lineEnd) {
        this.writer = writer;
        this.separator = separator;
        this.quoteChar = quoteChar;
        this.lineEnd = lineEnd;
    }

    public void writeNext(String[] values) {
        try {
            if (values != null) {
                for (int i = 0; i < values.length; i++) {
                    if (i > 0) writer.write(separator);
                    writeValue(values[i]);
                }
            }
            writer.write(lineEnd);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to write CSV record", e);
        }
    }

    private void writeValue(String value) throws IOException {
        if (value == null) return;
        boolean quote = quoteChar != '\0' && (value.contains(separator) || hasDelimiterPrefixSuffix(value)
                || value.indexOf(quoteChar) >= 0
                || value.indexOf('\n') >= 0 || value.indexOf('\r') >= 0);
        if (!quote) {
            writer.write(value);
            return;
        }
        writer.write(quoteChar);
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            // Match the parser's legacy backslash escaping within quoted fields.
            if (c == quoteChar || c == '\\') writer.write(c);
            writer.write(c);
        }
        writer.write(quoteChar);
    }

    private boolean hasDelimiterPrefixSuffix(String value) {
        // A field suffix plus the following delimiter can otherwise create an earlier match.
        for (int length = 1; length < separator.length() && length <= value.length(); length++) {
            if (value.regionMatches(value.length() - length, separator, 0, length)) return true;
        }
        return false;
    }

    public void flush() {
        try {
            writer.flush();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to flush CSV records", e);
        }
    }

    public void close() {
        try {
            writer.close();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to close CSV records", e);
        }
    }
}
