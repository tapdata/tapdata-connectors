package com.opencsv;

import com.opencsv.enums.CSVReaderNullFieldIndicator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/** CSV parser that supports multi-character delimiters while retaining OpenCSV's reader contract. */
public class CustomCsvParser implements ICSVParser {
    private final String separator;
    private final char quoteChar;
    private final StringBuilder pendingText = new StringBuilder();
    private boolean pending;
    private Locale errorLocale = Locale.getDefault();

    public CustomCsvParser(String separator) {
        this(separator, DEFAULT_QUOTE_CHARACTER);
    }

    public CustomCsvParser(char separator) {
        this(String.valueOf(separator), DEFAULT_QUOTE_CHARACTER);
    }

    public CustomCsvParser(String separator, char quoteChar) {
        if (separator == null || separator.isEmpty()) {
            throw new IllegalArgumentException("CSV separator cannot be empty");
        }
        this.separator = separator;
        this.quoteChar = quoteChar;
    }

    @Override
    public char getSeparator() {
        return separator.charAt(0);
    }

    @Override
    public char getQuotechar() {
        return quoteChar;
    }

    @Override
    public boolean isPending() {
        return pending;
    }

    @Override
    public String[] parseLineMulti(String line) throws IOException {
        if (line == null) {
            if (!pending) return null;
            line = "";
        }
        if (pending) {
            pendingText.append('\n');
        } else {
            pendingText.setLength(0);
        }
        pendingText.append(line);

        List<String> fields = new ArrayList<>();
        StringBuilder field = new StringBuilder();
        boolean fieldContainsNonWhitespace = false;
        boolean inQuotes = false;
        String input = pendingText.toString();
        for (int i = 0; i < input.length();) {
            char current = input.charAt(i);
            if (inQuotes && current == DEFAULT_ESCAPE_CHARACTER && i + 1 < input.length()) {
                char escaped = input.charAt(i + 1);
                if (escaped == quoteChar || escaped == DEFAULT_ESCAPE_CHARACTER) {
                    field.append(escaped);
                    fieldContainsNonWhitespace |= !Character.isWhitespace(escaped);
                    i += 2;
                    continue;
                }
            }
            if (quoteChar != NULL_CHARACTER && current == quoteChar) {
                if (inQuotes && i + 1 < input.length() && input.charAt(i + 1) == quoteChar) {
                    field.append(quoteChar);
                    fieldContainsNonWhitespace = true;
                    i += 2;
                    continue;
                }
                if (inQuotes || !fieldContainsNonWhitespace) {
                    inQuotes = !inQuotes;
                    i++;
                    continue;
                }
            }
            if (!inQuotes && input.startsWith(separator, i)) {
                fields.add(field.toString());
                field.setLength(0);
                fieldContainsNonWhitespace = false;
                i += separator.length();
                continue;
            }
            field.append(current);
            fieldContainsNonWhitespace |= !Character.isWhitespace(current);
            i++;
        }
        pending = inQuotes;
        if (pending) {
            pendingText.setLength(0);
            pendingText.append(input);
            return new String[0];
        }
        fields.add(field.toString());
        pendingText.setLength(0);
        return fields.toArray(new String[0]);
    }

    @Override
    public String[] parseLine(String line) throws IOException {
        return parseLineMulti(line);
    }

    @Override
    public String parseToLine(String[] values, boolean applyQuotesToAll) {
        if (values == null) return "";
        StringBuilder line = new StringBuilder();
        for (int i = 0; i < values.length; i++) {
            if (i > 0) line.append(separator);
            String value = values[i] == null ? "" : values[i];
            boolean quote = applyQuotesToAll || value.contains(separator) || value.indexOf(quoteChar) >= 0
                    || value.indexOf('\n') >= 0 || value.indexOf('\r') >= 0;
            if (quote && quoteChar != NULL_CHARACTER) {
                line.append(quoteChar).append(value.replace(String.valueOf(quoteChar), String.valueOf(quoteChar) + quoteChar)).append(quoteChar);
            } else {
                line.append(value);
            }
        }
        return line.toString();
    }

    @Override
    public CSVReaderNullFieldIndicator nullFieldIndicator() {
        return DEFAULT_NULL_FIELD_INDICATOR;
    }

    @Override
    public String getPendingText() {
        return pendingText.toString();
    }

    @Override
    public void setErrorLocale(Locale locale) {
        if (locale != null) errorLocale = locale;
    }
}
