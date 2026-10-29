package io.tapdata.connector.csv.config;

import com.opencsv.CustomCsvParser;
import io.tapdata.common.FileConfig;
import io.tapdata.kit.EmptyKit;

import java.util.Map;

public class CsvConfig extends FileConfig {

    private Boolean offStandard = false;
    private String lineExpression;
    private String separator = ",";
    private String quoteChar = "\"";
    private String lineEnd = "\n";
    private String separatorType = ",";
    private String lineEndType = "\n";

    public CsvConfig() {
        setFileType("csv");
    }

    public CsvConfig load(Map<String, Object> map) {
        super.load(map);
        String configuredSeparator = separator;
        switch (separatorType) {
            case "\\t":
                separator = "\t";
                break;
            case " ":
                separator = " ";
                break;
            case "ascii":
                separator = "" + (char) Integer.parseInt(configuredSeparator.startsWith("0x") ? configuredSeparator.substring(2) : configuredSeparator, 16);
                break;
            case "custom":
                separator = "\\t".equals(configuredSeparator) ? "\t" : configuredSeparator;
                break;
            default:
                separator = ",";
        }
        validateSeparator(separator);
        switch (lineEndType) {
            case " ":
                lineEnd = " ";
                break;
            case "ascii":
                lineEnd = "" + (char) Integer.parseInt(lineEnd.startsWith("0x") ? lineEnd.substring(2) : lineEnd, 16);
                break;
            default:
                lineEnd = "\n";
        }
        return this;
    }

    public static void validateSeparator(String separator) {
        if (separator == null || separator.isEmpty()) {
            throw new IllegalArgumentException("CSV separator cannot be empty");
        }
        if (separator.length() > 5) {
            throw new IllegalArgumentException("CSV separator cannot be longer than 5 characters");
        }
        if (separator.indexOf('\r') >= 0 || separator.indexOf('\n') >= 0) {
            throw new IllegalArgumentException("CSV separator cannot contain a line break");
        }
    }

    public Boolean getOffStandard() {
        return offStandard;
    }

    public void setOffStandard(Boolean offStandard) {
        this.offStandard = offStandard;
    }

    public String getLineExpression() {
        return lineExpression;
    }

    public void setLineExpression(String lineExpression) {
        this.lineExpression = lineExpression;
    }

    public String getSeparator() {
        return separator;
    }

    public void setSeparator(String separator) {
        this.separator = separator;
    }

    public String getQuoteChar() {
        return quoteChar;
    }

    /** Resolve disabled quoting consistently for every reader and writer. */
    public char getQuoteCharacter() {
        return EmptyKit.isBlank(quoteChar) ? '\0' : quoteChar.charAt(0);
    }

    /** Each file needs its own parser because quoted records can span lines. */
    public CustomCsvParser createParser() {
        return new CustomCsvParser(separator, getQuoteCharacter());
    }

    public void setQuoteChar(String quoteChar) {
        this.quoteChar = quoteChar;
    }

    public String getLineEnd() {
        return lineEnd;
    }

    public void setLineEnd(String lineEnd) {
        this.lineEnd = lineEnd;
    }

    public String getSeparatorType() {
        return separatorType;
    }

    public void setSeparatorType(String separatorType) {
        this.separatorType = separatorType;
    }

    public String getLineEndType() {
        return lineEndType;
    }

    public void setLineEndType(String lineEndType) {
        this.lineEndType = lineEndType;
    }
}
