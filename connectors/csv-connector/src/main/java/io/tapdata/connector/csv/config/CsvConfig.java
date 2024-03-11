package io.tapdata.connector.csv.config;

import io.tapdata.common.FileConfig;

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
        switch (separatorType) {
            case "\\t":
                separator = "\t";
                break;
            case " ":
                separator = " ";
                break;
            case "ascii":
                separator = "" + (char) Integer.parseInt(separator.startsWith("0x") ? separator.substring(2) : separator, 16);
                break;
            default:
                separator = ",";
        }
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
