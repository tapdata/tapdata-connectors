package io.tapdata.connector.tidb.stage;

public class VersionUpper8 implements VersionControl {
    public String redirectCDC(String version) {
        return "8.0.0";
    }
}
