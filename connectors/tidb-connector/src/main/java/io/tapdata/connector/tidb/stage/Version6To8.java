package io.tapdata.connector.tidb.stage;


public class Version6To8 implements VersionControl {

    public String redirectCDC(String version) {
        return "8.0.0";
    }
}
