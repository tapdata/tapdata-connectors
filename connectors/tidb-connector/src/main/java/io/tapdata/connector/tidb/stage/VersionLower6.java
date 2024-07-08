package io.tapdata.connector.tidb.stage;

import io.tapdata.entity.error.CoreException;

public class VersionLower6 implements VersionControl {

    public String redirectCDC(String version) {
        throw new CoreException("CDC is not supported when TiDB version is lower than 6.0.0, TiDB version: {}", version);
    }
}
