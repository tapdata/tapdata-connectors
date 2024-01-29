package io.tapdata.connector.gauss.cdc.logic;

import java.nio.ByteBuffer;

public interface AnalyzeLog {
    void analyze(ByteBuffer logEvent);
}
