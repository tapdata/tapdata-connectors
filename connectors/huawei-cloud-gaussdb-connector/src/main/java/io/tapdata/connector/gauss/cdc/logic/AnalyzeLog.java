package io.tapdata.connector.gauss.cdc.logic;

import io.tapdata.connector.gauss.cdc.logic.event.Event;

import java.nio.ByteBuffer;
import java.util.Map;

public interface AnalyzeLog<T> {
    Event.EventEntity<T> analyze(ByteBuffer logEvent, Map<String, Map<String, String>> dataTypeMap);
    default Event.EventEntity<T> analyze(ByteBuffer logEvent, Map<String, Map<String, String>> dataTypeMap, String charset) {
        return analyze(logEvent, dataTypeMap);
    }
}
