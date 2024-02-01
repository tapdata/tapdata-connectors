package io.tapdata.connector.gauss.cdc.logic;

import io.tapdata.connector.gauss.cdc.logic.event.Event;
import io.tapdata.entity.event.dml.TapRecordEvent;

import java.nio.ByteBuffer;

public interface AnalyzeLog<T> {
    Event.EventEntity<T> analyze(ByteBuffer logEvent);
}
